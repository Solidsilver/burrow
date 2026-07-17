<script lang="ts">
  import { onMount } from 'svelte';
  import { api, type SnapshotInfo } from '../lib/api.svelte';
  import { app, navigate, refresh, toast } from '../lib/state.svelte';
  import { fmtAgo, fmtBytes, fmtTime, shortHex } from '../lib/format';
  import StatusPill from '../components/StatusPill.svelte';
  import Modal from '../components/Modal.svelte';

  let { id }: { id: string } = $props();

  let backup = $derived(app.status?.backups.find((b) => b.backup_id === id));
  let config = $derived(app.backupConfigs.find((c) => c.id === id));

  let snapshots = $state<SnapshotInfo[] | null>(null);
  let running = $state(false);

  // Restore form
  let restoreOpen = $state(false);
  let restoreSnapshot = $state<number | null>(null); // null = latest
  let restoreTarget = $state('');
  let restoreConfirm = $state('');
  let restoring = $state(false);

  async function load() {
    try {
      snapshots = (await api.backupSnapshots(id)).sort((a, b) => b.created_at - a.created_at);
    } catch (e) {
      toast('err', e instanceof Error ? e.message : String(e));
      snapshots = [];
    }
  }

  onMount(() => {
    void load();
  });

  async function run() {
    running = true;
    try {
      const s = await api.backupRun(id);
      toast('ok', `snapshot: ${s.file_count} files, ${fmtBytes(s.bytes_new)} new`);
      await load();
    } catch (e) {
      toast('err', e instanceof Error ? e.message : String(e));
    } finally {
      running = false;
      await refresh({ slow: true });
    }
  }

  function openRestore() {
    restoreSnapshot = null;
    restoreTarget = '';
    restoreConfirm = '';
    restoreOpen = true;
  }

  async function doRestore() {
    restoring = true;
    try {
      const r = await api.restore(id, restoreSnapshot, restoreTarget);
      toast('ok', `restored ${r.files} files (${fmtBytes(r.bytes)}) to ${r.target}`);
      restoreOpen = false;
    } catch (e) {
      toast('err', e instanceof Error ? e.message : String(e));
    } finally {
      restoring = false;
    }
  }
</script>

<div class="stack-lg">
  <div class="page-head">
    <div>
      <div class="row" style="gap: 10px">
        <button class="ghost sm" onclick={() => navigate('#/backups')}>← backups</button>
        <h1>{id}</h1>
        {#if backup}<StatusPill health={backup.health} />{/if}
      </div>
      {#if backup}
        <div class="sub mono">{backup.paths.join(', ')}</div>
      {/if}
    </div>
    <div class="row">
      <button class="sm" disabled={running} onclick={run}>
        {#if running}<span class="spinner"></span>{:else}run now{/if}
      </button>
      <button class="primary sm" disabled={!snapshots?.length} onclick={openRestore}>restore…</button>
    </div>
  </div>

  {#if backup}
    <div class="grid cols-3">
      <div class="card">
        <div class="small muted">Snapshots</div>
        <div class="strong" style="font-size: 18px">{backup.snapshot_count}</div>
      </div>
      <div class="card">
        <div class="small muted">Replica target</div>
        <div class="strong" style="font-size: 18px">{backup.replicas}</div>
      </div>
      <div class="card">
        <div class="small muted">Last run</div>
        <div class="strong" style="font-size: 18px">
          {backup.last_snapshot ? fmtAgo(backup.last_snapshot.created_at) : 'never'}
        </div>
      </div>
    </div>
  {/if}

  {#if config}
    <div class="row wrap" style="gap: 8px">
      <span class="pill muted">{config.schedule ?? 'manual only'}</span>
      {#if config.keep_last}
        <span class="pill muted">keep last {config.keep_last}</span>
      {/if}
      {#if config.min_offsite > 0}
        <span class="pill muted">offsite ≥ {config.min_offsite}</span>
      {/if}
      {#each config.exclude as pattern (pattern)}
        <span class="pill muted mono">exclude {pattern}</span>
      {/each}
    </div>
  {/if}

  <div class="card card-pad-sm">
    {#if snapshots === null}
      <div class="empty"><span class="spinner"></span></div>
    {:else if snapshots.length === 0}
      <div class="empty">no snapshots yet — run this backup to create the first one</div>
    {:else}
      <table class="data">
        <thead>
          <tr>
            <th>Created</th>
            <th class="right">Files</th>
            <th class="right">Scanned</th>
            <th class="right">New</th>
            <th class="right">Chunks</th>
            <th>Manifest</th>
          </tr>
        </thead>
        <tbody>
          {#each snapshots as s (s.created_at)}
            <tr>
              <td class="nowrap">
                <span class="strong">{fmtTime(s.created_at)}</span>
                <span class="faint small"> · {fmtAgo(s.created_at)}</span>
              </td>
              <td class="right">{s.file_count}</td>
              <td class="right">{fmtBytes(s.bytes_scanned)}</td>
              <td class="right">+{fmtBytes(s.bytes_new)}</td>
              <td class="right">{s.chunk_count}</td>
              <td class="mono small muted">{shortHex(s.manifest_hash)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
</div>

{#if restoreOpen}
  <Modal title="Restore {id}" onclose={() => (restoreOpen = false)}>
    <form
      class="stack"
      onsubmit={(e) => {
        e.preventDefault();
        void doRestore();
      }}
    >
      <label class="field">
        snapshot
        <select bind:value={restoreSnapshot}>
          <option value={null}>latest ({snapshots?.length ? fmtTime(snapshots![0].created_at) : '—'})</option>
          {#each snapshots ?? [] as s (s.created_at)}
            <option value={s.created_at}>{fmtTime(s.created_at)} · {s.file_count} files</option>
          {/each}
        </select>
      </label>
      <label class="field">
        target directory (on this machine)
        <input
          type="text"
          class="mono"
          placeholder="/tmp/get-it-back"
          bind:value={restoreTarget}
          required
          spellcheck="false"
        />
      </label>
      <label class="field">
        type <span class="mono strong">{id}</span> to confirm
        <input type="text" bind:value={restoreConfirm} autocomplete="off" spellcheck="false" />
      </label>
      <div class="row" style="justify-content: flex-end">
        <button type="button" onclick={() => (restoreOpen = false)}>cancel</button>
        <button
          class="primary"
          type="submit"
          disabled={restoring || restoreConfirm !== id || !restoreTarget.trim()}
        >
          {#if restoring}<span class="spinner"></span> restoring…{:else}restore{/if}
        </button>
      </div>
      <p class="small faint" style="margin: 0">
        restore prefers local blobs and fetches anything missing from replica holders
      </p>
    </form>
  </Modal>
{/if}
