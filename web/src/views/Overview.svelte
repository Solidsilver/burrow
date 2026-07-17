<script lang="ts">
  import { api } from '../lib/api.svelte';
  import { act, app, navigate, refresh, toast } from '../lib/state.svelte';
  import { fmtAgo, fmtBytes, fmtUntil, plural } from '../lib/format';
  import StatusPill from '../components/StatusPill.svelte';
  import UsageBar from '../components/UsageBar.svelte';

  let running = $state<string | null>(null);
  let actionBusy = $state<string | null>(null);

  let activePeers = $derived(app.peers.filter((p) => p.state === 'active'));
  let given = $derived(activePeers.reduce((acc, p) => acc + p.given_bytes, 0));
  let givenUsed = $derived(activePeers.reduce((acc, p) => acc + p.given_used, 0));
  let received = $derived(activePeers.reduce((acc, p) => acc + p.received_bytes, 0));
  let receivedUsed = $derived(activePeers.reduce((acc, p) => acc + p.received_used, 0));

  function scheduleOf(id: string): string | null {
    return app.backupConfigs.find((c) => c.id === id)?.schedule ?? null;
  }

  async function run(id: string) {
    running = id;
    try {
      const s = await api.backupRun(id);
      toast('ok', `snapshot of ${id}: ${plural(s.file_count, 'file')}, ${fmtBytes(s.bytes_new)} new`);
    } catch (e) {
      toast('err', e instanceof Error ? e.message : String(e));
    } finally {
      running = null;
      await refresh({ slow: true });
    }
  }

  async function simple(key: string, work: () => Promise<unknown>) {
    actionBusy = key;
    try {
      await act(work, key);
    } finally {
      actionBusy = null;
    }
  }
</script>

<div class="stack-lg">
  <div class="page-head">
    <div>
      <h1>Overview</h1>
      {#if app.status}
        <div class="sub">
          {app.status.node_name} · {app.status.device_name} · {app.status.mode} mode
        </div>
      {/if}
    </div>
    <div class="row">
      {#if app.server?.paused_until}
        <button class="sm" disabled={actionBusy !== null} onclick={() => simple('resume', api.resume)}>
          resume
        </button>
      {:else}
        <button class="sm" disabled={actionBusy !== null} onclick={() => simple('pause 1h', () => api.pause(3600))}>
          pause 1h
        </button>
        <button class="sm" disabled={actionBusy !== null} onclick={() => simple('pause 2h', () => api.pause(7200))}>
          pause 2h
        </button>
        <button class="sm" disabled={actionBusy !== null} onclick={() => simple('pause', () => api.pause(null))}>
          pause indefinitely
        </button>
      {/if}
    </div>
  </div>

  {#if app.server?.paused_until}
    <div class="banner">
      <span>⏸</span>
      <span class="grow">
        Scheduled backups and replication are paused ({fmtUntil(app.server.paused_until)}).
        Manual runs still work.
      </span>
      <button class="sm" disabled={actionBusy !== null} onclick={() => simple('resume', api.resume)}>
        resume
      </button>
    </div>
  {/if}

  <div class="grid cols-3">
    <div class="card">
      <div class="small muted">You give friends</div>
      <div class="strong" style="font-size: 18px; margin: 4px 0 8px">{fmtBytes(given)}</div>
      <UsageBar used={givenUsed} total={given} />
      <div class="small faint" style="margin-top: 6px">{fmtBytes(givenUsed)} holding their data</div>
    </div>
    <div class="card">
      <div class="small muted">Friends give you</div>
      <div class="strong" style="font-size: 18px; margin: 4px 0 8px">{fmtBytes(received)}</div>
      <UsageBar used={receivedUsed} total={received} />
      <div class="small faint" style="margin-top: 6px">{fmtBytes(receivedUsed)} of yours offsite</div>
    </div>
    <div class="card">
      <div class="small muted">Friends' data on this device</div>
      <div class="strong" style="font-size: 18px; margin: 4px 0 8px">
        {fmtBytes(app.status?.hosting.held_total ?? 0)}
      </div>
      <UsageBar used={app.status?.hosting.held_total ?? 0} total={app.status?.hosting.offer_max ?? null} />
      <div class="small faint" style="margin-top: 6px">
        {#if app.status?.hosting.offer_max}
          ceiling {fmtBytes(app.status.hosting.offer_max)}
        {:else}
          no ceiling set
        {/if}
      </div>
    </div>
  </div>

  <section class="stack">
    <div class="row between">
      <h2>Backups</h2>
      <a href="#/backups" class="small">all backups →</a>
    </div>
    {#if !app.status?.backups.length}
      <div class="card empty">
        no backups configured — add <code>[[backup]]</code> sections to
        <code>config.toml</code> and restart the daemon
      </div>
    {:else}
      <div class="grid cols-2">
        {#each app.status.backups as b (b.backup_id)}
          <div class="card stack" style="gap: 10px">
            <div class="row between">
              <button class="ghost strong" style="padding: 0; font-size: 14px" onclick={() => navigate(`#/backups/${b.backup_id}`)}>
                {b.backup_id}
              </button>
              <StatusPill health={b.health} />
            </div>
            <div class="row small muted wrap" style="gap: 14px">
              <span>{plural(b.snapshot_count, 'snapshot')}</span>
              <span>target {b.replicas} replicas</span>
              {#if scheduleOf(b.backup_id)}
                <span class="mono">{scheduleOf(b.backup_id)}</span>
              {/if}
              {#if b.last_snapshot}
                <span>last {fmtAgo(b.last_snapshot.created_at)}</span>
              {:else}
                <span>never run</span>
              {/if}
            </div>
            <div class="row between">
              <span class="small faint mono" style="overflow: hidden; text-overflow: ellipsis">
                {b.paths.join(', ')}
              </span>
              <button class="sm" disabled={running !== null} onclick={() => run(b.backup_id)}>
                {#if running === b.backup_id}<span class="spinner"></span>{:else}run now{/if}
              </button>
            </div>
          </div>
        {/each}
      </div>
    {/if}
  </section>

  <div class="grid cols-2">
    <section class="card stack">
      <h2>Recent snapshots</h2>
      {#if !app.snapshots.length}
        <div class="muted small">nothing yet — snapshots show up here after the first run</div>
      {:else}
        <table class="data">
          <tbody>
            {#each app.snapshots.slice(0, 8) as s (s.backup_id + s.created_at)}
              <tr class="clickable" onclick={() => navigate(`#/backups/${s.backup_id}`)}>
                <td class="strong">{s.backup_id}</td>
                <td class="muted small nowrap" title={new Date(s.created_at * 1000).toLocaleString()}>
                  {fmtAgo(s.created_at)}
                </td>
                <td class="small right nowrap">{s.file_count} files</td>
                <td class="small right nowrap muted">+{fmtBytes(s.bytes_new)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </section>

    <section class="card stack">
      <h2>Maintenance</h2>
      <p class="small muted" style="margin: 0">
        Repair spot-checks replicas and re-replicates anything below target. Resync rebuilds
        your catalog from what peers hold (disaster recovery).
      </p>
      <div class="row wrap">
        <button class="sm" disabled={actionBusy !== null} onclick={() => simple('repair', api.repair)}>
          {#if actionBusy === 'repair'}<span class="spinner"></span>{:else}run repair{/if}
        </button>
        <button class="sm" disabled={actionBusy !== null} onclick={() => simple('resync', api.resync)}>
          {#if actionBusy === 'resync'}<span class="spinner"></span>{:else}resync catalog{/if}
        </button>
      </div>
      <p class="small faint" style="margin: 0">long operations block until done — this can take minutes</p>
    </section>
  </div>
</div>
