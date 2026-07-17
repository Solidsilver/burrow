<script lang="ts">
  import { api, type BackupConfigView } from '../lib/api.svelte';
  import { app, navigate, refresh, toast } from '../lib/state.svelte';
  import { fmtAgo, fmtBytes, plural } from '../lib/format';
  import StatusPill from '../components/StatusPill.svelte';

  let running = $state<string | null>(null);

  function cfgFor(id: string): BackupConfigView | undefined {
    return app.backupConfigs.find((c) => c.id === id);
  }

  function cfgLine(id: string): string {
    const c = cfgFor(id);
    if (!c) return '';
    const parts = [c.schedule ?? 'manual'];
    if (c.keep_last) parts.push(`keep ${c.keep_last}`);
    if (c.min_offsite > 0) parts.push(`offsite ≥ ${c.min_offsite}`);
    return parts.join(' · ');
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
</script>

<div class="stack-lg">
  <div class="page-head">
    <div>
      <h1>Backups</h1>
      <div class="sub">configured in config.toml — schedules run from the daemon</div>
    </div>
  </div>

  <div class="card card-pad-sm">
    {#if !app.status?.backups.length}
      <div class="empty">
        no backups configured — add <code>[[backup]]</code> sections to
        <code>config.toml</code> and restart the daemon
      </div>
    {:else}
      <table class="data">
        <thead>
          <tr>
            <th>Backup</th>
            <th>Paths</th>
            <th class="right">Replicas</th>
            <th class="right">Snapshots</th>
            <th>Last run</th>
            <th>Replication</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each app.status.backups as b (b.backup_id)}
            <tr class="clickable" onclick={() => navigate(`#/backups/${b.backup_id}`)}>
              <td>
                <div class="strong">{b.backup_id}</div>
                <div class="small faint mono">{cfgLine(b.backup_id)}</div>
              </td>
              <td class="mono small muted" style="max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap">
                {b.paths.join(', ')}
              </td>
              <td class="right">{b.replicas}</td>
              <td class="right">{b.snapshot_count}</td>
              <td class="small muted nowrap">
                {b.last_snapshot ? fmtAgo(b.last_snapshot.created_at) : 'never'}
              </td>
              <td><StatusPill health={b.health} /></td>
              <td class="right">
                <button
                  class="sm"
                  disabled={running !== null}
                  onclick={(e) => {
                    e.stopPropagation();
                    void run(b.backup_id);
                  }}
                >
                  {#if running === b.backup_id}<span class="spinner"></span>{:else}run now{/if}
                </button>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
</div>
