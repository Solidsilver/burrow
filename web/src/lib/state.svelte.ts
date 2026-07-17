// Global reactive store + polling. Views read from `app`; nothing here is
// sacred — any action can call refresh() to re-pull immediately.

import {
  api,
  ApiError,
  type BackupConfigView,
  type PeerInfo,
  type ServerInfo,
  type SnapshotInfo,
  type SpaceRequestInfo,
  type StatusInfo,
} from './api.svelte';

export type Route =
  | { view: 'overview' }
  | { view: 'backups' }
  | { view: 'backup'; id: string }
  | { view: 'friends' }
  | { view: 'devices' }
  | { view: 'storage' };

function parseRoute(): Route {
  const parts = location.hash.replace(/^#\/?/, '').split('/').filter(Boolean);
  if (parts[0] === 'backups' && parts[1]) return { view: 'backup', id: decodeURIComponent(parts[1]) };
  if (parts[0] === 'backups') return { view: 'backups' };
  if (parts[0] === 'friends') return { view: 'friends' };
  if (parts[0] === 'devices') return { view: 'devices' };
  if (parts[0] === 'storage') return { view: 'storage' };
  return { view: 'overview' };
}

export interface Toast {
  id: number;
  kind: 'ok' | 'err';
  text: string;
}

export const app = $state({
  route: parseRoute(),
  server: null as ServerInfo | null,
  status: null as StatusInfo | null,
  peers: [] as PeerInfo[],
  pendingPeers: [] as PeerInfo[],
  spaceRequests: [] as SpaceRequestInfo[],
  snapshots: [] as SnapshotInfo[],
  /** Configured [[backup]] sections (schedule/retention/excludes). */
  backupConfigs: [] as BackupConfigView[],
  /** True once the first successful load completed (drives the boot screen). */
  loaded: false,
  /** Set when the API answers 401: show the token gate. */
  needsAuth: false,
  /** Daemon unreachable (network-level failure). */
  unreachable: null as string | null,
  toasts: [] as Toast[],
});

let toastSeq = 0;

export function toast(kind: 'ok' | 'err', text: string) {
  const id = ++toastSeq;
  app.toasts.push({ id, kind, text });
  setTimeout(() => {
    const i = app.toasts.findIndex((t) => t.id === id);
    if (i >= 0) app.toasts.splice(i, 1);
  }, 6000);
}

export function navigate(to: string) {
  location.hash = to;
}

export async function refresh(opts: { slow?: boolean } = {}) {
  try {
    const [server, status] = await Promise.all([api.server(), api.status()]);
    app.server = server;
    app.status = status;
    app.loaded = true;
    app.needsAuth = false;
    app.unreachable = null;
  } catch (e) {
    if (e instanceof ApiError && e.status === 401) {
      app.needsAuth = true;
      return;
    }
    app.unreachable = e instanceof Error ? e.message : String(e);
    return;
  }
  // Peer endpoints do live reachability probes on the daemon — keep them on
  // the slower cadence unless asked for right now.
  if (opts.slow) {
    try {
      const [peers, pending, snapshots, backupConfigs] = await Promise.all([
        api.peers(),
        api.pending(),
        api.snapshots(),
        api.backupConfigs(),
      ]);
      app.peers = peers;
      app.pendingPeers = pending.peers;
      app.spaceRequests = pending.space_requests;
      app.snapshots = snapshots.sort((a, b) => b.created_at - a.created_at);
      app.backupConfigs = backupConfigs;
    } catch (e) {
      // A slow-path failure shouldn't blank the fast data already shown.
      console.warn('slow refresh failed', e);
    }
  }
}

let started = false;

/** Begin fast (5s: server+status) and slow (15s: peers/pending/snapshots)
 * polling, pausing while the tab is hidden. Call once from App. */
export function startPolling() {
  if (started) return;
  started = true;
  window.addEventListener('hashchange', () => (app.route = parseRoute()));

  const tick = async () => {
    if (document.hidden) return;
    await refresh({ slow: true });
  };
  void refresh({ slow: true });
  setInterval(() => void refresh(), 5000);
  setInterval(() => void tick(), 15000);
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) void refresh({ slow: true });
  });
}

/** Run an action, surface the result, then refresh everything. */
export async function act(work: () => Promise<{ message?: string } | unknown>, what: string) {
  try {
    const result = await work();
    const msg =
      result && typeof result === 'object' && 'message' in result
        ? String((result as { message: unknown }).message)
        : what;
    toast('ok', msg);
  } catch (e) {
    toast('err', e instanceof Error ? e.message : String(e));
  } finally {
    await refresh({ slow: true });
  }
}
