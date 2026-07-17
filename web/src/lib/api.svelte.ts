// Types mirror the serde structs in crates/burrow-proto/src/ctrl.rs — the
// daemon's JSON API serializes them verbatim. [u8; 32] arrives as number[].

export interface ServerInfo {
  version: string;
  bind: string;
  paused_until: number | null;
}

export interface StatusInfo {
  node_name: string;
  device_name: string;
  mode: string;
  version: string;
  data_dir: string;
  endpoint_id: number[];
  owner_pk: number[];
  backups: BackupStatus[];
  hosting: HostingInfo;
}

export interface HostingInfo {
  offer_max: number | null;
  held_total: number;
  grants: [string, number, number][];
}

export interface BackupStatus {
  backup_id: string;
  paths: string[];
  replicas: number;
  snapshot_count: number;
  last_snapshot: SnapshotInfo | null;
  health: ReplicationHealth;
}

export interface ReplicationHealth {
  total_blobs: number;
  satisfied: number;
  degraded: number;
  critical: number;
}

export interface SnapshotInfo {
  backup_id: string;
  created_at: number;
  manifest_hash: number[];
  file_count: number;
  bytes_scanned: number;
  bytes_new: number;
  chunk_count: number;
  files_cached: number;
}

export interface PeerInfo {
  name: string;
  owner_pk: number[];
  /** "active" | "pending_in" | "self" */
  state: string;
  given_bytes: number;
  given_used: number;
  received_bytes: number;
  received_used: number;
  approved_by_them: boolean | null;
  devices: DeviceInfo[];
}

export interface DeviceInfo {
  device_name: string;
  endpoint_id: number[];
  mode: string;
  last_seen: number | null;
  online: boolean | null;
}

export interface SpaceRequestInfo {
  peer_name: string;
  bytes: number;
  given_total: number;
  received_total: number;
  requested_at: number;
}

export interface PendingResponse {
  peers: PeerInfo[];
  space_requests: SpaceRequestInfo[];
}

/** View of a configured [[backup]] section (GET /backups). */
export interface BackupConfigView {
  id: string;
  paths: string[];
  exclude: string[];
  replicas: number;
  schedule: string | null;
  keep_last: number | null;
  min_offsite: number;
}

// ---------------------------------------------------------------------------

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
  ) {
    super(message);
  }
}

let token = $state(localStorage.getItem('burrow.token') ?? '');

export function getToken(): string {
  return token;
}

export function setToken(t: string) {
  token = t;
  if (t) localStorage.setItem('burrow.token', t);
  else localStorage.removeItem('burrow.token');
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(`/api/v1${path}`, {
    method,
    headers: {
      ...(body !== undefined ? { 'content-type': 'application/json' } : {}),
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    let message = `${res.status} ${res.statusText}`;
    try {
      const data = await res.json();
      if (typeof data?.error === 'string') message = data.error;
      else if (typeof data === 'string') message = data;
    } catch {
      const text = await res.text().catch(() => '');
      if (text) message = text;
    }
    throw new ApiError(message, res.status);
  }
  return (await res.json()) as T;
}

const get = <T>(path: string) => request<T>('GET', path);
const post = <T>(path: string, body?: unknown) =>
  request<T>('POST', path, body ?? {});

/** Peer names/backup ids are user-controlled — always encode path params. */
const enc = encodeURIComponent;

export const api = {
  server: () => get<ServerInfo>('/server'),
  status: () => get<StatusInfo>('/status'),
  peers: () => get<PeerInfo[]>('/peers'),
  pending: () => get<PendingResponse>('/pending'),
  snapshots: () => get<SnapshotInfo[]>('/snapshots'),
  backupConfigs: () => get<BackupConfigView[]>('/backups'),
  backupSnapshots: (id: string) => get<SnapshotInfo[]>(`/backups/${enc(id)}/snapshots`),
  backupRun: (id: string) => post<SnapshotInfo>(`/backups/${enc(id)}/run`),
  restore: (backup_id: string, snapshot: number | null, target: string) =>
    post<{ files: number; bytes: number; target: string }>('/restore', {
      backup_id,
      snapshot,
      target,
    }),
  invite: () => post<{ ticket: string }>('/peers/invite'),
  peerAdd: (ticket: string, name: string) =>
    post<{ message: string }>('/peers/add', { ticket, name }),
  peerRemove: (name: string) => post<{ message: string }>(`/peers/${enc(name)}/remove`),
  grant: (name: string, bytes: number) =>
    post<{ message: string }>(`/peers/${enc(name)}/grant`, { bytes }),
  requestSpace: (name: string, bytes: number) =>
    post<{ message: string }>(`/peers/${enc(name)}/request`, { bytes }),
  approve: (name: string) => post<{ message: string }>(`/requests/${enc(name)}/approve`),
  deny: (name: string) => post<{ message: string }>(`/requests/${enc(name)}/deny`),
  pause: (seconds: number | null) => post<{ message: string }>('/pause', { seconds }),
  resume: () => post<{ message: string }>('/resume'),
  repair: () => post<{ message: string }>('/repair'),
  resync: () => post<{ message: string }>('/resync'),
  deviceJoin: (ticket: string) => post<{ message: string }>('/devices/join', { ticket }),
};
