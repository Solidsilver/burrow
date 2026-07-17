// Presentation helpers mirroring the CLI's fmt_size / fmt_time, plus health
// summary logic kept in sync with ReplicationHealth::summary() in
// crates/burrow-proto/src/ctrl.rs.

import type { ReplicationHealth } from './api.svelte';

/** Human-readable decimal size, matching the daemon's fmt_size. */
export function fmtBytes(n: number): string {
  const units: [string, number][] = [
    ['TB', 1_000_000_000_000],
    ['GB', 1_000_000_000],
    ['MB', 1_000_000],
    ['KB', 1_000],
  ];
  for (const [unit, mult] of units) {
    if (n >= mult) {
      const v = n / mult;
      return v >= 100 ? `${v.toFixed(0)} ${unit}` : `${v.toFixed(1)} ${unit}`;
    }
  }
  return `${n} B`;
}

/** Local-time "2026-07-14 03:00:12", like the CLI. */
export function fmtTime(unix: number): string {
  const d = new Date(unix * 1000);
  const p = (x: number) => String(x).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

/** "3h ago" / "2d ago" / "just now". */
export function fmtAgo(unix: number): string {
  const s = Math.max(0, Math.floor(Date.now() / 1000) - unix);
  if (s < 60) return 'just now';
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

/** Remaining time until a unix deadline ("2h left", "until resumed"). */
export function fmtUntil(unix: number): string {
  if (unix >= Number.MAX_SAFE_INTEGER || unix >= 2 ** 63) return 'until resumed';
  const s = Math.max(0, unix - Math.floor(Date.now() / 1000));
  if (s < 60) return `${s}s left`;
  if (s < 3600) return `${Math.ceil(s / 60)}m left`;
  if (s < 86400) return `${Math.ceil(s / 3600)}h left`;
  return `${Math.ceil(s / 86400)}d left`;
}

export function hex(bytes: number[]): string {
  return bytes.map((b) => b.toString(16).padStart(2, '0')).join('');
}

export function shortHex(bytes: number[]): string {
  return hex(bytes).slice(0, 8);
}

export type HealthLevel = 'ok' | 'warn' | 'crit' | 'muted';

/** Mirrors ReplicationHealth::summary() on the daemon. */
export function healthSummary(h: ReplicationHealth): { label: string; level: HealthLevel } {
  if (h.total_blobs === 0) return { label: 'no data yet', level: 'muted' };
  if (h.critical === h.total_blobs) return { label: 'local only', level: 'warn' };
  if (h.satisfied === h.total_blobs) return { label: 'healthy', level: 'ok' };
  if (h.critical > 0)
    return { label: `CRITICAL (${h.critical}/${h.total_blobs} unreplicated)`, level: 'crit' };
  return { label: `degraded (${h.degraded}/${h.total_blobs} below target)`, level: 'warn' };
}

/** Parse human sizes like the daemon ("500gb", "1.5tb", "100 MiB"). */
export function parseSize(s: string): number | null {
  const clean = s.trim().toLowerCase().replace(/\s+/g, '');
  const m = /^([0-9]+(?:\.[0-9]+)?)([a-z]*)$/.exec(clean);
  if (!m) return null;
  const value = Number(m[1]);
  if (!Number.isFinite(value) || value < 0) return null;
  const mults: Record<string, number> = {
    '': 1,
    b: 1,
    kb: 1e3,
    mb: 1e6,
    gb: 1e9,
    tb: 1e12,
    kib: 1 << 10,
    mib: 1 << 20,
    gib: 1 << 30,
    tib: 1 << 40,
  };
  const mult = mults[m[2]];
  if (mult === undefined) return null;
  return Math.floor(value * mult);
}

/** Friendly give/take ratio: 1.0 = balanced. */
export function ratio(given: number, received: number): string {
  if (given === 0 && received === 0) return '—';
  if (received === 0) return '∞ give';
  if (given === 0) return '∞ take';
  const r = given / received;
  return r >= 0.95 && r <= 1.05 ? 'balanced' : r > 1 ? `${r.toFixed(1)}× give` : `${(1 / r).toFixed(1)}× take`;
}

/** "1 snapshot", "3 snapshots". */
export function plural(n: number, word: string): string {
  return `${n} ${word}${n === 1 ? '' : 's'}`;
}
