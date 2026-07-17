# burrow

**Distributed backup among friends.** You and your self-hosting friends reserve
slices of each other's disks; burrow keeps encrypted, deduplicated,
version-history backups of your folders replicated across their machines — and
theirs across yours. No cloud bill, no accounts, no port forwarding.

Built on [iroh](https://www.iroh.computer/): peers are cryptographic keys,
connections are end-to-end encrypted QUIC that hole-punches through NAT (with
public relay fallback), and blobs are content-addressed and BLAKE3-verified in
transit.

> **Built with AI assistance.** Burrow was developed with help from Anthropic's
> Claude models (via Claude Code). All code is human-reviewed and maintained by
> the author.

```
you                          anna's NAS                     ben's homelab
┌─────────────┐   sealed     ┌──────────────┐               ┌──────────────┐
│ ~/photos    │──chunks────▶ │ 200 GB grant │               │ 150 GB grant │
│ ~/documents │──────────────┼──────────────┼─────────────▶ │  (replica 2) │
│             │              │  (replica 1) │               │              │
│ 300 GB      │ ◀────────────│ anna's data  │ ◀─────────────│ ben's data   │
│ offered     │              └──────────────┘               └──────────────┘
└─────────────┘        everyone hosts everyone, nobody reads anything
```

## What it does

- **Restic-style snapshots**: content-defined chunking (FastCDC), incremental
  — only changed data uploads; identical data stored once.
- **Real end-to-end encryption**: chunks are sealed with XChaCha20-Poly1305
  before they leave your machine. Friends host ciphertext; only your 24-word
  recovery phrase decrypts it. Encryption is deterministic per repo key, so
  dedup and replica tracking survive even total metadata loss.
- **Pairwise storage contracts**: `burrow grant anna 200gb` reserves space
  unilaterally; `burrow request ben 100gb` asks (he approves). Give/take
  ratios are shown, never enforced — it's a friend group, not a market.
- **Per-backup redundancy**: `replicas = 3` keeps three copies on distinct
  peers. A placement planner spreads chunks by free space and liveness.
- **Self-healing**: peers offline past a grace period (default 72h) trigger
  re-replication. Random chunks are cryptographically spot-checked hourly —
  a verified range read is proof of possession, courtesy of BLAKE3 streaming.
- **Graceful shrink/revoke**: shrink a grant and the owner's daemon evacuates
  data elsewhere; forced eviction only after a deadline (default 14 days).
- **One identity, many devices**: your NAS, desktop, and laptop share one
  24-word phrase. `burrow device join` links a new machine with zero
  ceremony; friends approve *you* once and every device you add is trusted
  automatically (owner-signed device certificates). Laptops run in
  `client` mode (back up, never host); host devices serve your other
  devices with no grants needed.
- **Laptop-aware**: scheduled work defers on battery (`run_on_battery =
  false`), `burrow pause 2h` for tethered moments, and an unchanged-file
  cache so re-scanning a big tree reads only what changed.
- **Total disaster recovery**: with nothing but your recovery phrase and one
  friend's ticket, `burrow device join` + `burrow resync` rebuilds your
  entire catalog and `burrow restore` pulls everything back.

## Quick start

Both friends install burrow, then:

```console
# each machine, once
$ burrow init                       # writes keys, prints your RECOVERY PHRASE
$ $EDITOR ~/.config/burrow/config.toml   # declare what to back up
$ burrow daemon run &               # or systemd/launchd, see contrib/

# pair up (send the ticket over Signal/whatever)
you  $ burrow peer invite
anna $ burrow peer add <ticket> --name you
you  $ burrow requests              # see anna's request
you  $ burrow approve anna

# trade space
you  $ burrow grant anna 200gb
anna $ burrow grant you 150gb

# back up
you  $ burrow backup run photos
you  $ burrow status
BACKUP   REPLICAS  SNAPSHOTS  LAST RUN             REPLICATION   PATHS
photos   2         1          2026-07-14 03:00:12  healthy       /home/you/photos
```

Config (`~/.config/burrow/config.toml`, see
[contrib/config.example.toml](contrib/config.example.toml)):

```toml
[node]
name = "you"                 # your name, shown to friends (owner-level)

[device]
mode = "host"                # "client" on laptops: backs up, never hosts
run_on_battery = true        # false on laptops: defer scheduled work

[storage]
offer_max = "500gb"          # ceiling across all grants you give

[[backup]]
id = "photos"
paths = ["/home/you/photos"]
exclude = ["*.tmp", ".cache/**"]
replicas = 3                 # copies across distinct devices (yours count)
min_offsite = 1              # at least this many on OTHER people's machines
schedule = "0 3 * * *"       # 5-field crontab
keep_last = 30               # prune older snapshots
```

## Your own devices

```console
nas    $ burrow device link            # prints a ticket
laptop $ burrow device join <ticket> --device laptop
         # asks for your 24-word phrase — same identity, new device
laptop $ burrow status                 # NAS appears under MY DEVICES
```

Devices of one owner need no grants or approvals between them: host devices
serve their owner automatically, friends any device adds are known to all of
them, and a friend's grant to *you* is usable from every device. `replicas`
counts your own devices; `min_offsite` guarantees copies leave the building.

## Restore

```console
$ burrow snapshots photos
$ burrow restore photos --target /tmp/get-it-back          # latest
$ burrow restore photos --snapshot 1784018594 --target ...  # point in time
```

Restore prefers local blobs and transparently fetches anything missing from
replica holders — it works even after your blob store is gone.

## Disaster recovery (the whole machine burned down)

Your recovery phrase recovers **everything**: the repo key decrypts your data,
and your node identity is derived from it, so friends' daemons recognize the
recovered machine automatically.

```console
$ burrow recover                     # enter your 24 words
$ burrow daemon run &
$ burrow peer add <any-friend's-ticket> --name anna
$ burrow resync                      # rebuild catalog from what peers hold
$ burrow restore photos --target ~/photos
```

**Write the phrase down. Store it off-machine.** Without it your backups are
noise; with it anyone can read them.

## Web UI

An optional web UI mirrors everything the CLI can do — overview, backups and
restores, friends and space requests, devices, storage. It's off by default
and purely additive: the daemon runs identically without it.

```toml
# ~/.config/burrow/config.toml
[web]
enable = true
bind = "127.0.0.1:8385"   # default
```

Restart the daemon and open http://127.0.0.1:8385. Loopback browsers are
trusted (same model as the control socket). To reach it from another machine
(LAN, Tailscale), bind e.g. `0.0.0.0:8385` — non-loopback clients must send
the auto-generated token (`burrow web token` prints it; stored `0600` in
`~/.config/burrow/web.token`).

**Reverse-proxy warning**: loopback trust is based on the client IP. If you
put the UI behind a same-host reverse proxy (nginx, Caddy), every remote
client arrives as `127.0.0.1` and would be trusted — set
`trust_loopback = false` under `[web]` so all clients need the token.

Docker: publish the port and enable in your mounted config, e.g.
`docker run -p 8385:8385 …` with `bind = "0.0.0.0:8385"`; the token lives in
the config volume (`docker exec burrow burrow web token`).

The UI is a Svelte SPA embedded in the binary; released binaries, Docker
images, and the `prebuilt` image target ship it prebuilt. From a source
checkout, `cargo build` embeds a placeholder page until you build the
frontend: `cd web && npm install && npm run build` (vite dev server: `npm run
dev` proxies the API to a locally-running daemon). Lean builds drop the whole
feature: `cargo build --no-default-features`. The JSON API lives under
`/api/v1/` and works regardless of which page is served.

## Commands

| | |
|---|---|
| `burrow init` / `recover` | create / recover keys |
| `burrow device link/join/list` | one identity across your machines |
| `burrow pause [2h]` / `resume` | suspend scheduled work |
| `burrow daemon run` | run the daemon (foreground) |
| `burrow status` / `doctor` | health overview / diagnostics |
| `burrow peer invite/add/remove`, `peers` | manage friends |
| `burrow requests`, `approve`, `deny` | pending peerings & space requests |
| `burrow grant <peer> <size>` | reserve space for a friend (0 = revoke) |
| `burrow request <peer> <size>` | ask a friend for space |
| `burrow backup run <id>`, `snapshots` | snapshot now / list history |
| `burrow restore <id> [--snapshot ts] --target <dir>` | get data back |
| `burrow repair` / `resync` | force verify+re-replicate / rebuild catalog |
| `burrow web token` | print the web UI access token |
| `burrow key phrase` | reprint the recovery phrase |

## NixOS

The flake ships a package and a first-class module:

```nix
{
  inputs.burrow.url = "github:solidsilver/burrow";

  # in your configuration:
  imports = [ burrow.nixosModules.burrow ];
  services.burrow = {
    enable = true;
    settings = {
      node.name = "my-nas";
      storage.offer_max = "500gb";
      backup = [{
        id = "photos";
        paths = [ "/tank/photos" ];
        replicas = 3;
        schedule = "0 3 * * *";
      }];
    };
  };
}
```

The repo key is *state*, not config: run `burrow init` once as the service
user and stash the phrase. systemd (`contrib/burrow.service`) and launchd
(`contrib/com.burrow.daemon.plist`) units are provided for everyone else.

## Docker

Multi-arch images (`amd64` + `arm64`) are published to the GitHub Container
Registry on every release. `burrow` is the image entrypoint, and it keeps its
secret repo key and your friends' data on disk, so mount two volumes and run
`burrow init` once before starting the daemon:

```console
# one-time: writes the repo key and prints your RECOVERY PHRASE
# (store it OFF this machine — it is the only thing that decrypts your backups)
$ docker run --rm -it \
    -v burrow-config:/etc/burrow -v burrow-data:/var/lib/burrow \
    ghcr.io/solidsilver/burrow init

# run the daemon
$ docker run -d --name burrow --restart unless-stopped \
    -v burrow-config:/etc/burrow -v burrow-data:/var/lib/burrow \
    ghcr.io/solidsilver/burrow

# any CLI command runs against the same env
$ docker exec burrow burrow peer invite
$ docker exec burrow burrow status
```

No inbound ports are needed — iroh hole-punches outbound with relay fallback.
On a Linux server, add `--network host` for the best direct-connection rate
(it lets iroh enumerate the real interfaces; the flag is a no-op on Docker
Desktop for Mac/Windows). The image runs as a non-root user (uid 10001): named
volumes inherit the right ownership automatically, but a *bind* mount must be
`chown`ed to `10001:10001` first. Blobs can go on a separate pool via
`BURROW_BLOBS_DIR` and a third volume.

Build it yourself with `docker build -t burrow .` (compiles from source); the
published images are built with `--target prebuilt` from the release binary.

## Security model

- Peers are Ed25519 keys (iroh endpoint IDs); every connection is mutually
  authenticated and encrypted (QUIC/TLS).
- Data is sealed *before* leaving your machine:
  `chunk_key = BLAKE3-derive(repo_key ‖ keyed-hash(repo_key, plaintext))`,
  XChaCha20-Poly1305. Deterministic per repo key (stable content addresses,
  index-loss-proof), but keyed — no public convergent-encryption attacks.
  Holders learn only ciphertext sizes and equality among *your own* chunks.
- Blob access is gated per peer per hash: friends can fetch only blobs they
  own (theirs, stored on you) or replicas you asked them to hold.
- Threat model: honest-but-curious friends. Spot-checks catch bit rot and
  quietly deleted data; there are no Byzantine-fault incentives — if you don't
  trust someone with your ciphertext, don't peer with them.

## How it works

Each machine runs one daemon: an iroh endpoint speaking three protocols —
iroh-blobs (data plane, per-peer authorized), a small control protocol
(contracts, quotas, store/release requests), and a local unix socket for the
CLI. Metadata lives in SQLite; blobs in an iroh-blobs store with GC protection
driven by that metadata. Replication is pull-based: you ask a peer to hold a
blob, *they* fetch it from you (quota-checked, resumable, verified), and only
then is the replica counted. A planner converges placements toward each
backup's replica target; verification, repair, evacuation, and pruning run as
background loops.

## Limitations

- **Device names must be unique among your machines.** A device's identity is
  derived from your phrase + its name, so two machines joined under the same
  name (e.g. both defaulting to the hostname `macbook`) share one identity and
  peers cannot tell them apart. Pass `--device <name>` when joining.
- Non-UTF-8 filenames are stored lossily (restored under a sanitized name);
  hardlinks are stored and restored as independent copies; sockets, FIFOs and
  device nodes are skipped.
- Exclude patterns: without `/` a pattern matches a path component at any
  depth (`node_modules`, `*.tmp`); with `/` it anchors to the backup root and
  `*` stops at separators (`.cache/**`).
- A scheduled backup that has *never* run waits for its first cron slot; after
  that, slots missed while the machine was off fire at the next daemon start.

## Building from source

```console
$ cargo build --release          # → target/release/burrow
$ cargo test --workspace
```

Rust 1.85+. Tested on Linux (x86_64, aarch64) and macOS.

## License

MIT or Apache-2.0, at your option.
