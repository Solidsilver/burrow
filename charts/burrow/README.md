# burrow Helm chart

Runs the [burrow](https://github.com/solidsilver/burrow) daemon on Kubernetes
as a single-replica StatefulSet, using Kubernetes-native resources:

| burrow needs | Kubernetes resource |
|---|---|
| 24-word recovery phrase | `Secret` → init container derives the repo key on first boot |
| `config.toml` | `ConfigMap`, mounted read-only, pod rolls on change |
| identity + metadata + blobs | `volumeClaimTemplates` on the StatefulSet |
| folders to back up | `hostPath` or existing PVC mounts, read-only |
| web UI (optional) | `Service` + `Ingress` (TLS terminates at the ingress) |
| iroh peer traffic | nothing — hole-punches outbound, no inbound ports |

No root anywhere: the pod runs as uid/gid 10001 with `fsGroup`, a
`RuntimeDefault` seccomp profile, and dropped capabilities (PodSecurity
"restricted" compatible).

## Prerequisites

- Kubernetes 1.23+ with a default StorageClass (or set `persistence.*.storageClass`)
- Helm 3
- A burrow identity (24-word recovery phrase). Generate one on any machine:

  ```console
  $ docker run --rm ghcr.io/solidsilver/burrow:0.2.2 init
  ```

  Write the phrase down offline — it is the only thing that decrypts your
  backups.

## Install

```console
# 1. Put the phrase in a Secret (or use SOPS / Sealed Secrets / ESO)
$ kubectl create namespace burrow
$ kubectl -n burrow create secret generic burrow-identity \
    --from-literal=recovery-phrase='word1 word2 ... word24'

# 2. Install from a source checkout
$ helm install burrow ./charts/burrow -n burrow \
    --set burrow.existingSecret=burrow-identity

# 3. Watch first boot (init container recovers the identity)
$ kubectl -n burrow logs burrow-0 -c bootstrap
$ kubectl -n burrow exec burrow-0 -- burrow status
```

Alternative: `--set burrow.recoveryPhrase='word1 ...'` lets the chart create
the Secret — convenient for a test run, but the phrase then sits in your
shell history / values files. Prefer `existingSecret`.

## Backing up host folders

The pod can only back up what it can mount. Mount each source folder at the
**same path** you list under `[[backup]]` in `config`, and pin the pod to
the node that holds the data:

```yaml
backupSources:
  - name: photos
    hostPath: /tank/photos
    readOnly: true
nodeSelector:
  kubernetes.io/hostname: nas

config: |
  [[backup]]
  id = "photos"
  paths = ["/tank/photos"]
  replicas = 3
  schedule = "0 3 * * *"
```

`existingClaim: <pvc>` works too, for backing up other PVCs.

## Web UI

```yaml
config: |
  [web]
  enable = true
  bind = "0.0.0.0:8385"
  trust_loopback = false
  allowed_hosts = ["burrow.example.com"]

web:
  service:
    enabled: true
  ingress:
    enabled: true
    className: nginx
    hosts:
      - host: burrow.example.com
        paths: [{ path: /, pathType: Prefix }]
```

The API is plain HTTP with a bearer token — terminate TLS at the ingress or
reach the UI over Tailscale. Print the token with
`kubectl exec burrow-0 -- burrow web token`.

## Day-to-day

Every CLI command runs against the pod:

```console
$ kubectl -n burrow exec burrow-0 -- burrow peer invite
$ kubectl -n burrow exec burrow-0 -- burrow grant anna 200gb
$ kubectl -n burrow exec burrow-0 -- burrow snapshots photos
$ kubectl -n burrow exec burrow-0 -- burrow restore photos --target /tmp/restore
```

## Disaster recovery

PVCs are kept on `helm uninstall` — reinstalling reattaches the same
identity. Total cluster loss: create a fresh Secret from your offline phrase
copy and reinstall; the init container recovers the key, then
`burrow resync` rebuilds your catalog from what friends hold.

## Notes & caveats

- **Never scale beyond 1 replica.** One repo key = one node identity; two
  pods sharing it corrupt the SQLite metadata and confuse every peer.
- Prefer block/local storage over NFS for the data volume (SQLite locking).
- Egress must allow outbound UDP (QUIC) and TCP — iroh hole-punches and
  falls back to public relays; no inbound ports are needed.
- The device name defaults to the stable pod hostname; set
  `burrow.deviceName` if you ever plan to rename the release (device names
  are part of the identity and can't be renamed later).
