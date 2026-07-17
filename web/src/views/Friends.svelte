<script lang="ts">
  import { api, type PeerInfo } from '../lib/api.svelte';
  import { act, app, toast } from '../lib/state.svelte';
  import { fmtAgo, fmtBytes, parseSize, ratio } from '../lib/format';
  import Modal from '../components/Modal.svelte';
  import UsageBar from '../components/UsageBar.svelte';

  let friends = $derived(app.peers.filter((p) => p.state !== 'self'));

  // Invite / add
  let inviteTicket = $state<string | null>(null);
  let addOpen = $state(false);
  let addTicket = $state('');
  let addName = $state('');

  // Grant / request
  let grantPeer = $state<PeerInfo | null>(null);
  let grantSize = $state('');
  let grantMode = $state<'give' | 'ask'>('give');
  let confirmingRevoke = $state(false);

  // Remove
  let removingPeer = $state<PeerInfo | null>(null);

  let busy = $state(false);

  async function invite() {
    busy = true;
    try {
      inviteTicket = (await api.invite()).ticket;
    } catch (e) {
      toast('err', e instanceof Error ? e.message : String(e));
    } finally {
      busy = false;
    }
  }

  function copy(text: string) {
    navigator.clipboard
      .writeText(text)
      .then(() => toast('ok', 'copied to clipboard'))
      .catch(() => toast('err', 'copy failed — select the text manually'));
  }

  async function addFriend() {
    busy = true;
    try {
      await act(() => api.peerAdd(addTicket.trim(), addName.trim()), 'peer added');
      addOpen = false;
      addTicket = addName = '';
    } finally {
      busy = false;
    }
  }

  function openGrant(peer: PeerInfo, mode: 'give' | 'ask') {
    grantPeer = peer;
    grantMode = mode;
    grantSize = mode === 'give' && peer.given_bytes > 0 ? fmtBytes(peer.given_bytes) : '';
    confirmingRevoke = false;
  }

  async function submitGrant() {
    const bytes = parseSize(grantSize);
    if (bytes === null) {
      toast('err', `can't parse size "${grantSize}" (try "200gb")`);
      return;
    }
    if (!grantPeer) return;
    busy = true;
    const peer = grantPeer;
    try {
      if (grantMode === 'give') {
        await act(() => api.grant(peer.name, bytes), 'grant updated');
      } else {
        await act(() => api.requestSpace(peer.name, bytes), 'request sent');
      }
      grantPeer = null;
    } finally {
      busy = false;
    }
  }

  async function revokeGrant() {
    if (!grantPeer) return;
    busy = true;
    try {
      await act(() => api.grant(grantPeer!.name, 0), 'grant revoked');
      grantPeer = null;
    } finally {
      busy = false;
    }
  }

  async function removePeer() {
    if (!removingPeer) return;
    busy = true;
    try {
      await act(() => api.peerRemove(removingPeer!.name), 'peer removed');
      removingPeer = null;
    } finally {
      busy = false;
    }
  }

  function onlineCount(peer: PeerInfo): string {
    const devices = peer.devices;
    const online = devices.filter((d) => d.online === true).length;
    if (!devices.length) return 'no devices';
    return `${online}/${devices.length} devices online`;
  }
</script>

<div class="stack-lg">
  <div class="page-head">
    <div>
      <h1>Friends</h1>
      <div class="sub">pairwise storage contracts — shown, never enforced</div>
    </div>
    <div class="row">
      <button class="sm" disabled={busy} onclick={invite}>invite…</button>
      <button class="primary sm" disabled={busy} onclick={() => (addOpen = true)}>add friend…</button>
    </div>
  </div>

  {#if app.pendingPeers.length || app.spaceRequests.length}
    <section class="card stack" style="border-color: var(--accent-dim)">
      <h2>Inbox</h2>
      {#each app.pendingPeers as p (p.owner_pk.join(','))}
        <div class="row between wrap">
          <div>
            <span class="strong">{p.name}</span>
            <span class="muted small"> wants to peer with you</span>
          </div>
          <div class="row">
            <button class="primary sm" disabled={busy} onclick={() => act(() => api.approve(p.name), 'approved')}>
              approve
            </button>
            <button class="danger sm" disabled={busy} onclick={() => act(() => api.deny(p.name), 'denied')}>
              deny
            </button>
          </div>
        </div>
      {/each}
      {#each app.spaceRequests as r (r.peer_name)}
        <div class="row between wrap">
          <div>
            <span class="strong">{r.peer_name}</span>
            <span class="muted small">
              asks for <span class="strong">{fmtBytes(r.bytes)}</span> · gives {fmtBytes(r.given_total)},
              gets {fmtBytes(r.received_total)} · {fmtAgo(r.requested_at)}
            </span>
          </div>
          <div class="row">
            <button
              class="primary sm"
              disabled={busy}
              onclick={() => {
                const peer = friends.find((f) => f.name === r.peer_name);
                if (peer) openGrant(peer, 'give');
                grantSize = fmtBytes(r.bytes);
              }}
            >
              grant…
            </button>
            <button class="danger sm" disabled={busy} onclick={() => act(() => api.deny(r.peer_name), 'cleared')}>
              deny
            </button>
          </div>
        </div>
      {/each}
    </section>
  {/if}

  <div class="card card-pad-sm">
    {#if !friends.length}
      <div class="empty">
        <p>no friends yet — burrow is backup <em>among friends</em></p>
        <p class="small">
          <button class="sm" onclick={invite}>invite…</button> to get your ticket, then they add you
          with it (or you add theirs)
        </p>
      </div>
    {:else}
      <table class="data">
        <thead>
          <tr>
            <th>Friend</th>
            <th>You give (used)</th>
            <th>You get (used)</th>
            <th>Ratio</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each friends as p (p.owner_pk.join(','))}
            <tr>
              <td>
                <div class="strong">{p.name}</div>
                <div class="small faint">
                  {p.state === 'pending_in' ? 'pending your approval' : onlineCount(p)}
                  {#if p.approved_by_them === false}· awaiting their approval{/if}
                </div>
              </td>
              <td style="min-width: 150px">
                {#if p.given_bytes > 0}
                  <UsageBar used={p.given_used} total={p.given_bytes} />
                {:else}
                  <span class="faint small">—</span>
                {/if}
              </td>
              <td style="min-width: 150px">
                {#if p.received_bytes > 0}
                  <UsageBar used={p.received_used} total={p.received_bytes} />
                {:else}
                  <span class="faint small">—</span>
                {/if}
              </td>
              <td class="small muted nowrap">{ratio(p.given_bytes, p.received_bytes)}</td>
              <td class="right nowrap">
                <button class="sm ghost" onclick={() => openGrant(p, 'give')}>grant</button>
                <button class="sm ghost" onclick={() => openGrant(p, 'ask')}>request</button>
                <button class="sm ghost danger" onclick={() => (removingPeer = p)}>remove</button>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
</div>

{#if inviteTicket}
  <Modal title="Your pairing ticket" onclose={() => (inviteTicket = null)}>
    <div class="stack">
      <p class="small muted" style="margin: 0">
        Send this to your friend over any channel (Signal, email…). They run
        <code>burrow peer add &lt;ticket&gt; --name you</code>, or paste it under <em>add friend</em>
        in their own UI.
      </p>
      <div class="ticket-box">{inviteTicket}</div>
      <div class="row" style="justify-content: flex-end">
        <button class="primary sm" onclick={() => copy(inviteTicket!)}>copy</button>
      </div>
    </div>
  </Modal>
{/if}

{#if addOpen}
  <Modal title="Add a friend" onclose={() => (addOpen = false)}>
    <form
      class="stack"
      onsubmit={(e) => {
        e.preventDefault();
        void addFriend();
      }}
    >
      <label class="field">
        their ticket
        <textarea class="mono" rows="4" bind:value={addTicket} spellcheck="false" required></textarea>
      </label>
      <label class="field">
        nickname for them (yours only)
        <input type="text" bind:value={addName} placeholder="anna" required spellcheck="false" />
      </label>
      <div class="row" style="justify-content: flex-end">
        <button type="button" onclick={() => (addOpen = false)}>cancel</button>
        <button class="primary" type="submit" disabled={busy || !addTicket.trim() || !addName.trim()}>
          add friend
        </button>
      </div>
    </form>
  </Modal>
{/if}

{#if grantPeer}
  <Modal
    title={grantMode === 'give' ? `Space for ${grantPeer.name}` : `Ask ${grantPeer.name} for space`}
    onclose={() => (grantPeer = null)}
  >
    <form
      class="stack"
      onsubmit={(e) => {
        e.preventDefault();
        void submitGrant();
      }}
    >
      {#if grantMode === 'give'}
        <p class="small muted" style="margin: 0">
          Currently reserving <span class="strong">{fmtBytes(grantPeer.given_bytes)}</span> for them,
          of which they use {fmtBytes(grantPeer.given_used)}. Shrinking below usage starts a graceful
          evacuation on their side.
        </p>
      {:else}
        <p class="small muted" style="margin: 0">
          They currently reserve <span class="strong">{fmtBytes(grantPeer.received_bytes)}</span> for
          you; you use {fmtBytes(grantPeer.received_used)}. Requests show up in their inbox.
        </p>
      {/if}
      <label class="field">
        size (e.g. 200gb, 1.5tb)
        <input type="text" bind:value={grantSize} placeholder="200gb" required spellcheck="false" />
      </label>
      <div class="row wrap" style="gap: 6px">
        {#each ['50gb', '100gb', '200gb', '500gb'] as preset (preset)}
          <button type="button" class="sm ghost" onclick={() => (grantSize = preset)}>{preset}</button>
        {/each}
      </div>
      <div class="row between">
        {#if grantMode === 'give' && grantPeer.given_bytes > 0}
          {#if confirmingRevoke}
            <button type="button" class="danger sm" disabled={busy} onclick={revokeGrant}>
              really revoke (evacuates their data)?
            </button>
          {:else}
            <button type="button" class="ghost danger sm" onclick={() => (confirmingRevoke = true)}>
              revoke…
            </button>
          {/if}
        {:else}
          <span></span>
        {/if}
        <div class="row">
          <button type="button" onclick={() => (grantPeer = null)}>cancel</button>
          <button class="primary" type="submit" disabled={busy || !grantSize.trim()}>
            {grantMode === 'give' ? 'set grant' : 'send request'}
          </button>
        </div>
      </div>
    </form>
  </Modal>
{/if}

{#if removingPeer}
  <Modal title="Remove {removingPeer.name}?" onclose={() => (removingPeer = null)}>
    <div class="stack">
      <p class="small muted" style="margin: 0">
        This removes the peering and all their devices. Any of their data you hold is released;
        your replicas on their machines will be re-placed elsewhere by the repair loop.
      </p>
      <div class="row" style="justify-content: flex-end">
        <button onclick={() => (removingPeer = null)}>cancel</button>
        <button class="danger" disabled={busy} onclick={removePeer}>remove {removingPeer.name}</button>
      </div>
    </div>
  </Modal>
{/if}
