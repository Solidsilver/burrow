<script lang="ts">
  import { api } from '../lib/api.svelte';
  import { act, app, toast } from '../lib/state.svelte';
  import { fmtAgo, fmtTime, shortHex } from '../lib/format';
  import Modal from '../components/Modal.svelte';

  let me = $derived(app.peers.find((p) => p.state === 'self'));

  let linkTicket = $state<string | null>(null);
  let joinTicket = $state('');
  let busy = $state(false);

  async function link() {
    busy = true;
    try {
      linkTicket = (await api.invite()).ticket;
    } catch (e) {
      toast('err', e instanceof Error ? e.message : String(e));
    } finally {
      busy = false;
    }
  }

  async function join() {
    busy = true;
    try {
      await act(() => api.deviceJoin(joinTicket.trim()), 'device linked');
      joinTicket = '';
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
</script>

<div class="stack-lg">
  <div class="page-head">
    <div>
      <h1>Devices</h1>
      <div class="sub">one identity, many machines — same 24-word phrase everywhere</div>
    </div>
    <button class="sm" disabled={busy} onclick={link}>link new device…</button>
  </div>

  <div class="card card-pad-sm">
    {#if !me}
      <div class="empty"><span class="spinner"></span></div>
    {:else}
      <table class="data">
        <thead>
          <tr>
            <th>Device</th>
            <th>Mode</th>
            <th>Online</th>
            <th>Last seen</th>
            <th>Endpoint</th>
          </tr>
        </thead>
        <tbody>
          {#each me.devices as d (d.endpoint_id.join(','))}
            <tr>
              <td class="strong">
                {d.device_name}
                {#if d.device_name === app.status?.device_name}
                  <span class="pill accent" style="margin-left: 6px">this device</span>
                {/if}
              </td>
              <td class="small">{d.mode}</td>
              <td>
                {#if d.online === true}
                  <span class="pill ok"><span class="dot"></span>online</span>
                {:else if d.online === false}
                  <span class="pill muted"><span class="dot"></span>offline</span>
                {:else}
                  <span class="pill muted"><span class="dot"></span>—</span>
                {/if}
              </td>
              <td class="small muted nowrap" title={d.last_seen ? fmtTime(d.last_seen) : ''}>
                {d.last_seen ? fmtAgo(d.last_seen) : '—'}
              </td>
              <td class="mono small muted">{shortHex(d.endpoint_id)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>

  <div class="card stack">
    <h2>Join this machine to your identity</h2>
    <p class="small muted" style="margin: 0">
      Paste a ticket from one of your existing devices (<em>link new device</em> there). The
      joining machine must already share your 24-word recovery phrase — same identity, new
      device. Friends approve <em>you</em> once; every device you add is trusted automatically.
    </p>
    <form
      class="row"
      onsubmit={(e) => {
        e.preventDefault();
        void join();
      }}
    >
      <input
        type="text"
        class="mono grow"
        placeholder="ticket from `burrow device link`"
        bind:value={joinTicket}
        spellcheck="false"
      />
      <button class="primary" type="submit" disabled={busy || !joinTicket.trim()}>join</button>
    </form>
    <p class="small faint" style="margin: 0">
      device names must be unique among your machines — a second machine joined under an
      existing name derives the SAME identity and peers can't tell them apart
    </p>
  </div>
</div>

{#if linkTicket}
  <Modal title="Link a new device" onclose={() => (linkTicket = null)}>
    <div class="stack">
      <p class="small muted" style="margin: 0">
        On the new machine (after <code>burrow recover</code> with your phrase, or on first
        <code>device join</code>):
      </p>
      <p class="mono small" style="margin: 0">burrow device join &lt;ticket&gt; --device &lt;unique-name&gt;</p>
      <div class="ticket-box">{linkTicket}</div>
      <div class="row" style="justify-content: flex-end">
        <button class="primary sm" onclick={() => copy(linkTicket!)}>copy</button>
      </div>
    </div>
  </Modal>
{/if}
