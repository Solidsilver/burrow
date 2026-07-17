<script lang="ts">
  import { app } from '../lib/state.svelte';
  import { fmtBytes } from '../lib/format';
  import UsageBar from '../components/UsageBar.svelte';

  let friends = $derived(app.peers.filter((p) => p.state === 'active'));
  let hosting = $derived(app.status?.hosting);
</script>

<div class="stack-lg">
  <div class="page-head">
    <div>
      <h1>Storage</h1>
      <div class="sub">space you host for friends, and space friends host for you</div>
    </div>
  </div>

  <div class="card stack">
    <div class="row between">
      <h2>This device</h2>
      <span class="small muted">
        {#if hosting?.offer_max}
          ceiling {fmtBytes(hosting.offer_max)}
        {:else}
          no ceiling (<code>storage.offer_max</code> unset)
        {/if}
      </span>
    </div>
    <UsageBar used={hosting?.held_total ?? 0} total={hosting?.offer_max ?? null} />
    <div class="small muted">
      holding {fmtBytes(hosting?.held_total ?? 0)} of friends' data on this device
    </div>
  </div>

  <section class="card stack">
    <h2>You host for them</h2>
    {#if !hosting?.grants.length}
      <div class="muted small">no grants given yet — <a href="#/friends">grant a friend space</a></div>
    {:else}
      <table class="data">
        <thead>
          <tr>
            <th>Friend</th>
            <th style="width: 45%">Usage</th>
          </tr>
        </thead>
        <tbody>
          {#each hosting.grants as [name, granted, used] (name)}
            <tr>
              <td class="strong">{name}</td>
              <td><UsageBar {used} total={granted} /></td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </section>

  <section class="card stack">
    <h2>They host for you</h2>
    {#if !friends.some((p) => p.received_bytes > 0)}
      <div class="muted small">
        no space received yet — ask with <em>request</em> on the <a href="#/friends">friends page</a>
      </div>
    {:else}
      <table class="data">
        <thead>
          <tr>
            <th>Friend</th>
            <th style="width: 45%">Usage</th>
          </tr>
        </thead>
        <tbody>
          {#each friends.filter((p) => p.received_bytes > 0) as p (p.owner_pk.join(','))}
            <tr>
              <td class="strong">{p.name}</td>
              <td><UsageBar used={p.received_used} total={p.received_bytes} /></td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </section>
</div>
