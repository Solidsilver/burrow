<script lang="ts">
  import { onMount } from 'svelte';
  import { setToken } from './lib/api.svelte';
  import { app, refresh, startPolling } from './lib/state.svelte';
  import Sidebar from './components/Sidebar.svelte';
  import Overview from './views/Overview.svelte';
  import Backups from './views/Backups.svelte';
  import BackupDetail from './views/BackupDetail.svelte';
  import Friends from './views/Friends.svelte';
  import Devices from './views/Devices.svelte';
  import Storage from './views/Storage.svelte';

  onMount(() => {
    startPolling();
  });

  let tokenInput = $state('');

  async function submitToken() {
    setToken(tokenInput.trim());
    tokenInput = '';
    await refresh({ slow: true });
  }
</script>

{#if app.needsAuth}
  <div class="shell" style="display: grid; place-items: center; grid-template-columns: 1fr">
    <div class="card" style="max-width: 420px; width: 100%">
      <div class="stack">
        <div>
          <h1>burrow</h1>
          <p class="muted small" style="margin: 6px 0 0">
            This server requires its access token from non-loopback connections. Get it on the
            host with <code>burrow web token</code>.
          </p>
        </div>
        <form
          class="stack"
          onsubmit={(e) => {
            e.preventDefault();
            void submitToken();
          }}
        >
          <input
            type="text"
            class="mono"
            placeholder="access token"
            bind:value={tokenInput}
            autocomplete="off"
            spellcheck="false"
          />
          <button class="primary" type="submit" disabled={!tokenInput.trim()}>Connect</button>
        </form>
      </div>
    </div>
  </div>
{:else if !app.loaded}
  <div class="shell" style="display: grid; place-items: center; grid-template-columns: 1fr">
    <div class="stack" style="align-items: center">
      <span class="spinner"></span>
      {#if app.unreachable}
        <p class="muted">can't reach the daemon: {app.unreachable}</p>
      {:else}
        <p class="muted">connecting…</p>
      {/if}
    </div>
  </div>
{:else}
  <div class="shell">
    <Sidebar />
    <main class="main">
      {#if app.unreachable}
        <div class="banner" style="margin-bottom: 16px">
          <span class="spinner"></span>
          <span class="grow">connection to the daemon lost — retrying every 5s</span>
        </div>
      {/if}
      {#if app.route.view === 'overview'}
        <Overview />
      {:else if app.route.view === 'backups'}
        <Backups />
      {:else if app.route.view === 'backup'}
        {#key app.route.id}
          <BackupDetail id={app.route.id} />
        {/key}
      {:else if app.route.view === 'friends'}
        <Friends />
      {:else if app.route.view === 'devices'}
        <Devices />
      {:else if app.route.view === 'storage'}
        <Storage />
      {/if}
    </main>
  </div>
{/if}

<div class="toasts">
  {#each app.toasts as toast (toast.id)}
    <div class="toast {toast.kind}">{toast.text}</div>
  {/each}
</div>
