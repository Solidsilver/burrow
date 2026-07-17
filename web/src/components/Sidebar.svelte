<script lang="ts">
  import { app, navigate } from '../lib/state.svelte';

  const items = [
    { view: 'overview', label: 'Overview', hash: '#/' },
    { view: 'backups', label: 'Backups', hash: '#/backups' },
    { view: 'friends', label: 'Friends', hash: '#/friends' },
    { view: 'devices', label: 'Devices', hash: '#/devices' },
    { view: 'storage', label: 'Storage', hash: '#/storage' },
  ] as const;

  let pendingCount = $derived(app.pendingPeers.length + app.spaceRequests.length);

  // Initial value applied in main.ts before mount (no flash); kept in sync here.
  let theme = $state(localStorage.getItem('burrow.theme') ?? 'dark');

  function toggleTheme() {
    theme = theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('burrow.theme', theme);
  }

  function isActive(view: string): boolean {
    if (view === 'backups') return app.route.view === 'backups' || app.route.view === 'backup';
    return app.route.view === view;
  }
</script>

<nav class="sidebar">
  <div class="brand">
    <span>🕳️</span>
    <span>burrow</span>
  </div>

  {#each items as item (item.view)}
    <a
      class="nav-item"
      class:active={isActive(item.view)}
      href={item.hash}
      onclick={(e) => {
        e.preventDefault();
        navigate(item.hash);
      }}
    >
      {item.label}
      {#if item.view === 'friends' && pendingCount > 0}
        <span class="badge">{pendingCount}</span>
      {/if}
    </a>
  {/each}

  <div class="sidebar-footer">
    {#if app.status}
      <div class="strong">{app.status.node_name}</div>
      <div>{app.status.device_name} · {app.status.mode}</div>
    {/if}
    {#if app.server}
      <div>v{app.server.version}</div>
    {/if}
    <div
      class="theme-toggle"
      role="button"
      tabindex="0"
      onclick={toggleTheme}
      onkeydown={(e) => e.key === 'Enter' && toggleTheme()}
    >
      {theme === 'dark' ? '☾ dark' : '☀ light'}
    </div>
  </div>
</nav>
