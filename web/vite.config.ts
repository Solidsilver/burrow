import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

export default defineConfig({
  plugins: [svelte()],
  // Relative asset URLs: the daemon may serve the UI from any mount point.
  base: './',
  build: {
    // Embedded into the burrow binary via rust-embed (gitignored output).
    outDir: '../crates/burrow-daemon/web-dist',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:8385',
    },
  },
});
