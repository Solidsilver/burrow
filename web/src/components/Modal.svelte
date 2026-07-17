<script lang="ts">
  import type { Snippet } from 'svelte';

  interface Props {
    title: string;
    onclose: () => void;
    children: Snippet;
  }

  let { title, onclose, children }: Props = $props();
</script>

<svelte:window
  onkeydown={(e) => {
    if (e.key === 'Escape') onclose();
  }}
/>

<div
  class="modal-backdrop"
  onclick={(e) => {
    if (e.target === e.currentTarget) onclose();
  }}
  role="presentation"
>
  <div class="modal" role="dialog" aria-modal="true" aria-label={title}>
    <div class="row between" style="margin-bottom: 14px">
      <h2>{title}</h2>
      <button class="ghost sm" onclick={onclose} aria-label="Close">✕</button>
    </div>
    {@render children()}
  </div>
</div>
