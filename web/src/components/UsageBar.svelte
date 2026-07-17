<script lang="ts">
  import { fmtBytes } from '../lib/format';

  interface Props {
    used: number;
    total: number | null;
    /** Show "used / total" text to the right of the bar. */
    label?: boolean;
  }

  let { used, total, label = true }: Props = $props();

  let pct = $derived(total && total > 0 ? Math.min(100, (used / total) * 100) : 0);
  let nearlyFull = $derived(total !== null && total > 0 && used / total > 0.9);
</script>

<div class="row" style="gap: 8px">
  <div class="bar grow" class:full={nearlyFull}>
    <div style="width: {pct}%"></div>
  </div>
  {#if label}
    <span class="small muted nowrap">
      {#if used === 0 && (total === null || total === 0)}
        —
      {:else}
        {fmtBytes(used)}{total !== null ? ` / ${fmtBytes(total)}` : ''}
      {/if}
    </span>
  {/if}
</div>
