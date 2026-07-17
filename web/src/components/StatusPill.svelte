<script lang="ts">
  import type { ReplicationHealth } from '../lib/api.svelte';
  import { healthSummary } from '../lib/format';

  interface Props {
    health: ReplicationHealth;
  }

  let { health }: Props = $props();
  let summary = $derived(healthSummary(health));
</script>

<span class="pill {summary.level}">
  <span class="dot"></span>
  {summary.label}
</span>
