<script setup lang="ts">
import type { CommitTreeItem } from '@/utils/commit-tree';

import ArtifactCommitTreeNode from './ArtifactCommitTreeNode.vue';

defineProps<{
  roots: CommitTreeItem[];
  headCommitId: string | undefined;
}>();

const emit = defineEmits<{
  select: [commitId: string];
}>();
</script>

<template>
  <ol class="commit-tree" aria-label="Commit 分支树">
    <ArtifactCommitTreeNode
      v-for="root in roots"
      :key="root.node.commit_id"
      :item="root"
      :head-commit-id="headCommitId"
      :depth="0"
      @select="emit('select', $event)"
    />
  </ol>
</template>
