<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query';
import { computed, ref, watch } from 'vue';

import { queryArtifactCommitGraph } from '@/api/operations';
import type { CommitNode } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import { formatTime } from '@/utils/format';

const props = withDefaults(
  defineProps<{
    tenantId: string;
    projectId: string;
    artifactId: string;
    headCommitId: string | undefined;
    modelValue: string;
    enabled?: boolean;
    allowHistory?: boolean;
  }>(),
  { enabled: true, allowHistory: true },
);
const emit = defineEmits<{ 'update:modelValue': [value: string] }>();

const commitNodes = ref<CommitNode[]>([]);
const nextCursor = ref<string>();
const loadingMore = ref(false);
const loadMoreError = ref<unknown>();
const scopeKey = computed(() => [props.tenantId, props.projectId, props.artifactId].join('\u0000'));
let dataEpoch = 0;

function shortCommitId(commitId: string): string {
  return commitId.slice(0, 12);
}

function commitOptionLabel(commit: CommitNode): string {
  return `${commit.message} · ${shortCommitId(commit.commit_id)}`;
}

const commitGraphQuery = useQuery({
  queryKey: computed(() => [
    'artifact-commits',
    props.tenantId,
    props.projectId,
    props.artifactId,
    'playground-create',
  ]),
  queryFn: () => queryArtifactCommitGraph(props.tenantId, props.projectId, props.artifactId),
  enabled: computed(
    () =>
      props.enabled &&
      props.allowHistory &&
      Boolean(props.tenantId && props.projectId && props.artifactId && props.headCommitId),
  ),
});

watch(scopeKey, () => {
  dataEpoch += 1;
  commitNodes.value = [];
  nextCursor.value = undefined;
  loadingMore.value = false;
  loadMoreError.value = undefined;
});

watch(
  () => commitGraphQuery.data.value,
  (result) => {
    if (!result) return;
    dataEpoch += 1;
    commitNodes.value = [...result.data.graph.nodes];
    nextCursor.value = result.data.graph.next_cursor;
    loadingMore.value = false;
    loadMoreError.value = undefined;
  },
  { immediate: true },
);

async function loadMoreCommits(): Promise<void> {
  const requestCursor = nextCursor.value;
  if (!requestCursor || loadingMore.value) return;
  const requestScopeKey = scopeKey.value;
  const requestEpoch = dataEpoch;
  const isCurrentRequest = () => scopeKey.value === requestScopeKey && dataEpoch === requestEpoch;
  loadingMore.value = true;
  loadMoreError.value = undefined;
  try {
    const result = await queryArtifactCommitGraph(
      props.tenantId,
      props.projectId,
      props.artifactId,
      requestCursor,
    );
    if (!isCurrentRequest() || nextCursor.value !== requestCursor) return;
    const byId = new Map(commitNodes.value.map((commit) => [commit.commit_id, commit]));
    for (const commit of result.data.graph.nodes) byId.set(commit.commit_id, commit);
    commitNodes.value = [...byId.values()];
    nextCursor.value = result.data.graph.next_cursor;
  } catch (error) {
    if (isCurrentRequest()) loadMoreError.value = error;
  } finally {
    if (isCurrentRequest()) loadingMore.value = false;
  }
}
</script>

<template>
  <el-input
    v-if="!headCommitId || !allowHistory"
    :model-value="headCommitId || '空 Artifact'"
    aria-label="Base Commit"
    readonly
  />
  <div v-else class="commit-select">
    <ApiProblemAlert
      v-if="commitGraphQuery.error.value"
      :error="commitGraphQuery.error.value"
      :retrying="commitGraphQuery.isFetching.value"
      @retry="commitGraphQuery.refetch"
    />
    <ApiProblemAlert v-if="loadMoreError" :error="loadMoreError" />
    <el-select
      :model-value="modelValue"
      aria-label="Base Commit"
      filterable
      :loading="commitGraphQuery.isFetching.value"
      popper-class="artifact-commit-select-dropdown"
      placeholder="选择一个固定版本"
      @update:model-value="emit('update:modelValue', $event)"
    >
      <el-option
        v-for="commit in commitNodes"
        :key="commit.commit_id"
        :label="commitOptionLabel(commit)"
        :value="commit.commit_id"
        :aria-label="commitOptionLabel(commit)"
        :title="commit.commit_id"
      >
        <span class="commit-option">
          <span class="commit-option__copy">
            <strong>{{ commit.message }}</strong>
            <small>{{ formatTime(commit.created_at_unix_ms) }}</small>
          </span>
          <span class="commit-option__identity">
            <el-tag
              v-if="commit.commit_id === headCommitId"
              size="small"
              type="success"
              effect="plain"
            >
              Head
            </el-tag>
            <code :title="commit.commit_id">{{ shortCommitId(commit.commit_id) }}</code>
          </span>
        </span>
      </el-option>
    </el-select>
    <el-button
      v-if="nextCursor"
      text
      type="primary"
      :loading="loadingMore"
      @click="loadMoreCommits"
    >
      加载更多 Commit
    </el-button>
  </div>
</template>

<style scoped>
.commit-select,
.commit-select :deep(.el-select) {
  width: 100%;
}

.commit-select :deep(.el-select__selected-item) {
  min-width: 0;
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.commit-select {
  display: grid;
  gap: 6px;
}

:global(.artifact-commit-select-dropdown .el-select-dropdown__item) {
  height: 52px;
  padding-top: 7px;
  padding-bottom: 7px;
  line-height: normal;
}

.commit-option {
  display: flex;
  height: 100%;
  width: 100%;
  min-width: 0;
  align-items: center;
  gap: 8px;
}

.commit-option__copy {
  display: flex;
  min-width: 0;
  flex: 1;
  flex-direction: column;
  justify-content: center;
}

.commit-option__copy strong,
.commit-option__copy small {
  overflow: hidden;
  line-height: 17px;
  text-overflow: ellipsis;
}

.commit-option__copy small,
.commit-select code {
  color: var(--muted);
}

.commit-option__copy strong,
.commit-option__copy small,
.commit-option__identity code {
  max-width: 100%;
  white-space: nowrap;
}

.commit-option__identity {
  display: flex;
  min-width: 0;
  max-width: 45%;
  align-items: center;
  gap: 6px;
}

.commit-select code {
  display: block;
  overflow: hidden;
  font-size: 10px;
  text-overflow: ellipsis;
}
</style>
