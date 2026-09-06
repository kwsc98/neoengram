<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query';
import { computed, ref, watch } from 'vue';

import { queryWorkspaceList } from '@/api/operations';
import type { WorkspaceView } from '@/api/types';
import { workspaceOperationUnavailableReason } from '@/features/precommit/status';

const props = withDefaults(
  defineProps<{
    tenantId: string;
    modelValue: WorkspaceView | undefined;
    clearable?: boolean;
  }>(),
  { clearable: false },
);
const emit = defineEmits<{ 'update:modelValue': [value: WorkspaceView | undefined] }>();

const search = ref('');
const workspacesQuery = useQuery({
  queryKey: computed(() => ['workspaces', props.tenantId, 'selector', search.value.trim()]),
  queryFn: () =>
    queryWorkspaceList({
      tenant_id: props.tenantId,
      page_size: 50,
      ...(search.value.trim() ? { query: search.value.trim() } : {}),
    }),
  enabled: computed(() => Boolean(props.tenantId)),
});

function workspaceKey(workspace: WorkspaceView): string {
  return [workspace.project_id, workspace.artifact_id, workspace.workspace_id].join('\u0000');
}

const options = computed(() => {
  const items = workspacesQuery.data.value?.data.items ?? [];
  if (
    !props.modelValue ||
    items.some((item) => workspaceKey(item) === workspaceKey(props.modelValue!))
  ) {
    return items;
  }
  return [props.modelValue, ...items];
});
const selectedKey = computed(() => (props.modelValue ? workspaceKey(props.modelValue) : ''));

function select(value: string): void {
  const workspace = value
    ? options.value.find((candidate) => workspaceKey(candidate) === value)
    : undefined;
  emit('update:modelValue', workspace);
}

function optionUnavailableReason(workspace: WorkspaceView): string | undefined {
  return workspaceOperationUnavailableReason(workspace);
}

watch(
  () => props.tenantId,
  () => emit('update:modelValue', undefined),
);
</script>

<template>
  <el-select
    :model-value="selectedKey"
    aria-label="Workspace 选择"
    :clearable="clearable"
    filterable
    remote
    reserve-keyword
    placeholder="搜索并选择 Workspace"
    :loading="workspacesQuery.isFetching.value"
    :remote-method="(query: string) => (search = query)"
    @update:model-value="select"
  >
    <el-option
      v-for="workspace in options"
      :key="workspaceKey(workspace)"
      :label="`${workspace.display_name} · ${workspace.project_id}/${workspace.artifact_id}/${workspace.workspace_id}`"
      :value="workspaceKey(workspace)"
      :disabled="Boolean(optionUnavailableReason(workspace))"
    >
      <span class="workspace-option__name">{{ workspace.display_name }}</span>
      <code>
        {{ workspace.project_id }}/{{ workspace.artifact_id }}/{{ workspace.workspace_id }}
      </code>
      <small v-if="optionUnavailableReason(workspace)" class="workspace-option__state">
        {{ optionUnavailableReason(workspace) }}
      </small>
    </el-option>
  </el-select>
</template>

<style scoped>
.workspace-option__name {
  margin-right: 12px;
}

.workspace-option__state {
  margin-left: 12px;
  color: var(--muted);
}
</style>
