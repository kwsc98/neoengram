<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query';
import { computed, ref, watch } from 'vue';

import { queryArtifactList } from '@/api/operations';
import type { ArtifactView } from '@/api/types';

const props = withDefaults(
  defineProps<{
    tenantId: string;
    modelValue: ArtifactView | undefined;
    clearable?: boolean;
    allowNonEmpty?: boolean;
  }>(),
  { clearable: false, allowNonEmpty: false },
);
const emit = defineEmits<{ 'update:modelValue': [value: ArtifactView | undefined] }>();

const search = ref('');
const artifactsQuery = useQuery({
  queryKey: computed(() => ['artifacts', props.tenantId, 'selector', search.value.trim()]),
  queryFn: () =>
    queryArtifactList({
      tenant_id: props.tenantId,
      page_size: 50,
      ...(search.value.trim() ? { query: search.value.trim() } : {}),
    }),
  enabled: computed(() => Boolean(props.tenantId)),
});

function artifactKey(artifact: ArtifactView): string {
  return `${artifact.project_id}\u0000${artifact.artifact_id}`;
}

const options = computed(() => {
  const items = artifactsQuery.data.value?.data.items ?? [];
  if (
    !props.modelValue ||
    items.some((item) => artifactKey(item) === artifactKey(props.modelValue!))
  ) {
    return items;
  }
  return [props.modelValue, ...items];
});
const selectedKey = computed(() => (props.modelValue ? artifactKey(props.modelValue) : ''));

function select(value: string): void {
  const artifact = value
    ? options.value.find((candidate) => artifactKey(candidate) === value)
    : undefined;
  emit(
    'update:modelValue',
    artifact && (props.allowNonEmpty || !artifact.head_commit_id) ? artifact : undefined,
  );
}

watch(
  () => props.tenantId,
  () => emit('update:modelValue', undefined),
);
watch(
  () => props.allowNonEmpty,
  (allowed) => {
    if (!allowed && props.modelValue?.head_commit_id) emit('update:modelValue', undefined);
  },
);
</script>

<template>
  <el-select
    :model-value="selectedKey"
    aria-label="Artifact 选择"
    :clearable="clearable"
    filterable
    remote
    reserve-keyword
    placeholder="搜索并选择 Artifact"
    :loading="artifactsQuery.isFetching.value"
    :remote-method="(query: string) => (search = query)"
    @update:model-value="select"
  >
    <el-option
      v-for="artifact in options"
      :key="artifactKey(artifact)"
      :label="`${artifact.display_name} · ${artifact.project_id}/${artifact.artifact_id}`"
      :value="artifactKey(artifact)"
      :disabled="!allowNonEmpty && Boolean(artifact.head_commit_id)"
    >
      <span class="artifact-option__name">{{ artifact.display_name }}</span>
      <code>{{ artifact.project_id }}/{{ artifact.artifact_id }}</code>
    </el-option>
  </el-select>
</template>

<style scoped>
.artifact-option__name {
  margin-right: 12px;
}
</style>
