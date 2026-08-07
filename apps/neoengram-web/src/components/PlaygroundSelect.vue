<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query';
import { computed, ref, watch } from 'vue';

import { queryPlaygroundList } from '@/api/operations';
import type { PlaygroundView } from '@/api/types';

const props = withDefaults(
  defineProps<{
    tenantId: string;
    modelValue: PlaygroundView | undefined;
    clearable?: boolean;
  }>(),
  { clearable: false },
);
const emit = defineEmits<{ 'update:modelValue': [value: PlaygroundView | undefined] }>();

const search = ref('');
const playgroundsQuery = useQuery({
  queryKey: computed(() => ['playgrounds', props.tenantId, 'selector', search.value.trim()]),
  queryFn: () =>
    queryPlaygroundList({
      tenant_id: props.tenantId,
      page_size: 50,
      ...(search.value.trim() ? { query: search.value.trim() } : {}),
    }),
  enabled: computed(() => Boolean(props.tenantId)),
});

function playgroundKey(playground: PlaygroundView): string {
  return [playground.project_id, playground.artifact_id, playground.playground_id].join('\u0000');
}

const options = computed(() => {
  const items = playgroundsQuery.data.value?.data.items ?? [];
  if (
    !props.modelValue ||
    items.some((item) => playgroundKey(item) === playgroundKey(props.modelValue!))
  ) {
    return items;
  }
  return [props.modelValue, ...items];
});
const selectedKey = computed(() => (props.modelValue ? playgroundKey(props.modelValue) : ''));

function select(value: string): void {
  const playground = value
    ? options.value.find((candidate) => playgroundKey(candidate) === value)
    : undefined;
  emit('update:modelValue', playground);
}

watch(
  () => props.tenantId,
  () => emit('update:modelValue', undefined),
);
</script>

<template>
  <el-select
    :model-value="selectedKey"
    aria-label="Playground 选择"
    :clearable="clearable"
    filterable
    remote
    reserve-keyword
    placeholder="搜索并选择 Playground"
    :loading="playgroundsQuery.isFetching.value"
    :remote-method="(query: string) => (search = query)"
    @update:model-value="select"
  >
    <el-option
      v-for="playground in options"
      :key="playgroundKey(playground)"
      :label="`${playground.display_name} · ${playground.project_id}/${playground.artifact_id}/${playground.playground_id}`"
      :value="playgroundKey(playground)"
    >
      <span class="playground-option__name">{{ playground.display_name }}</span>
      <code>
        {{ playground.project_id }}/{{ playground.artifact_id }}/{{ playground.playground_id }}
      </code>
    </el-option>
  </el-select>
</template>

<style scoped>
.playground-option__name {
  margin-right: 12px;
}
</style>
