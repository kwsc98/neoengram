<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query';
import { computed } from 'vue';

import { queryProjectList } from '@/api/operations';

const props = defineProps<{ tenantId: string; modelValue: string }>();
const emit = defineEmits<{ 'update:modelValue': [value: string] }>();

const projectsQuery = useQuery({
  queryKey: computed(() => ['projects', props.tenantId, 'filter']),
  queryFn: () => queryProjectList({ tenant_id: props.tenantId, page_size: 100 }),
  enabled: computed(() => Boolean(props.tenantId)),
});
</script>

<template>
  <el-select
    :model-value="modelValue"
    aria-label="Project 筛选"
    clearable
    filterable
    placeholder="全部 Project"
    :loading="projectsQuery.isFetching.value"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-option
      v-for="project in projectsQuery.data.value?.data.items ?? []"
      :key="project.project_id"
      :label="project.display_name"
      :value="project.project_id"
    >
      <span class="tenant-option__name">{{ project.display_name }}</span>
      <code>{{ project.project_id }}</code>
    </el-option>
  </el-select>
</template>
