<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query';
import { computed } from 'vue';

import { queryStorageVolumeList } from '@/api/operations';

const props = defineProps<{
  tenantId: string;
  modelValue: string;
  region?: string;
  clearable?: boolean;
}>();
const emit = defineEmits<{ 'update:modelValue': [value: string] }>();

const storageVolumesQuery = useQuery({
  queryKey: computed(() => ['storage-volumes', props.tenantId, props.region ?? '', 'filter']),
  queryFn: () =>
    queryStorageVolumeList({
      tenant_id: props.tenantId,
      page_size: 100,
      ...(props.region ? { region: props.region } : {}),
    }),
  enabled: computed(() => Boolean(props.tenantId)),
});
</script>

<template>
  <el-select
    :model-value="modelValue"
    aria-label="StorageVolume 选择"
    :clearable="clearable"
    filterable
    placeholder="选择 StorageVolume"
    :loading="storageVolumesQuery.isFetching.value"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-option
      v-for="storageVolume in storageVolumesQuery.data.value?.data.items ?? []"
      :key="storageVolume.storage_volume_id"
      :label="`${storageVolume.display_name} · ${storageVolume.region}`"
      :value="storageVolume.storage_volume_id"
      :disabled="storageVolume.state === 'unavailable'"
    >
      <span class="storage-option__name">{{ storageVolume.display_name }}</span>
      <span class="storage-option__meta">
        {{ storageVolume.region }} · {{ storageVolume.backend_type.toUpperCase() }}
      </span>
    </el-option>
  </el-select>
</template>
