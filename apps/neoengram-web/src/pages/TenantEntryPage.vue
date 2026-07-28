<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query';
import { computed, watch } from 'vue';
import { useRouter } from 'vue-router';

import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import { useTenantsStore } from '@/stores/tenants';

const router = useRouter();
const tenants = useTenantsStore();
const tenantQuery = useQuery({
  queryKey: ['tenants', 'entry'],
  queryFn: () => tenants.load(),
});
const preferred = computed(() => {
  const remembered = tenants.items.find((tenant) => tenant.tenant_id === tenants.lastTenantId);
  return remembered ?? tenants.items[0];
});

watch(
  preferred,
  async (tenant) => {
    if (!tenant) return;
    tenants.remember(tenant.tenant_id);
    await router.replace({ name: 'tenant-overview', params: { tenantId: tenant.tenant_id } });
  },
  { immediate: true },
);
</script>

<template>
  <div class="page tenant-entry">
    <ApiProblemAlert
      v-if="tenantQuery.error.value"
      :error="tenantQuery.error.value"
      :retrying="tenantQuery.isFetching.value"
      @retry="tenantQuery.refetch"
    />
    <el-skeleton v-if="tenantQuery.isPending.value" :rows="6" animated />
    <el-empty
      v-else-if="tenants.items.length === 0"
      description="当前账号还没有可访问的租户"
      :image-size="86"
    />
  </div>
</template>
