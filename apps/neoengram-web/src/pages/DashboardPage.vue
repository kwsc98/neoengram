<script setup lang="ts">
import {
  Box,
  Collection,
  Connection,
  DataLine,
  DocumentCopy,
  RefreshRight,
  SetUp,
} from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { liveProbe, queryApiVersion, queryTenant, readyProbe } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const tenantQuery = useQuery({
  queryKey: computed(() => ['tenant', tenantId.value]),
  queryFn: () => queryTenant(tenantId.value),
});
const versionQuery = useQuery({ queryKey: ['system', 'version'], queryFn: queryApiVersion });
const liveQuery = useQuery({
  queryKey: ['health', 'live'],
  queryFn: liveProbe,
  refetchInterval: 15_000,
});
const readyQuery = useQuery({
  queryKey: ['health', 'ready'],
  queryFn: readyProbe,
  refetchInterval: 15_000,
});
const tenant = computed(() => tenantQuery.data.value?.data.tenant);
const version = computed(() => versionQuery.data.value?.data);
const firstError = computed(
  () =>
    tenantQuery.error.value ??
    versionQuery.error.value ??
    liveQuery.error.value ??
    readyQuery.error.value,
);
const refreshing = computed(
  () =>
    tenantQuery.isFetching.value ||
    versionQuery.isFetching.value ||
    liveQuery.isFetching.value ||
    readyQuery.isFetching.value,
);
const resourceLinks = [
  { name: 'artifact-list', label: 'Artifacts', description: '提交历史与关联资源', icon: Box },
  {
    name: 'playground-list',
    label: 'Playgrounds',
    description: '受管可写工作区',
    icon: Collection,
  },
  {
    name: 'snapshot-list',
    label: 'Snapshots',
    description: '固定 Commit 的只读视图',
    icon: DocumentCopy,
  },
];

async function refresh(): Promise<void> {
  await Promise.all([
    tenantQuery.refetch(),
    versionQuery.refetch(),
    liveQuery.refetch(),
    readyQuery.refetch(),
  ]);
}
</script>

<template>
  <div class="page">
    <PageHeading
      :title="tenant?.display_name ?? '租户概览'"
      :description="tenant?.tenant_id ?? tenantId"
    >
      <template #actions>
        <el-button :icon="RefreshRight" :loading="refreshing" @click="refresh">刷新</el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="firstError"
      :error="firstError"
      :retrying="refreshing"
      @retry="refresh"
    />

    <section class="status-grid" aria-label="服务状态">
      <article class="status-panel">
        <el-icon class="status-panel__icon"><Connection /></el-icon>
        <div>
          <span class="status-panel__label">进程存活</span
          ><strong>{{ liveQuery.data.value ? '正常' : '检查中' }}</strong>
        </div>
        <span
          class="status-dot"
          :class="liveQuery.data.value ? 'status-dot--ok' : 'status-dot--idle'"
        />
      </article>
      <article class="status-panel">
        <el-icon class="status-panel__icon"><DataLine /></el-icon>
        <div>
          <span class="status-panel__label">Authority 就绪</span
          ><strong>{{ readyQuery.data.value ? '可接收请求' : '检查中' }}</strong>
        </div>
        <span
          class="status-dot"
          :class="readyQuery.data.value ? 'status-dot--ok' : 'status-dot--idle'"
        />
      </article>
      <article class="status-panel">
        <el-icon class="status-panel__icon"><SetUp /></el-icon>
        <div>
          <span class="status-panel__label">Public API</span
          ><strong>v{{ version?.api_versions.join(', ') ?? '—' }}</strong>
        </div>
      </article>
    </section>

    <section v-if="tenant" class="content-section">
      <div class="section-heading">
        <div>
          <h2>租户信息</h2>
          <p>当前账号可见的 TenantView</p>
        </div>
      </div>
      <dl class="definition-grid">
        <div>
          <dt>Tenant ID</dt>
          <dd>
            <code>{{ tenant.tenant_id }}</code>
          </dd>
        </div>
        <div>
          <dt>Resource version</dt>
          <dd>{{ tenant.resource_version }}</dd>
        </div>
        <div>
          <dt>创建时间</dt>
          <dd>{{ formatTime(tenant.created_at_unix_ms) }}</dd>
        </div>
        <div>
          <dt>更新时间</dt>
          <dd>{{ formatTime(tenant.updated_at_unix_ms) }}</dd>
        </div>
        <div class="definition-grid__wide">
          <dt>描述</dt>
          <dd>{{ tenant.description ?? '—' }}</dd>
        </div>
        <div class="definition-grid__wide">
          <dt>当前权限</dt>
          <dd class="tag-list">
            <el-tag v-for="permission in tenant.permissions" :key="permission" effect="plain">{{
              permission
            }}</el-tag>
          </dd>
        </div>
      </dl>
    </section>

    <section class="resource-shortcuts" aria-label="租户资源入口">
      <button
        v-for="item in resourceLinks"
        :key="item.name"
        type="button"
        @click="router.push({ name: item.name, params: { tenantId } })"
      >
        <el-icon><component :is="item.icon" /></el-icon>
        <span
          ><strong>{{ item.label }}</strong
          ><small>{{ item.description }}</small></span
        >
      </button>
    </section>
  </div>
</template>
