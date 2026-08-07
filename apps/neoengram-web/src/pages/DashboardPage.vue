<script setup lang="ts">
import {
  ArrowRight,
  Box,
  Collection,
  Connection,
  DocumentCopy,
  RefreshRight,
  TakeawayBox,
} from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { liveProbe, queryApiVersion, queryTenant, readyProbe } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { supportsArtifactCatalog, supportsSnapshotMaterialize } from '@/features/capabilities';
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

const resourceLinks = computed(() => [
  ...(supportsArtifactCatalog(version.value?.capabilities)
    ? [
        {
          name: 'artifact-list',
          label: '数据资产',
          detail: '管理工作区与快照所依赖的权威数据资产',
          icon: Box,
        },
      ]
    : []),
  {
    name: 'playground-list',
    label: '工作区',
    detail: '查看 Playground 与 Pre-commit 状态',
    icon: Collection,
  },
  ...(supportsSnapshotMaterialize(version.value?.capabilities)
    ? [
        {
          name: 'snapshot-list',
          label: '快照与交付',
          detail: '查看固定 Commit 的区域只读视图',
          icon: DocumentCopy,
        },
      ]
    : []),
  {
    name: 'storage-volume-list',
    label: '存储卷',
    detail: '查看已登记的 StorageVolume 与放置状态',
    icon: TakeawayBox,
  },
]);

async function refresh(): Promise<void> {
  await Promise.all([
    tenantQuery.refetch(),
    versionQuery.refetch(),
    liveQuery.refetch(),
    readyQuery.refetch(),
  ]);
}

async function openResource(name: string): Promise<void> {
  await router.push({ name, params: { tenantId: tenantId.value } });
}
</script>

<template>
  <div class="page tenant-home">
    <PageHeading
      :title="tenant?.display_name ?? '租户概览'"
      :description="tenant?.description ?? '当前租户的公开资源入口'"
    >
      <template #actions>
        <span class="control-health">
          <span class="status-dot" :class="readyQuery.data.value ? 'status-dot--ok' : ''" />
          {{ readyQuery.data.value ? '控制面正常' : '状态检查中' }}
        </span>
        <el-button :icon="RefreshRight" :loading="refreshing" @click="refresh">刷新</el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="firstError"
      :error="firstError"
      :retrying="refreshing"
      @retry="refresh"
    />

    <section class="resource-navigation" aria-label="租户资源">
      <button
        v-for="item in resourceLinks"
        :key="item.name"
        type="button"
        @click="openResource(item.name)"
      >
        <span class="resource-navigation__icon"><component :is="item.icon" /></span>
        <span>
          <strong>{{ item.label }}</strong>
          <small>{{ item.detail }}</small>
        </span>
        <ArrowRight />
      </button>
    </section>

    <section v-if="tenant" class="tenant-summary">
      <header>
        <h2>Tenant 信息</h2>
      </header>
      <dl class="definition-grid definition-grid--scope">
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
          <dt>当前权限</dt>
          <dd class="permission-list">
            <el-tag v-for="permission in tenant.permissions" :key="permission" effect="plain">
              {{ permission }}
            </el-tag>
          </dd>
        </div>
      </dl>
    </section>

    <footer class="system-strip">
      <span><Connection /> API v{{ version?.api_versions.join(', ') ?? '—' }}</span>
      <span
        ><i
          class="status-dot"
          :class="liveQuery.data.value ? 'status-dot--ok' : ''"
        />存活探针</span
      >
      <span
        ><i
          class="status-dot"
          :class="readyQuery.data.value ? 'status-dot--ok' : ''"
        />就绪探针</span
      >
      <code>{{ tenantId }}</code>
    </footer>
  </div>
</template>

<style scoped>
.tenant-home {
  --home-border: #d8dfdc;
}

.control-health {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: var(--muted);
  font-size: 12px;
}

.resource-navigation {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  border: 1px solid var(--home-border);
  background: #fff;
}

.resource-navigation button {
  min-width: 0;
  min-height: 104px;
  display: grid;
  grid-template-columns: 40px minmax(0, 1fr) 18px;
  align-items: center;
  gap: 14px;
  border: 0;
  border-right: 1px solid var(--home-border);
  border-bottom: 1px solid var(--home-border);
  padding: 18px 20px;
  background: transparent;
  cursor: pointer;
  text-align: left;
}

.resource-navigation button:nth-child(2n) {
  border-right: 0;
}

.resource-navigation button:nth-last-child(-n + 2) {
  border-bottom: 0;
}

.resource-navigation button:hover {
  background: #f7faf8;
}

.resource-navigation__icon {
  width: 40px;
  height: 40px;
  display: grid;
  place-items: center;
  border-radius: 6px;
  color: var(--green);
  background: #e5f0eb;
  font-size: 19px;
}

.resource-navigation strong,
.resource-navigation small {
  display: block;
}

.resource-navigation strong {
  font-size: 14px;
}

.resource-navigation small {
  margin-top: 6px;
  color: var(--muted);
  font-size: 12px;
}

.resource-navigation button > svg {
  width: 16px;
  color: #89938f;
}

.tenant-summary {
  margin-top: 20px;
  border: 1px solid var(--home-border);
  background: #fff;
}

.tenant-summary > header {
  padding: 16px 20px;
  border-bottom: 1px solid var(--home-border);
}

.tenant-summary h2 {
  margin: 0;
  font-size: 15px;
}

.tenant-summary .definition-grid {
  margin: 0;
  padding: 20px;
}

.permission-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.system-strip {
  display: flex;
  align-items: center;
  gap: 18px;
  margin-top: 20px;
  padding: 11px 14px;
  color: var(--muted);
  background: #e9eeeb;
  font-size: 11px;
}

.system-strip span {
  display: inline-flex;
  align-items: center;
  gap: 7px;
}

.system-strip svg {
  width: 14px;
}

.system-strip code {
  margin-left: auto;
}

@media (max-width: 650px) {
  .resource-navigation {
    grid-template-columns: 1fr;
  }

  .resource-navigation button,
  .resource-navigation button:nth-child(2n),
  .resource-navigation button:nth-last-child(-n + 2) {
    border-right: 0;
    border-bottom: 1px solid var(--home-border);
  }

  .resource-navigation button:last-child {
    border-bottom: 0;
  }

  .control-health {
    display: none;
  }

  .system-strip {
    flex-wrap: wrap;
    gap: 9px 14px;
  }

  .system-strip code {
    width: 100%;
    margin-left: 0;
  }
}
</style>
