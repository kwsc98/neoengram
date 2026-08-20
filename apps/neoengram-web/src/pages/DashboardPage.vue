<script setup lang="ts">
import {
  ArrowRight,
  Box,
  Collection,
  Connection,
  DocumentCopy,
  Folder,
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
const canCreateProject = computed(() => tenant.value?.permissions.includes('project.create') ?? false);
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
  {
    name: 'project-list',
    label: 'Projects',
    detail: '创建和管理租户内的逻辑项目',
    icon: Folder,
  },
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

async function openProjectCreate(): Promise<void> {
  await router.push({
    name: 'project-list',
    params: { tenantId: tenantId.value },
    query: { create: '1' },
  });
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
        <el-button
          v-if="canCreateProject"
          type="primary"
          :icon="Folder"
          @click="openProjectCreate"
        >
          创建 Project
        </el-button>
        <el-button :icon="RefreshRight" :loading="refreshing" @click="refresh">刷新</el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="firstError"
      :error="firstError"
      :retrying="refreshing"
      @retry="refresh"
    />

    <section class="overview-status" aria-label="运行状态" aria-live="polite">
      <div class="overview-status__item">
        <span class="overview-status__icon"><Connection /></span>
        <span>
          <small>API 协议</small>
          <strong>v{{ version?.api_version ?? '—' }}</strong>
        </span>
      </div>
      <div class="overview-status__item">
        <i class="status-dot" :class="liveQuery.data.value ? 'status-dot--ok' : ''" />
        <span>
          <small>存活探针</small>
          <strong>{{ liveQuery.data.value ? '运行正常' : '检查中' }}</strong>
        </span>
      </div>
      <div class="overview-status__item">
        <i class="status-dot" :class="readyQuery.data.value ? 'status-dot--ok' : ''" />
        <span>
          <small>就绪探针</small>
          <strong>{{ readyQuery.data.value ? '服务就绪' : '检查中' }}</strong>
        </span>
      </div>
    </section>

    <section class="workspace-section" aria-labelledby="resource-heading">
      <header class="workspace-section__heading">
        <div>
          <h2 id="resource-heading">租户资源</h2>
          <p>进入当前租户的数据、工作区与基础设施</p>
        </div>
        <code>{{ tenantId }}</code>
      </header>
      <div class="resource-navigation">
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
      </div>
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
  </div>
</template>

<style scoped>
.tenant-home {
  --home-border: var(--line);
}

.control-health {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: var(--muted);
  font-size: 12px;
  font-weight: 600;
}

.overview-status {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  border: 1px solid var(--home-border);
  border-radius: 8px;
  background: #fff;
  box-shadow: var(--shadow-xs);
  overflow: hidden;
}

.overview-status__item {
  min-width: 0;
  min-height: 86px;
  display: flex;
  align-items: center;
  gap: 13px;
  border-right: 1px solid var(--home-border);
  padding: 18px 20px;
}

.overview-status__item:last-child {
  border-right: 0;
}

.overview-status__icon {
  width: 34px;
  height: 34px;
  display: grid;
  flex: 0 0 auto;
  place-items: center;
  border-radius: 8px;
  color: var(--blue);
  background: var(--blue-soft);
}

.overview-status__icon svg {
  width: 17px;
}

.overview-status__item > .status-dot {
  width: 10px;
  height: 10px;
  margin: 0 12px;
}

.overview-status small,
.overview-status strong {
  display: block;
}

.overview-status small {
  color: var(--muted);
  font-size: 11px;
  font-weight: 650;
}

.overview-status strong {
  margin-top: 5px;
  color: var(--ink);
  font-size: 14px;
}

.workspace-section {
  margin-top: 30px;
}

.workspace-section__heading {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 20px;
  margin-bottom: 14px;
}

.workspace-section__heading h2,
.tenant-summary h2 {
  margin: 0;
  font-size: 15px;
  line-height: 1.35;
}

.workspace-section__heading p {
  margin: 4px 0 0;
  color: var(--muted);
  font-size: 12px;
}

.workspace-section__heading code {
  max-width: 40%;
  color: var(--muted);
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.resource-navigation {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.resource-navigation button {
  min-width: 0;
  min-height: 108px;
  display: grid;
  grid-template-columns: 42px minmax(0, 1fr) 18px;
  align-items: center;
  gap: 15px;
  border: 1px solid var(--home-border);
  border-radius: 8px;
  padding: 18px 20px;
  background: #fff;
  box-shadow: var(--shadow-xs);
  cursor: pointer;
  text-align: left;
  transition:
    border-color 160ms ease,
    box-shadow 160ms ease,
    transform 160ms ease;
}

.resource-navigation button:hover {
  border-color: var(--line-strong);
  box-shadow: var(--shadow-sm);
  transform: translateY(-1px);
}

.resource-navigation__icon {
  width: 42px;
  height: 42px;
  display: grid;
  place-items: center;
  border-radius: 8px;
  color: var(--green);
  background: var(--green-soft);
  font-size: 19px;
}

.resource-navigation button:nth-child(2) .resource-navigation__icon {
  color: var(--blue);
  background: var(--blue-soft);
}

.resource-navigation button:nth-child(3) .resource-navigation__icon {
  color: var(--amber);
  background: var(--amber-soft);
}

.resource-navigation button:nth-child(4) .resource-navigation__icon {
  color: var(--rose);
  background: var(--rose-soft);
}

.resource-navigation strong,
.resource-navigation small {
  display: block;
}

.resource-navigation strong {
  font-size: 14px;
}

.resource-navigation small {
  max-width: 420px;
  margin-top: 6px;
  color: var(--muted);
  font-size: 12px;
  line-height: 1.55;
}

.resource-navigation button > svg {
  width: 16px;
  color: var(--muted-light);
  transition: color 160ms ease;
}

.resource-navigation button:hover > svg {
  color: var(--green);
}

.tenant-summary {
  margin-top: 30px;
  padding-top: 22px;
  border-top: 1px solid var(--home-border);
}

.tenant-summary > header {
  margin-bottom: 12px;
}

.tenant-summary .definition-grid {
  margin: 0;
  border: 1px solid var(--home-border);
  border-radius: 8px;
  background: #fff;
  box-shadow: var(--shadow-xs);
  overflow: hidden;
}

.permission-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

@media (max-width: 650px) {
  .overview-status {
    grid-template-columns: 1fr 1fr;
  }

  .overview-status__item {
    min-height: 76px;
    border-bottom: 1px solid var(--home-border);
  }

  .overview-status__item:nth-child(2) {
    border-right: 0;
  }

  .overview-status__item:last-child {
    grid-column: 1 / -1;
    border-bottom: 0;
  }

  .resource-navigation {
    grid-template-columns: 1fr;
  }

  .resource-navigation button {
    min-height: 96px;
    padding: 16px;
  }

  .workspace-section__heading code {
    display: none;
  }

  .control-health {
    display: none;
  }
}
</style>
