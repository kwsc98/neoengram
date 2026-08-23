<script setup lang="ts">
import {
  Box,
  Close,
  Collection,
  Connection,
  DataAnalysis,
  Delete,
  DocumentCopy,
  Fold,
  Key,
  Plus,
  Search,
  FolderOpened,
  Folder,
  SwitchButton,
} from '@element-plus/icons-vue';
import { useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import zhCn from 'element-plus/es/locale/lang/zh-cn';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { queryApiVersion } from '@/api/operations';
import { runtimeConfig } from '@/config';
import {
  supportsArtifactCatalog,
  supportsS3ReadonlyAccessPoint,
  supportsResourceLifecycle,
  supportsSnapshotMaterialize,
} from '@/features/capabilities';
import { useAuthStore } from '@/stores/auth';
import { useTenantsStore } from '@/stores/tenants';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const auth = useAuthStore();
const tenants = useTenantsStore();
const drawerOpen = ref(false);
const createOpen = ref(false);
const creating = ref(false);
const createError = ref('');
const form = reactive({ tenantId: '', displayName: '', description: '' });
const currentTenantId = computed(() => String(route.params.tenantId ?? ''));
const currentTenant = computed(() => tenants.byId(currentTenantId.value));
const identityInitial = computed(() => auth.displayName.trim().charAt(0).toUpperCase() || 'U');
const versionQuery = useQuery({
  queryKey: ['system', 'version'],
  queryFn: queryApiVersion,
  staleTime: Number.POSITIVE_INFINITY,
});
const artifactCatalogEnabled = computed(() =>
  supportsArtifactCatalog(versionQuery.data.value?.data.capabilities),
);
const snapshotMaterializeEnabled = computed(() =>
  supportsSnapshotMaterialize(versionQuery.data.value?.data.capabilities),
);
const s3ReadonlyEnabled = computed(() =>
  Boolean(
    supportsS3ReadonlyAccessPoint(versionQuery.data.value?.data.capabilities) &&
    currentTenant.value?.permissions.includes('s3.access.read'),
  ),
);
const resourceLifecycleEnabled = computed(() =>
  Boolean(
    supportsResourceLifecycle(versionQuery.data.value?.data.capabilities) &&
    currentTenant.value?.permissions.includes('resource.lifecycle.read' as never),
  ),
);
const controlPlaneState = computed(() => {
  if (versionQuery.isPending.value) return { label: '连接检查中', online: false };
  if (versionQuery.isError.value) return { label: '控制面不可用', online: false };
  return { label: '控制面在线', online: true };
});

const navGroups = computed(() => {
  if (!currentTenantId.value) return [];
  const tenantId = currentTenantId.value;
  return [
    {
      label: '数据工作流',
      items: [
        { name: 'tenant-overview', label: '概览', icon: DataAnalysis, params: { tenantId } },
        { name: 'project-list', label: 'Projects', icon: Folder, params: { tenantId } },
        ...(artifactCatalogEnabled.value
          ? [{ name: 'artifact-list', label: '数据资产', icon: Box, params: { tenantId } }]
          : []),
        { name: 'playground-list', label: '工作区', icon: Collection, params: { tenantId } },
        ...(snapshotMaterializeEnabled.value
          ? [
              {
                name: 'snapshot-list',
                label: '快照与交付',
                icon: DocumentCopy,
                params: { tenantId },
              },
            ]
          : []),
        ...(s3ReadonlyEnabled.value
          ? [
              {
                name: 'object-storage-list',
                label: '对象存储',
                icon: FolderOpened,
                params: { tenantId },
              },
            ]
          : []),
        { name: 'job-query', label: '活动', icon: Search, params: { tenantId } },
        ...(resourceLifecycleEnabled.value
          ? [{ name: 'recycle-bin', label: '回收站', icon: Delete, params: { tenantId } }]
          : []),
      ],
    },
    {
      label: '基础设施',
      items: [
        {
          name: 'storage-volume-list',
          label: '集群与存储',
          icon: Connection,
          params: { tenantId },
        },
      ],
    },
  ];
});

const activeMenu = computed(() => {
  const name = String(route.name ?? '');
  if (name.startsWith('storage-volume-')) return 'storage-volume-list';
  if (name.startsWith('project-')) return 'project-list';
  if (name.startsWith('artifact-')) return 'artifact-list';
  if (name.startsWith('playground-')) return 'playground-list';
  if (name.startsWith('snapshot-')) return 'snapshot-list';
  if (name.startsWith('object-storage-')) return 'object-storage-list';
  if (name === 'recycle-bin') return 'recycle-bin';
  if (name === 'job-detail') return 'job-query';
  return name;
});

watch(
  () => auth.authenticated,
  async (authenticated) => {
    if (authenticated && !tenants.loaded && !tenants.loading) await tenants.load();
  },
  { immediate: true },
);

async function navigate(name: string, params: Record<string, string>): Promise<void> {
  drawerOpen.value = false;
  await router.push({ name, params });
}

function targetForTenantSwitch(): string {
  const name = String(route.name ?? '');
  if (name.startsWith('storage-volume-')) return 'storage-volume-list';
  if (name.startsWith('project-')) return 'project-list';
  if (name.startsWith('artifact-')) return 'artifact-list';
  if (name.startsWith('playground-')) return 'playground-list';
  if (name.startsWith('snapshot-')) return 'snapshot-list';
  if (name.startsWith('object-storage-')) return 'object-storage-list';
  if (name === 'recycle-bin') return 'recycle-bin';
  if (name === 'job-create') return 'job-query';
  if (name === 'job-query' || name === 'job-detail') return 'job-query';
  return 'tenant-overview';
}

async function switchTenant(tenantId: string): Promise<void> {
  if (tenantId === '__load_more__') {
    await tenants.loadMore();
    return;
  }
  if (!tenantId || tenantId === currentTenantId.value) return;
  await queryClient.cancelQueries();
  tenants.remember(tenantId);
  await router.push({ name: targetForTenantSwitch(), params: { tenantId } });
}

async function searchTenants(query: string): Promise<void> {
  await tenants.load(query.trim());
}

async function tenantDropdownVisible(visible: boolean): Promise<void> {
  if (visible && tenants.searchQuery) await tenants.load();
}

function openCreate(): void {
  form.tenantId = '';
  form.displayName = '';
  form.description = '';
  createError.value = '';
  createOpen.value = true;
}

async function submitTenant(): Promise<void> {
  createError.value = '';
  const tenantId = form.tenantId.trim();
  const displayName = form.displayName.trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(tenantId)) {
    createError.value = 'Tenant ID 必须是 1-128 位合法资源标识';
    return;
  }
  if (!displayName) {
    createError.value = '请输入租户名称';
    return;
  }
  creating.value = true;
  try {
    const result = await tenants.create({
      tenant_id: tenantId,
      display_name: displayName,
      ...(form.description.trim() ? { description: form.description.trim() } : {}),
    });
    createOpen.value = false;
    ElMessage.success(result.data.replayed ? '已返回现有租户' : '租户已创建');
    await router.push({ name: 'tenant-overview', params: { tenantId } });
  } catch (error) {
    createError.value = error instanceof Error ? error.message : '创建租户失败';
  } finally {
    creating.value = false;
  }
}
</script>

<template>
  <el-config-provider :locale="zhCn">
    <div class="app-shell">
      <a class="skip-link" href="#main-content">跳到主内容</a>
      <header class="topbar">
        <button
          class="icon-button mobile-menu"
          type="button"
          title="打开导航"
          @click="drawerOpen = true"
        >
          <el-icon><Fold /></el-icon>
        </button>
        <button
          class="brand"
          type="button"
          aria-label="返回 NeoEngram 首页"
          @click="router.push('/')"
        >
          <span class="brand__mark" aria-hidden="true"><span>N</span></span>
          <span class="brand__wordmark">
            <strong>NeoEngram</strong>
            <small>Workspace Console</small>
          </span>
        </button>
        <div class="topbar__right">
          <el-tag v-if="runtimeConfig.apiMode === 'mock'" type="warning" effect="plain">
            MOCK
          </el-tag>
          <el-tag
            v-else-if="runtimeConfig.authMode === 'development'"
            type="warning"
            effect="plain"
          >
            DEV
          </el-tag>
          <div v-if="auth.authenticated" class="tenant-switcher">
            <el-select
              :model-value="currentTenantId"
              aria-label="当前租户"
              filterable
              remote
              placeholder="选择租户"
              :loading="tenants.loading"
              @change="switchTenant"
              @remote-method="searchTenants"
              @visible-change="tenantDropdownVisible"
            >
              <el-option
                v-for="tenant in tenants.items"
                :key="tenant.tenant_id"
                :label="tenant.display_name"
                :value="tenant.tenant_id"
              >
                <span class="tenant-option__name">{{ tenant.display_name }}</span>
                <code>{{ tenant.tenant_id }}</code>
              </el-option>
              <el-option v-if="tenants.nextCursor" label="加载更多租户" value="__load_more__" />
            </el-select>
            <el-button
              v-if="tenants.canCreateTenant"
              class="tenant-create-button"
              :icon="Plus"
              title="创建租户"
              aria-label="创建租户"
              @click="openCreate"
            />
          </div>
          <div v-if="auth.authenticated" class="identity" :title="auth.displayName">
            <span class="identity__avatar" aria-hidden="true">{{ identityInitial }}</span>
            <span class="identity__name">{{ auth.displayName }}</span>
          </div>
          <el-button
            v-if="auth.authenticated"
            class="sign-out-button"
            text
            :icon="SwitchButton"
            title="退出登录"
            aria-label="退出登录"
            @click="auth.logout()"
          />
          <el-button v-else type="primary" :icon="Key" @click="auth.login()">登录</el-button>
        </div>
      </header>

      <aside class="sidebar">
        <div v-if="currentTenant" class="sidebar-tenant">
          <small>当前租户</small>
          <span class="sidebar-tenant__name">
            {{ currentTenant.display_name }}
            <i
              class="status-dot"
              :class="{ 'status-dot--ok': controlPlaneState.online }"
              :title="controlPlaneState.label"
            />
          </span>
          <code>{{ currentTenant.tenant_id }}</code>
        </div>
        <nav aria-label="主导航">
          <div v-for="group in navGroups" :key="group.label" class="nav-group">
            <span class="nav-group__label">{{ group.label }}</span>
            <button
              v-for="item in group.items"
              :key="item.name"
              type="button"
              class="nav-item"
              :class="{ 'nav-item--active': activeMenu === item.name }"
              :aria-current="activeMenu === item.name ? 'page' : undefined"
              @click="navigate(item.name, item.params)"
            >
              <el-icon><component :is="item.icon" /></el-icon>
              <span>{{ item.label }}</span>
            </button>
          </div>
        </nav>
        <div class="sidebar__footer" role="status" aria-live="polite">
          <span class="status-dot" :class="{ 'status-dot--ok': controlPlaneState.online }" />
          <span>{{ controlPlaneState.label }}</span>
        </div>
      </aside>

      <el-drawer
        v-model="drawerOpen"
        direction="ltr"
        size="280px"
        :with-header="false"
        class="mobile-drawer"
      >
        <div class="drawer-header">
          <div class="drawer-brand">
            <span class="brand__mark" aria-hidden="true"><span>N</span></span>
            <span>NeoEngram</span>
          </div>
          <button
            class="icon-button drawer-close"
            type="button"
            title="关闭导航"
            aria-label="关闭导航"
            @click="drawerOpen = false"
          >
            <el-icon><Close /></el-icon>
          </button>
        </div>
        <div v-if="currentTenant" class="sidebar-tenant sidebar-tenant--drawer">
          <small>当前租户</small>
          <span class="sidebar-tenant__name">{{ currentTenant.display_name }}</span>
          <code>{{ currentTenant.tenant_id }}</code>
        </div>
        <nav aria-label="移动端主导航">
          <div v-for="group in navGroups" :key="group.label" class="nav-group">
            <span class="nav-group__label">{{ group.label }}</span>
            <button
              v-for="item in group.items"
              :key="item.name"
              type="button"
              class="nav-item"
              :class="{ 'nav-item--active': activeMenu === item.name }"
              :aria-current="activeMenu === item.name ? 'page' : undefined"
              @click="navigate(item.name, item.params)"
            >
              <el-icon><component :is="item.icon" /></el-icon>
              <span>{{ item.label }}</span>
            </button>
          </div>
        </nav>
      </el-drawer>

      <main id="main-content" class="main-content" tabindex="-1">
        <router-view />
      </main>

      <el-dialog v-model="createOpen" title="创建租户" width="min(520px, calc(100vw - 28px))">
        <el-alert v-if="createError" :title="createError" type="error" :closable="false" />
        <el-form class="tenant-create-form" label-position="top" @submit.prevent="submitTenant">
          <el-form-item label="Tenant ID" required>
            <el-input v-model="form.tenantId" placeholder="tenant-lab" />
          </el-form-item>
          <el-form-item label="租户名称" required>
            <el-input v-model="form.displayName" placeholder="算法实验室" />
          </el-form-item>
          <el-form-item label="描述">
            <el-input v-model="form.description" type="textarea" :rows="3" maxlength="2048" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="createOpen = false">取消</el-button>
          <el-button type="primary" :loading="creating" @click="submitTenant">创建租户</el-button>
        </template>
      </el-dialog>
    </div>
  </el-config-provider>
</template>
