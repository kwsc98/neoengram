<script setup lang="ts">
import {
  Box,
  Collection,
  Coin,
  DataAnalysis,
  DocumentCopy,
  Fold,
  Key,
  Plus,
  Search,
  SwitchButton,
} from '@element-plus/icons-vue';
import { useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import zhCn from 'element-plus/es/locale/lang/zh-cn';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { queryApiVersion } from '@/api/operations';
import { runtimeConfig } from '@/config';
import { supportsArtifactCatalog, supportsSnapshotMaterialize } from '@/features/capabilities';
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

const navGroups = computed(() => {
  if (!currentTenantId.value) return [];
  const tenantId = currentTenantId.value;
  return [
    {
      label: '数据工作流',
      items: [
        { name: 'tenant-overview', label: '概览', icon: DataAnalysis, params: { tenantId } },
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
        { name: 'job-query', label: '活动', icon: Search, params: { tenantId } },
      ],
    },
    {
      label: '基础设施',
      items: [
        {
          name: 'storage-volume-list',
          label: '存储资源',
          icon: Coin,
          params: { tenantId },
        },
      ],
    },
  ];
});

const activeMenu = computed(() => {
  const name = String(route.name ?? '');
  if (name.startsWith('storage-volume-')) return 'storage-volume-list';
  if (name.startsWith('artifact-')) return 'artifact-list';
  if (name.startsWith('playground-')) return 'playground-list';
  if (name.startsWith('snapshot-')) return 'snapshot-list';
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
  if (name.startsWith('artifact-')) return 'artifact-list';
  if (name.startsWith('playground-')) return 'playground-list';
  if (name.startsWith('snapshot-')) return 'snapshot-list';
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
      <header class="topbar">
        <button
          class="icon-button mobile-menu"
          type="button"
          title="打开导航"
          @click="drawerOpen = true"
        >
          <el-icon><Fold /></el-icon>
        </button>
        <button class="brand" type="button" @click="router.push('/')">
          <span class="brand__mark">N</span>
          <span>
            <strong>NeoEngram</strong>
            <small>Control Console</small>
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
          <span class="identity">{{ auth.displayName }}</span>
          <el-button
            v-if="auth.authenticated"
            text
            :icon="SwitchButton"
            title="退出登录"
            @click="auth.logout()"
          />
          <el-button v-else type="primary" :icon="Key" @click="auth.login()">登录</el-button>
        </div>
      </header>

      <aside class="sidebar">
        <div v-if="currentTenant" class="sidebar-tenant">
          <span>{{ currentTenant.display_name }}</span>
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
              @click="navigate(item.name, item.params)"
            >
              <el-icon><component :is="item.icon" /></el-icon>
              <span>{{ item.label }}</span>
            </button>
          </div>
        </nav>
        <div class="sidebar__footer">
          <span class="status-dot status-dot--ok" />
          <span>控制面在线</span>
        </div>
      </aside>

      <el-drawer
        v-model="drawerOpen"
        direction="ltr"
        size="280px"
        :with-header="false"
        class="mobile-drawer"
      >
        <div class="drawer-brand">NeoEngram</div>
        <div v-if="currentTenant" class="sidebar-tenant sidebar-tenant--drawer">
          <span>{{ currentTenant.display_name }}</span>
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
              @click="navigate(item.name, item.params)"
            >
              <el-icon><component :is="item.icon" /></el-icon>
              <span>{{ item.label }}</span>
            </button>
          </div>
        </nav>
      </el-drawer>

      <main class="main-content">
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
