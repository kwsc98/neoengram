<script setup lang="ts">
import { Plus, Search } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, ref } from 'vue';
import { useRoute } from 'vue-router';

import { createStorageVolume, queryStorageVolumeList } from '@/api/operations';
import type { StorageAccessMode, StorageBackendType } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

const route = useRoute();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const searchInput = ref('');
const search = ref('');
const region = ref('');
const backendType = ref<StorageBackendType | ''>('');
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);
const createOpen = ref(false);
const createError = ref('');
const createForm = reactive<{
  storageVolumeId: string;
  displayName: string;
  edgeClusterId: string;
  region: string;
  backendType: StorageBackendType;
  accessMode: StorageAccessMode;
  pvcNamespace: string;
  pvcClaimName: string;
  nfsServer: string;
  nfsExportPath: string;
}>({
  storageVolumeId: '',
  displayName: '',
  edgeClusterId: '',
  region: '',
  backendType: 'pvc',
  accessMode: 'read_write_many',
  pvcNamespace: '',
  pvcClaimName: '',
  nfsServer: '',
  nfsExportPath: '',
});
const canCreate = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('storage.create') ?? false,
);
const createMutation = useMutation({ mutationFn: createStorageVolume });

const storageVolumesQuery = useQuery({
  queryKey: computed(() => [
    'storage-volumes',
    tenantId.value,
    region.value,
    backendType.value,
    search.value,
    cursor.value ?? '',
  ]),
  queryFn: () =>
    queryStorageVolumeList({
      tenant_id: tenantId.value,
      page_size: 50,
      ...(region.value ? { region: region.value } : {}),
      ...(backendType.value ? { backend_type: backendType.value } : {}),
      ...(search.value ? { query: search.value } : {}),
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
});

function applyFilters(): void {
  search.value = searchInput.value.trim();
  cursor.value = undefined;
  cursorHistory.value = [];
}

function nextPage(): void {
  const next = storageVolumesQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  const previous = cursorHistory.value.pop();
  cursor.value = previous || undefined;
}

function openCreate(): void {
  Object.assign(createForm, {
    storageVolumeId: '',
    displayName: '',
    edgeClusterId: '',
    region: '',
    backendType: 'pvc',
    accessMode: 'read_write_many',
    pvcNamespace: '',
    pvcClaimName: '',
    nfsServer: '',
    nfsExportPath: '',
  });
  createError.value = '';
  createOpen.value = true;
}

async function submitCreate(): Promise<void> {
  createError.value = '';
  const resourceId = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
  const regionName = /^[a-z0-9][a-z0-9-]{0,63}$/;
  if (
    !resourceId.test(createForm.storageVolumeId) ||
    !resourceId.test(createForm.edgeClusterId) ||
    !createForm.displayName.trim() ||
    !regionName.test(createForm.region)
  ) {
    createError.value = '请填写合法的 StorageVolume ID、名称、EdgeCluster ID 和 region';
    return;
  }
  if (
    createForm.backendType === 'pvc' &&
    (!createForm.pvcNamespace.trim() || !createForm.pvcClaimName.trim())
  ) {
    createError.value = 'PVC 后端需要填写 Namespace 和 Claim name';
    return;
  }
  if (
    createForm.backendType === 'nfs' &&
    (!createForm.nfsServer.trim() || !createForm.nfsExportPath.startsWith('/'))
  ) {
    createError.value = 'NFS 后端需要填写 Server，Export path 必须以 / 开头';
    return;
  }

  const common = {
    tenant_id: tenantId.value,
    storage_volume_id: createForm.storageVolumeId,
    display_name: createForm.displayName.trim(),
    edge_cluster_id: createForm.edgeClusterId,
    region: createForm.region,
    access_mode: createForm.accessMode,
  };
  try {
    const result = await createMutation.mutateAsync(
      createForm.backendType === 'pvc'
        ? {
            ...common,
            backend_type: 'pvc',
            pvc_reference: {
              namespace: createForm.pvcNamespace.trim(),
              claim_name: createForm.pvcClaimName.trim(),
            },
          }
        : {
            ...common,
            backend_type: 'nfs',
            nfs_reference: {
              server: createForm.nfsServer.trim(),
              export_path: createForm.nfsExportPath.trim(),
            },
          },
    );
    createOpen.value = false;
    await queryClient.invalidateQueries({ queryKey: ['storage-volumes', tenantId.value] });
    ElMessage.success(result.data.replayed ? '已返回现有 StorageVolume' : 'StorageVolume 已登记');
  } catch (error) {
    createError.value = error instanceof Error ? error.message : '登记 StorageVolume 失败';
  }
}

function stateType(state: string): 'success' | 'warning' | 'danger' {
  if (state === 'ready') return 'success';
  if (state === 'degraded') return 'warning';
  return 'danger';
}
</script>

<template>
  <div class="page">
    <PageHeading title="存储资源" :description="`${tenantId} 内按区域登记的 StorageVolume`">
      <template #actions>
        <el-button v-if="canCreate" type="primary" :icon="Plus" @click="openCreate">
          登记 StorageVolume
        </el-button>
      </template>
    </PageHeading>

    <form class="resource-toolbar storage-toolbar" @submit.prevent="applyFilters">
      <el-input v-model="region" clearable placeholder="Region，例如 cn-shanghai" />
      <el-select v-model="backendType" clearable placeholder="全部后端">
        <el-option label="PVC" value="pvc" />
        <el-option label="NFS" value="nfs" />
      </el-select>
      <el-input v-model="searchInput" clearable placeholder="搜索名称或 StorageVolume ID" />
      <el-button type="primary" native-type="submit" :icon="Search">查询</el-button>
    </form>

    <ApiProblemAlert
      v-if="storageVolumesQuery.error.value"
      :error="storageVolumesQuery.error.value"
      :retrying="storageVolumesQuery.isFetching.value"
      @retry="storageVolumesQuery.refetch"
    />

    <section class="content-section resource-section">
      <el-skeleton v-if="storageVolumesQuery.isPending.value" :rows="7" animated />
      <el-empty
        v-else-if="!storageVolumesQuery.data.value?.data.items.length"
        description="当前筛选下没有 StorageVolume"
        :image-size="78"
      />
      <template v-else>
        <el-table
          :data="storageVolumesQuery.data.value?.data.items"
          class="resource-table desktop-table"
        >
          <el-table-column label="StorageVolume" min-width="240">
            <template #default="scope">
              <div class="resource-identity">
                <strong>{{ scope.row.display_name }}</strong>
                <code>{{ scope.row.storage_volume_id }}</code>
              </div>
            </template>
          </el-table-column>
          <el-table-column prop="region" label="Region" min-width="135" />
          <el-table-column prop="edge_cluster_id" label="EdgeCluster" min-width="180" />
          <el-table-column label="后端" min-width="180">
            <template #default="scope">
              <strong>{{ scope.row.backend_type.toUpperCase() }}</strong>
              <small v-if="scope.row.pvc_reference" class="table-secondary">
                {{ scope.row.pvc_reference.namespace }}/{{ scope.row.pvc_reference.claim_name }}
              </small>
            </template>
          </el-table-column>
          <el-table-column label="状态" width="110">
            <template #default="scope">
              <el-tag :type="stateType(scope.row.state)" effect="plain">{{
                scope.row.state
              }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="更新时间" min-width="160">
            <template #default="scope">{{ formatTime(scope.row.updated_at_unix_ms) }}</template>
          </el-table-column>
        </el-table>
        <div class="mobile-resource-list">
          <div
            v-for="storageVolume in storageVolumesQuery.data.value?.data.items"
            :key="storageVolume.storage_volume_id"
            class="mobile-resource-item mobile-resource-item--static"
          >
            <span>
              <strong>{{ storageVolume.display_name }}</strong>
              <code>{{ storageVolume.storage_volume_id }}</code>
            </span>
            <span>
              <small
                >{{ storageVolume.region }} · {{ storageVolume.backend_type.toUpperCase() }}</small
              >
              <el-tag :type="stateType(storageVolume.state)" size="small" effect="plain">
                {{ storageVolume.state }}
              </el-tag>
            </span>
          </div>
        </div>
        <PageCursor
          :has-previous="cursorHistory.length > 0"
          :has-next="Boolean(storageVolumesQuery.data.value?.data.next_cursor)"
          :loading="storageVolumesQuery.isFetching.value"
          @previous="previousPage"
          @next="nextPage"
        />
      </template>
    </section>

    <el-dialog
      v-model="createOpen"
      title="登记 StorageVolume"
      width="min(620px, calc(100vw - 32px))"
    >
      <ApiProblemAlert v-if="createMutation.error.value" :error="createMutation.error.value" />
      <el-alert v-if="createError" :title="createError" type="error" :closable="false" />
      <el-form label-position="top" class="dialog-form">
        <div class="dialog-form-grid">
          <el-form-item label="StorageVolume ID" required>
            <el-input v-model="createForm.storageVolumeId" placeholder="volume-vision" />
          </el-form-item>
          <el-form-item label="名称" required>
            <el-input v-model="createForm.displayName" placeholder="视觉数据 PVC" />
          </el-form-item>
          <el-form-item label="EdgeCluster ID" required>
            <el-input v-model="createForm.edgeClusterId" placeholder="cluster-cn-east-1" />
          </el-form-item>
          <el-form-item label="Region" required>
            <el-input v-model="createForm.region" placeholder="cn-shanghai" />
          </el-form-item>
        </div>
        <el-form-item label="后端类型" required>
          <el-radio-group v-model="createForm.backendType">
            <el-radio-button value="pvc">PVC</el-radio-button>
            <el-radio-button value="nfs">NFS</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <div v-if="createForm.backendType === 'pvc'" class="dialog-form-grid">
          <el-form-item label="PVC Namespace" required>
            <el-input v-model="createForm.pvcNamespace" placeholder="neoengram-data" />
          </el-form-item>
          <el-form-item label="PVC Claim name" required>
            <el-input v-model="createForm.pvcClaimName" placeholder="vision-data" />
          </el-form-item>
        </div>
        <div v-else class="dialog-form-grid">
          <el-form-item label="NFS Server" required>
            <el-input v-model="createForm.nfsServer" placeholder="nas.internal" />
          </el-form-item>
          <el-form-item label="NFS Export path" required>
            <el-input v-model="createForm.nfsExportPath" placeholder="/exports/team-a" />
          </el-form-item>
        </div>
        <el-form-item label="访问模式" required>
          <el-select v-model="createForm.accessMode">
            <el-option label="ReadWriteMany" value="read_write_many" />
            <el-option label="ReadWriteOnce" value="read_write_once" />
            <el-option label="ReadOnlyMany" value="read_only_many" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createOpen = false">取消</el-button>
        <el-button type="primary" :loading="createMutation.isPending.value" @click="submitCreate">
          登记 StorageVolume
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>
