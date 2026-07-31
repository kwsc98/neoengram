<script setup lang="ts">
import { Check, Close, CopyDocument, Key, Plus, Search } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute } from 'vue-router';

import {
  approveStorageEnrollment,
  createStorageEnrollmentToken,
  createStorageVolume,
  queryStorageEnrollmentList,
  queryStorageVolumeList,
  rejectStorageEnrollment,
} from '@/api/operations';
import { isApiProblem } from '@/api/problem';
import type {
  ApproveStorageEnrollmentRequest,
  CreateStorageEnrollmentTokenRequest,
  CreateStorageEnrollmentTokenResponse,
  RejectStorageEnrollmentRequest,
  StorageAccessMode,
  StorageBackendType,
  StorageEnrollmentAccessMode,
  StorageEnrollmentState,
  StorageEnrollmentView,
} from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import { runtimeConfig } from '@/config';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

type ViewName = 'volumes' | 'enrollments';
type TagType = 'success' | 'warning' | 'danger' | 'info';

const route = useRoute();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const permissions = computed(() => tenants.byId(tenantId.value)?.permissions ?? []);
const canCreateNfs = computed(() => permissions.value.includes('storage.create'));
const canCreateEnrollment = computed(() => permissions.value.includes('storage.enrollment.create'));
const canReadEnrollments = computed(() => permissions.value.includes('storage.enrollment.read'));
const canReviewEnrollments = computed(() =>
  permissions.value.includes('storage.enrollment.review'),
);

const activeView = ref<ViewName>('volumes');
const searchInput = ref('');
const search = ref('');
const region = ref('');
const backendType = ref<StorageBackendType | ''>('');
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);
const enrollmentCursor = ref<string>();
const enrollmentCursorHistory = ref<string[]>([]);
const tenantScopeVersion = ref(0);

const enrollmentOpen = ref(false);
const enrollmentError = ref('');
const tokenResult = ref<CreateStorageEnrollmentTokenResponse>();
const pendingTokenRequest = ref<CreateStorageEnrollmentTokenRequest>();
const enrollmentForm = reactive({
  storageVolumeId: '',
  displayName: '',
  edgeClusterId: '',
  region: '',
  accessMode: 'read_write_many' as StorageEnrollmentAccessMode,
  pvcNamespace: '',
  pvcClaimName: '',
});

const nfsOpen = ref(false);
const nfsError = ref('');
const nfsApiError = ref<unknown>();
const nfsForm = reactive({
  storageVolumeId: '',
  displayName: '',
  edgeClusterId: '',
  region: '',
  accessMode: 'read_write_many' as Extract<
    StorageAccessMode,
    'read_write_many' | 'read_write_once'
  >,
  server: '',
  exportPath: '',
});

const approvalRequests = new Map<string, ApproveStorageEnrollmentRequest>();
const rejectionRequests = new Map<string, RejectStorageEnrollmentRequest>();
const approvalError = ref<unknown>();
const rejectionError = ref<unknown>();

const tokenMutation = useMutation({ mutationFn: createStorageEnrollmentToken });
const nfsMutation = useMutation({ mutationFn: createStorageVolume });
const approveMutation = useMutation({ mutationFn: approveStorageEnrollment });
const rejectMutation = useMutation({ mutationFn: rejectStorageEnrollment });

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

const enrollmentsQuery = useQuery({
  queryKey: computed(() => [
    'storage-enrollments',
    tenantId.value,
    'pending_approval',
    enrollmentCursor.value ?? '',
  ]),
  enabled: computed(() => canReadEnrollments.value && activeView.value === 'enrollments'),
  queryFn: () =>
    queryStorageEnrollmentList({
      tenant_id: tenantId.value,
      state: 'pending_approval',
      page_size: 50,
      ...(enrollmentCursor.value ? { cursor: enrollmentCursor.value } : {}),
    }),
});

function centralEndpoint(): string {
  const configured = runtimeConfig.apiBaseUrl.trim();
  if (!configured) return globalThis.location.origin;
  try {
    return new URL(configured).toString().replace(/\/$/, '');
  } catch {
    return globalThis.location.origin;
  }
}

const deploymentConfig = computed(() => {
  const token = tokenResult.value;
  const descriptor = pendingTokenRequest.value;
  if (!token || !descriptor) return '';
  return [
    'schema_version: 1',
    'protocol_version: 1',
    `central_endpoint: ${centralEndpoint()}`,
    `tenant_id: ${descriptor.tenant_id}`,
    `edge_cluster_id: ${descriptor.edge_cluster_id}`,
    `storage_volume_id: ${descriptor.storage_volume_id}`,
    `region: ${descriptor.region}`,
    'storage:',
    '  backend_type: pvc',
    `  access_mode: ${descriptor.access_mode}`,
    '  mount_path: /volume',
    '  state_dir: /var/lib/neoengram-agent',
    '  marker_file: /volume/.neoengram-volume-marker',
    `  expected_volume_marker: ${descriptor.storage_volume_id}`,
    '  pvc_reference:',
    `    namespace: ${descriptor.pvc_reference.namespace}`,
    `    claim_name: ${descriptor.pvc_reference.claim_name}`,
    'registration:',
    '  approval_required: true',
    `  token_id: ${token.token_id}`,
    '  bootstrap_token_file: /var/run/secrets/neoengram/bootstrap-token',
    'session:',
    '  heartbeat_interval_seconds: 10',
    '  reconnect_max_delay_seconds: 30',
    'logging:',
    '  format: json',
    '  level: info',
  ].join('\n');
});

watch([tenantId, region, backendType], () => {
  cursor.value = undefined;
  cursorHistory.value = [];
});

watch(tenantId, () => {
  tenantScopeVersion.value += 1;
  activeView.value = 'volumes';
  enrollmentCursor.value = undefined;
  enrollmentCursorHistory.value = [];
  enrollmentOpen.value = false;
  nfsOpen.value = false;
  clearEnrollmentSecret();
  nfsError.value = '';
  nfsApiError.value = undefined;
  approvalError.value = undefined;
  rejectionError.value = undefined;
  nfsMutation.reset();
  approveMutation.reset();
  rejectMutation.reset();
  approvalRequests.clear();
  rejectionRequests.clear();
});

watch(
  enrollmentForm,
  () => {
    pendingTokenRequest.value = undefined;
    tokenResult.value = undefined;
    enrollmentError.value = '';
  },
  { deep: true },
);

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

function nextEnrollmentPage(): void {
  const next = enrollmentsQuery.data.value?.data.next_cursor;
  if (!next) return;
  enrollmentCursorHistory.value.push(enrollmentCursor.value ?? '');
  enrollmentCursor.value = next;
}

function previousEnrollmentPage(): void {
  const previous = enrollmentCursorHistory.value.pop();
  enrollmentCursor.value = previous || undefined;
}

function openEnrollment(): void {
  Object.assign(enrollmentForm, {
    storageVolumeId: '',
    displayName: '',
    edgeClusterId: '',
    region: '',
    accessMode: 'read_write_many',
    pvcNamespace: '',
    pvcClaimName: '',
  });
  pendingTokenRequest.value = undefined;
  tokenResult.value = undefined;
  enrollmentError.value = '';
  tokenMutation.reset();
  enrollmentOpen.value = true;
}

function clearEnrollmentSecret(): void {
  tokenResult.value = undefined;
  pendingTokenRequest.value = undefined;
  enrollmentError.value = '';
  tokenMutation.reset();
}

function validateResourceFields(fields: {
  storageVolumeId: string;
  displayName: string;
  edgeClusterId: string;
  region: string;
}): boolean {
  const resourceId = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
  const regionName = /^[a-z0-9][a-z0-9-]{0,63}$/;
  return (
    resourceId.test(fields.storageVolumeId) &&
    resourceId.test(fields.edgeClusterId) &&
    Boolean(fields.displayName.trim()) &&
    regionName.test(fields.region)
  );
}

async function submitEnrollment(): Promise<void> {
  enrollmentError.value = '';
  if (!validateResourceFields(enrollmentForm)) {
    enrollmentError.value = '请填写合法的 StorageVolume ID、名称、EdgeCluster ID 和 Region';
    return;
  }
  const kubernetesNamespace = /^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$/;
  const kubernetesClaim =
    /^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)*$/;
  const pvcNamespace = enrollmentForm.pvcNamespace.trim();
  const pvcClaimName = enrollmentForm.pvcClaimName.trim();
  if (
    pvcNamespace.length > 63 ||
    !kubernetesNamespace.test(pvcNamespace) ||
    pvcClaimName.length > 253 ||
    !kubernetesClaim.test(pvcClaimName)
  ) {
    enrollmentError.value = 'PVC Namespace 必须是 DNS label，Claim name 必须是 DNS subdomain';
    return;
  }

  pendingTokenRequest.value ??= {
    tenant_id: tenantId.value,
    token_request_id: `storage-enrollment-token-${globalThis.crypto.randomUUID()}`,
    storage_volume_id: enrollmentForm.storageVolumeId,
    display_name: enrollmentForm.displayName.trim(),
    edge_cluster_id: enrollmentForm.edgeClusterId,
    region: enrollmentForm.region,
    access_mode: enrollmentForm.accessMode,
    pvc_reference: {
      namespace: pvcNamespace,
      claim_name: pvcClaimName,
    },
  };
  const request = pendingTokenRequest.value;
  const requestTenantId = tenantId.value;

  try {
    const result = await tokenMutation.mutateAsync(request);
    if (tenantId.value !== requestTenantId || pendingTokenRequest.value !== request) return;
    tokenResult.value = result.data;
    ElMessage.success(result.data.replayed ? '已返回原接入凭证' : '接入凭证已生成');
  } catch (error) {
    if (tenantId.value !== requestTenantId || pendingTokenRequest.value !== request) return;
    enrollmentError.value = error instanceof Error ? error.message : '生成接入凭证失败';
  }
}

function openNfsCreate(): void {
  Object.assign(nfsForm, {
    storageVolumeId: '',
    displayName: '',
    edgeClusterId: '',
    region: '',
    accessMode: 'read_write_many',
    server: '',
    exportPath: '',
  });
  nfsError.value = '';
  nfsApiError.value = undefined;
  nfsMutation.reset();
  nfsOpen.value = true;
}

async function submitNfsCreate(): Promise<void> {
  nfsError.value = '';
  nfsApiError.value = undefined;
  if (!validateResourceFields(nfsForm)) {
    nfsError.value = '请填写合法的 StorageVolume ID、名称、EdgeCluster ID 和 Region';
    return;
  }
  if (!nfsForm.server.trim() || !nfsForm.exportPath.startsWith('/')) {
    nfsError.value = 'NFS Server 不能为空，Export path 必须以 / 开头';
    return;
  }

  const requestTenantId = tenantId.value;
  const requestScopeVersion = tenantScopeVersion.value;
  const request = {
    tenant_id: requestTenantId,
    storage_volume_id: nfsForm.storageVolumeId,
    display_name: nfsForm.displayName.trim(),
    edge_cluster_id: nfsForm.edgeClusterId,
    region: nfsForm.region,
    backend_type: 'nfs' as const,
    access_mode: nfsForm.accessMode,
    nfs_reference: {
      server: nfsForm.server.trim(),
      export_path: nfsForm.exportPath.trim(),
    },
  };

  try {
    const result = await nfsMutation.mutateAsync(request);
    await queryClient.invalidateQueries({ queryKey: ['storage-volumes', request.tenant_id] });
    if (!isCurrentTenantScope(requestTenantId, requestScopeVersion)) return;
    nfsOpen.value = false;
    ElMessage.success(
      result.data.replayed ? '已返回现有 StorageVolume' : '已登记，等待挂载健康检查',
    );
  } catch (error) {
    if (!isCurrentTenantScope(requestTenantId, requestScopeVersion)) return;
    nfsApiError.value = error;
  }
}

function reviewRequestKey(requestTenantId: string, storageEnrollmentId: string): string {
  return JSON.stringify([requestTenantId, storageEnrollmentId]);
}

function isCurrentTenantScope(requestTenantId: string, requestScopeVersion: number): boolean {
  return tenantId.value === requestTenantId && tenantScopeVersion.value === requestScopeVersion;
}

async function approve(enrollment: StorageEnrollmentView): Promise<void> {
  const requestTenantId = tenantId.value;
  const requestScopeVersion = tenantScopeVersion.value;
  const requestKey = reviewRequestKey(requestTenantId, enrollment.storage_enrollment_id);
  approvalError.value = undefined;
  rejectionError.value = undefined;
  let request = approvalRequests.get(requestKey);
  if (!request) {
    try {
      await ElMessageBox.confirm(
        enrollment.registration_kind === 'replacement'
          ? '确认旧实例已停止且 PVC 已解除旧挂载后，再批准接管。'
          : '批准后将创建或绑定 StorageVolume；它会保持 unavailable，直到接入实例上报健康 RW 挂载。',
        enrollment.registration_kind === 'replacement' ? '确认接管' : '批准存储接入',
        { type: 'warning', confirmButtonText: '批准', cancelButtonText: '取消' },
      );
    } catch {
      return;
    }
    if (!isCurrentTenantScope(requestTenantId, requestScopeVersion)) return;
    request = {
      tenant_id: requestTenantId,
      storage_enrollment_id: enrollment.storage_enrollment_id,
      approval_request_id: `storage-enrollment-approve-${globalThis.crypto.randomUUID()}`,
      expected_resource_version: enrollment.resource_version,
      confirm_replacement: enrollment.registration_kind === 'replacement',
    };
    approvalRequests.set(requestKey, request);
  }

  try {
    const result = await approveMutation.mutateAsync(request);
    if (approvalRequests.get(requestKey) === request) approvalRequests.delete(requestKey);
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ['storage-enrollments', request.tenant_id] }),
      queryClient.invalidateQueries({ queryKey: ['storage-volumes', request.tenant_id] }),
    ]);
    if (!isCurrentTenantScope(requestTenantId, requestScopeVersion)) return;
    ElMessage.success(result.data.replayed ? '已返回原审批结果' : '存储接入已批准');
  } catch (error) {
    if (isApiProblem(error) && !error.retryable) {
      if (approvalRequests.get(requestKey) === request) approvalRequests.delete(requestKey);
      await queryClient.invalidateQueries({
        queryKey: ['storage-enrollments', request.tenant_id],
      });
    }
    if (isCurrentTenantScope(requestTenantId, requestScopeVersion)) approvalError.value = error;
    // Transport and retryable service failures retain the exact request.
  }
}

async function reject(enrollment: StorageEnrollmentView): Promise<void> {
  const requestTenantId = tenantId.value;
  const requestScopeVersion = tenantScopeVersion.value;
  const requestKey = reviewRequestKey(requestTenantId, enrollment.storage_enrollment_id);
  approvalError.value = undefined;
  rejectionError.value = undefined;
  let request = rejectionRequests.get(requestKey);
  if (!request) {
    try {
      const prompt = await ElMessageBox.prompt(
        '拒绝后旧安装身份和密钥会退休；再次接入需要初始化新身份并使用新的 bootstrap token。',
        '拒绝存储接入',
        {
          type: 'warning',
          inputPlaceholder: '拒绝原因',
          inputValidator: (value: string) => value.trim().length > 0 || '请填写拒绝原因',
          confirmButtonText: '拒绝',
          cancelButtonText: '取消',
        },
      );
      if (!isCurrentTenantScope(requestTenantId, requestScopeVersion)) return;
      request = {
        tenant_id: requestTenantId,
        storage_enrollment_id: enrollment.storage_enrollment_id,
        rejection_request_id: `storage-enrollment-reject-${globalThis.crypto.randomUUID()}`,
        expected_resource_version: enrollment.resource_version,
        reason: prompt.value.trim(),
      };
      rejectionRequests.set(requestKey, request);
    } catch {
      return;
    }
  }

  try {
    const result = await rejectMutation.mutateAsync(request);
    if (rejectionRequests.get(requestKey) === request) rejectionRequests.delete(requestKey);
    await queryClient.invalidateQueries({
      queryKey: ['storage-enrollments', request.tenant_id],
    });
    if (!isCurrentTenantScope(requestTenantId, requestScopeVersion)) return;
    ElMessage.success(result.data.replayed ? '已返回原拒绝结果' : '存储接入已拒绝');
  } catch (error) {
    if (isApiProblem(error) && !error.retryable) {
      if (rejectionRequests.get(requestKey) === request) rejectionRequests.delete(requestKey);
      await queryClient.invalidateQueries({
        queryKey: ['storage-enrollments', request.tenant_id],
      });
    }
    if (isCurrentTenantScope(requestTenantId, requestScopeVersion)) rejectionError.value = error;
    // Transport and retryable service failures retain the exact request.
  }
}

async function copyText(value: string): Promise<void> {
  try {
    await globalThis.navigator.clipboard.writeText(value);
    ElMessage.success('已复制');
  } catch {
    ElMessage.error('复制失败');
  }
}

function volumeStateType(state: string): TagType {
  if (state === 'ready') return 'success';
  if (state === 'degraded') return 'warning';
  return 'danger';
}

function enrollmentStateLabel(state: StorageEnrollmentState): string {
  return {
    pending_approval: '待审批',
    approved: '已批准',
    enrolled: '已接入',
    rejected: '已拒绝',
    expired: '已过期',
  }[state];
}

function enrollmentStateType(state: StorageEnrollmentState): TagType {
  if (state === 'enrolled') return 'success';
  if (state === 'approved' || state === 'pending_approval') return 'warning';
  if (state === 'rejected' || state === 'expired') return 'danger';
  return 'info';
}

function probePassed(enrollment: StorageEnrollmentView): boolean {
  return (
    enrollment.probe.descriptor_matches &&
    enrollment.probe.protocol_compatible &&
    enrollment.probe.observed_access_mode === 'read_write'
  );
}

function fingerprintSummary(value: string): string {
  if (value.length <= 24) return value;
  return `${value.slice(0, 12)}...${value.slice(-8)}`;
}
</script>

<template>
  <div class="page storage-page">
    <PageHeading title="存储资源" :description="`${tenantId} 内按区域接入的 StorageVolume`">
      <template #actions>
        <el-button v-if="canCreateNfs" :icon="Plus" @click="openNfsCreate">登记 NFS</el-button>
        <el-button v-if="canCreateEnrollment" type="primary" :icon="Key" @click="openEnrollment">
          接入 PVC
        </el-button>
      </template>
    </PageHeading>

    <el-tabs v-model="activeView" class="storage-tabs">
      <el-tab-pane label="已登记" name="volumes">
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
                  <el-tag :type="volumeStateType(scope.row.state)" effect="plain">
                    {{ scope.row.state }}
                  </el-tag>
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
                    >{{ storageVolume.region }} ·
                    {{ storageVolume.backend_type.toUpperCase() }}</small
                  >
                  <el-tag :type="volumeStateType(storageVolume.state)" size="small" effect="plain">
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
      </el-tab-pane>

      <el-tab-pane v-if="canReadEnrollments" label="待审批" name="enrollments">
        <ApiProblemAlert
          v-if="enrollmentsQuery.error.value"
          :error="enrollmentsQuery.error.value"
          :retrying="enrollmentsQuery.isFetching.value"
          @retry="enrollmentsQuery.refetch"
        />
        <ApiProblemAlert
          v-if="approvalError || rejectionError"
          :error="approvalError ?? rejectionError"
        />

        <section class="content-section resource-section enrollment-section">
          <el-skeleton v-if="enrollmentsQuery.isPending.value" :rows="6" animated />
          <el-empty
            v-else-if="!enrollmentsQuery.data.value?.data.items.length"
            description="没有待审批的存储接入"
            :image-size="78"
          />
          <template v-else>
            <el-table
              :data="enrollmentsQuery.data.value?.data.items"
              class="resource-table desktop-table"
            >
              <el-table-column label="存储" min-width="230">
                <template #default="scope">
                  <div class="resource-identity">
                    <strong>{{ scope.row.display_name }}</strong>
                    <code>{{ scope.row.storage_volume_id }}</code>
                    <small>{{
                      scope.row.registration_kind === 'replacement' ? '替换接入' : '首次接入'
                    }}</small>
                  </div>
                </template>
              </el-table-column>
              <el-table-column label="PVC" min-width="210">
                <template #default="scope">
                  <strong>{{ scope.row.pvc_reference.namespace }}</strong>
                  <small class="table-secondary">{{ scope.row.pvc_reference.claim_name }}</small>
                </template>
              </el-table-column>
              <el-table-column label="位置 / 访问" min-width="210">
                <template #default="scope">
                  <div class="enrollment-scope">
                    <strong>{{ scope.row.region }}</strong>
                    <small>{{ scope.row.edge_cluster_id }}</small>
                    <code>{{ scope.row.access_mode }}</code>
                  </div>
                </template>
              </el-table-column>
              <el-table-column label="接入探测" min-width="180">
                <template #default="scope">
                  <div class="enrollment-probe">
                    <el-tag :type="probePassed(scope.row) ? 'success' : 'danger'" effect="plain">
                      {{ probePassed(scope.row) ? '通过' : '不匹配' }}
                    </el-tag>
                    <small
                      >{{ scope.row.agent_version }} ·
                      {{ scope.row.probe.observed_access_mode }}</small
                    >
                  </div>
                </template>
              </el-table-column>
              <el-table-column label="身份摘要" min-width="175">
                <template #default="scope">
                  <code class="fingerprint">{{
                    fingerprintSummary(scope.row.identity_fingerprint)
                  }}</code>
                </template>
              </el-table-column>
              <el-table-column label="状态" width="110">
                <template #default="scope">
                  <el-tag :type="enrollmentStateType(scope.row.state)" effect="plain">
                    {{ enrollmentStateLabel(scope.row.state) }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column v-if="canReviewEnrollments" label="操作" width="170" fixed="right">
                <template #default="scope">
                  <div class="row-actions">
                    <el-tooltip content="批准接入" placement="top">
                      <el-button
                        type="success"
                        :icon="Check"
                        circle
                        :aria-label="`批准 ${scope.row.storage_volume_id}`"
                        :disabled="!probePassed(scope.row)"
                        :loading="approveMutation.isPending.value"
                        @click="approve(scope.row)"
                      />
                    </el-tooltip>
                    <el-tooltip content="拒绝接入" placement="top">
                      <el-button
                        type="danger"
                        :icon="Close"
                        circle
                        :aria-label="`拒绝 ${scope.row.storage_volume_id}`"
                        :loading="rejectMutation.isPending.value"
                        @click="reject(scope.row)"
                      />
                    </el-tooltip>
                  </div>
                </template>
              </el-table-column>
            </el-table>

            <div class="mobile-resource-list">
              <div
                v-for="enrollment in enrollmentsQuery.data.value?.data.items"
                :key="enrollment.storage_enrollment_id"
                class="mobile-resource-item enrollment-mobile-item"
              >
                <span>
                  <strong>{{ enrollment.display_name }}</strong>
                  <code>{{ enrollment.storage_volume_id }}</code>
                  <small
                    >{{ enrollment.pvc_reference.namespace }}/{{
                      enrollment.pvc_reference.claim_name
                    }}</small
                  >
                  <small>{{ enrollment.region }} · {{ enrollment.edge_cluster_id }}</small>
                  <code>{{ enrollment.access_mode }}</code>
                </span>
                <span>
                  <el-tag
                    :type="probePassed(enrollment) ? 'success' : 'danger'"
                    size="small"
                    effect="plain"
                  >
                    {{ probePassed(enrollment) ? '探测通过' : '探测不匹配' }}
                  </el-tag>
                  <small>{{ enrollment.agent_version }}</small>
                  <code class="fingerprint">{{
                    fingerprintSummary(enrollment.identity_fingerprint)
                  }}</code>
                  <div v-if="canReviewEnrollments" class="row-actions">
                    <el-button
                      type="success"
                      :icon="Check"
                      circle
                      :aria-label="`批准 ${enrollment.storage_volume_id}`"
                      :disabled="!probePassed(enrollment)"
                      @click="approve(enrollment)"
                    />
                    <el-button
                      type="danger"
                      :icon="Close"
                      circle
                      :aria-label="`拒绝 ${enrollment.storage_volume_id}`"
                      @click="reject(enrollment)"
                    />
                  </div>
                </span>
              </div>
            </div>

            <PageCursor
              :has-previous="enrollmentCursorHistory.length > 0"
              :has-next="Boolean(enrollmentsQuery.data.value?.data.next_cursor)"
              :loading="enrollmentsQuery.isFetching.value"
              @previous="previousEnrollmentPage"
              @next="nextEnrollmentPage"
            />
          </template>
        </section>
      </el-tab-pane>
    </el-tabs>

    <el-dialog
      v-model="enrollmentOpen"
      title="接入 PVC"
      width="min(680px, calc(100vw - 32px))"
      :close-on-click-modal="false"
      destroy-on-close
      @closed="clearEnrollmentSecret"
    >
      <ApiProblemAlert v-if="tokenMutation.error.value" :error="tokenMutation.error.value" />
      <el-alert v-if="enrollmentError" :title="enrollmentError" type="error" :closable="false" />

      <template v-if="!tokenResult">
        <el-alert
          title="审批仅授权中心调度；PVC 挂载权限由 Kubernetes 管理。"
          type="info"
          :closable="false"
          show-icon
        />
        <el-form label-position="top" class="dialog-form enrollment-form">
          <div class="dialog-form-grid">
            <el-form-item label="StorageVolume ID" required>
              <el-input v-model="enrollmentForm.storageVolumeId" placeholder="volume-vision" />
            </el-form-item>
            <el-form-item label="名称" required>
              <el-input v-model="enrollmentForm.displayName" placeholder="视觉数据 PVC" />
            </el-form-item>
            <el-form-item label="EdgeCluster ID" required>
              <el-input v-model="enrollmentForm.edgeClusterId" placeholder="cluster-cn-east-1" />
            </el-form-item>
            <el-form-item label="Region" required>
              <el-input v-model="enrollmentForm.region" placeholder="cn-shanghai" />
            </el-form-item>
            <el-form-item label="PVC Namespace" required>
              <el-input v-model="enrollmentForm.pvcNamespace" placeholder="neoengram-data" />
            </el-form-item>
            <el-form-item label="PVC Claim name" required>
              <el-input v-model="enrollmentForm.pvcClaimName" placeholder="vision-data" />
            </el-form-item>
          </div>
          <el-form-item label="访问模式" required>
            <el-select v-model="enrollmentForm.accessMode">
              <el-option label="ReadWriteMany" value="read_write_many" />
              <el-option label="ReadWriteOnce" value="read_write_once" />
            </el-select>
          </el-form-item>
        </el-form>
      </template>

      <div v-else class="token-result">
        <el-alert
          title="bootstrap token 仅显示在本次响应中，15 分钟内有效。"
          type="success"
          :closable="false"
          show-icon
        />
        <div class="token-secret">
          <code aria-label="Bootstrap token">{{ tokenResult.bootstrap_token }}</code>
          <el-tooltip content="复制 bootstrap token" placement="top">
            <el-button
              :icon="CopyDocument"
              circle
              aria-label="复制 bootstrap token"
              @click="copyText(tokenResult.bootstrap_token)"
            />
          </el-tooltip>
        </div>
        <dl class="token-metadata">
          <div>
            <dt>Token ID</dt>
            <dd>
              <code>{{ tokenResult.token_id }}</code>
            </dd>
          </div>
          <div>
            <dt>过期时间</dt>
            <dd>{{ formatTime(tokenResult.expires_at_unix_ms) }}</dd>
          </div>
        </dl>
        <div class="deployment-config-heading">
          <strong>Agent 配置</strong>
          <el-tooltip content="复制 Agent 配置" placement="top">
            <el-button
              :icon="CopyDocument"
              circle
              aria-label="复制 Agent 配置"
              @click="copyText(deploymentConfig)"
            />
          </el-tooltip>
        </div>
        <pre class="deployment-config"><code>{{ deploymentConfig }}</code></pre>
      </div>

      <template #footer>
        <el-button v-if="!tokenResult" @click="enrollmentOpen = false">取消</el-button>
        <el-button
          v-if="!tokenResult"
          type="primary"
          :loading="tokenMutation.isPending.value"
          @click="submitEnrollment"
        >
          生成接入凭证
        </el-button>
        <el-button v-else type="primary" @click="enrollmentOpen = false">完成</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="nfsOpen" title="登记 NFS" width="min(620px, calc(100vw - 32px))">
      <ApiProblemAlert v-if="nfsApiError" :error="nfsApiError" />
      <el-alert v-if="nfsError" :title="nfsError" type="error" :closable="false" />
      <el-form label-position="top" class="dialog-form">
        <div class="dialog-form-grid">
          <el-form-item label="StorageVolume ID" required>
            <el-input v-model="nfsForm.storageVolumeId" placeholder="volume-archive" />
          </el-form-item>
          <el-form-item label="名称" required>
            <el-input v-model="nfsForm.displayName" placeholder="共享归档" />
          </el-form-item>
          <el-form-item label="EdgeCluster ID" required>
            <el-input v-model="nfsForm.edgeClusterId" placeholder="cluster-cn-east-1" />
          </el-form-item>
          <el-form-item label="Region" required>
            <el-input v-model="nfsForm.region" placeholder="cn-shanghai" />
          </el-form-item>
          <el-form-item label="NFS Server" required>
            <el-input v-model="nfsForm.server" placeholder="nas.internal" />
          </el-form-item>
          <el-form-item label="NFS Export path" required>
            <el-input v-model="nfsForm.exportPath" placeholder="/exports/team-a" />
          </el-form-item>
        </div>
        <el-form-item label="访问模式" required>
          <el-select v-model="nfsForm.accessMode">
            <el-option label="ReadWriteMany" value="read_write_many" />
            <el-option label="ReadWriteOnce" value="read_write_once" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="nfsOpen = false">取消</el-button>
        <el-button type="primary" :loading="nfsMutation.isPending.value" @click="submitNfsCreate">
          登记 NFS
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.storage-tabs :deep(.el-tabs__header) {
  margin-bottom: 18px;
}

.enrollment-section {
  min-height: 260px;
}

.enrollment-probe,
.enrollment-scope {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 6px;
}

.fingerprint {
  overflow-wrap: anywhere;
}

.row-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.enrollment-form {
  margin-top: 18px;
}

.token-result {
  display: grid;
  gap: 16px;
}

.token-secret {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 36px;
  align-items: center;
  gap: 10px;
}

.token-secret code {
  min-width: 0;
  padding: 11px 12px;
  overflow-wrap: anywhere;
  border: 1px solid var(--el-border-color);
  border-radius: 6px;
  background: var(--el-fill-color-light);
}

.token-metadata {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  margin: 0;
}

.token-metadata div {
  min-width: 0;
}

.token-metadata dt {
  margin-bottom: 4px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.token-metadata dd {
  margin: 0;
  overflow-wrap: anywhere;
}

.deployment-config-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.deployment-config {
  max-height: 280px;
  margin: 0;
  padding: 14px;
  overflow: auto;
  border: 1px solid var(--el-border-color);
  border-radius: 6px;
  background: #101827;
  color: #e5edf7;
  font-size: 12px;
  line-height: 1.65;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

.enrollment-mobile-item {
  align-items: flex-start;
}

@media (max-width: 720px) {
  .token-metadata {
    grid-template-columns: 1fr;
  }

  .enrollment-mobile-item {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    align-items: stretch;
    gap: 12px;
    cursor: default;
  }

  .enrollment-mobile-item > span {
    width: 100%;
    min-width: 0;
  }

  .enrollment-mobile-item > span:last-child {
    align-items: center;
    flex-wrap: wrap;
  }

  .enrollment-mobile-item .row-actions {
    flex: 0 0 auto;
    margin-left: auto;
  }
}
</style>
