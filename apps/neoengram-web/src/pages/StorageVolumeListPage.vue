<script setup lang="ts">
import {
  Check,
  Close,
  Coin,
  Connection,
  CopyDocument,
  Key,
  Plus,
  Search,
} from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute } from 'vue-router';

import {
  approveStorageEnrollment,
  queryApiVersion,
  createStorageEnrollmentToken,
  createStorageVolume,
  queryGatewayPoolList,
  queryStorageEnrollmentList,
  queryStorageVolumeList,
  rejectStorageEnrollment,
} from '@/api/operations';
import { isApiProblem } from '@/api/problem';
import type {
  ApproveStorageEnrollmentRequest,
  CreateStorageEnrollmentTokenRequest,
  CreateStorageEnrollmentTokenResponse,
  GatewayPoolState,
  RejectStorageEnrollmentRequest,
  StorageAccessMode,
  StorageBackendType,
  StorageEnrollmentAccessMode,
  StorageEnrollmentState,
  StorageEnrollmentView,
} from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import ResourceDeletionDialog from '@/components/ResourceDeletionDialog.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import { runtimeConfig } from '@/config';
import { supportsResourceLifecycle } from '@/features/capabilities';
import { lifecycleResourceVersion } from '@/features/lifecycle';
import { groupStorageVolumesByCluster } from '@/features/storage/cluster-groups';
import {
  buildAgentConfig,
  canonicalGatewayEndpoint,
  canonicalGatewayWorkloadTrustDomain,
  validateGatewayClusterBinding,
} from '@/features/storage/agent-config';
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
const canReadGateways = computed(() => permissions.value.includes('gateway.read'));
const canReviewEnrollments = computed(() =>
  permissions.value.includes('storage.enrollment.review'),
);
const versionQuery = useQuery({
  queryKey: ['system', 'version'],
  queryFn: queryApiVersion,
  staleTime: Number.POSITIVE_INFINITY,
});
const lifecycleEnabled = computed(
  () =>
    supportsResourceLifecycle(versionQuery.data.value?.data.capabilities) &&
    permissions.value.includes('resource.lifecycle.manage' as never),
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
  edgeClusterId: runtimeConfig.gatewayEdgeClusterId,
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
  refetchInterval: 5_000,
  refetchIntervalInBackground: false,
});

const gatewayPoolsQuery = useQuery({
  queryKey: ['gateway-pools'],
  enabled: canReadGateways,
  queryFn: () => queryGatewayPoolList({ page_size: 255 }),
  refetchInterval: 5_000,
  refetchIntervalInBackground: false,
});

const storageClusters = computed(() =>
  groupStorageVolumesByCluster(
    storageVolumesQuery.data.value?.data.items ?? [],
    canReadGateways.value ? (gatewayPoolsQuery.data.value?.data.items ?? []) : [],
  ),
);

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
  refetchInterval: 5_000,
  refetchIntervalInBackground: false,
});

const deploymentConfigResult = computed(() => {
  const token = tokenResult.value;
  const descriptor = pendingTokenRequest.value;
  if (!token || !descriptor) return { yaml: '', error: '' };
  try {
    return {
      yaml: buildAgentConfig(
        runtimeConfig.gatewayEndpoint,
        runtimeConfig.gatewayWorkloadTrustDomain,
        descriptor,
        token,
        runtimeConfig.gatewayEdgeClusterId,
      ),
      error: '',
    };
  } catch (error) {
    return {
      yaml: '',
      error: error instanceof Error ? error.message : 'Agent 配置生成失败',
    };
  }
});
const deploymentConfig = computed(() => deploymentConfigResult.value.yaml);

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
    edgeClusterId: runtimeConfig.gatewayEdgeClusterId,
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
  try {
    const endpoint = canonicalGatewayEndpoint(runtimeConfig.gatewayEndpoint);
    if (new URL(endpoint).protocol === 'https:') {
      canonicalGatewayWorkloadTrustDomain(runtimeConfig.gatewayWorkloadTrustDomain);
    }
  } catch (error) {
    enrollmentError.value = error instanceof Error ? error.message : 'Gateway endpoint 配置无效';
    return;
  }
  if (!validateResourceFields(enrollmentForm)) {
    enrollmentError.value = '请填写合法的 StorageVolume ID、名称、EdgeCluster ID 和 Region';
    return;
  }
  try {
    validateGatewayClusterBinding(
      runtimeConfig.gatewayEndpoint,
      runtimeConfig.gatewayEdgeClusterId,
      enrollmentForm.edgeClusterId,
    );
  } catch (error) {
    enrollmentError.value = error instanceof Error ? error.message : 'Gateway 集群绑定配置无效';
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
    ElMessage.success(result.data.request_replayed ? '已返回原接入凭证' : '接入凭证已生成');
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
      result.data.request_replayed ? '已返回现有 StorageVolume' : '已登记，等待挂载健康检查',
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
    ElMessage.success(result.data.request_replayed ? '已返回原审批结果' : '存储接入已批准');
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
    ElMessage.success(result.data.request_replayed ? '已返回原拒绝结果' : '存储接入已拒绝');
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

function gatewayPoolStateType(state: GatewayPoolState): TagType {
  if (state === 'ready') return 'success';
  if (state === 'provisioning' || state === 'draining') return 'warning';
  return 'info';
}

function gatewayPoolStateLabel(state: GatewayPoolState): string {
  return {
    provisioning: '配置中',
    ready: '就绪',
    draining: '排空中',
    disabled: '已停用',
  }[state];
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
    enrollment.probe.descriptor_matches && enrollment.probe.observed_access_mode === 'read_write'
  );
}

function fingerprintSummary(value: string): string {
  if (value.length <= 24) return value;
  return `${value.slice(0, 12)}...${value.slice(-8)}`;
}
</script>

<template>
  <div class="page storage-page">
    <PageHeading title="集群与存储" :description="`${tenantId} 的 Gateway 集群与磁盘`">
      <template #actions>
        <el-button v-if="canCreateNfs" :icon="Plus" @click="openNfsCreate">登记 NFS</el-button>
        <el-button v-if="canCreateEnrollment" type="primary" :icon="Key" @click="openEnrollment">
          接入 PVC
        </el-button>
      </template>
    </PageHeading>

    <el-tabs v-model="activeView" class="storage-tabs">
      <el-tab-pane label="集群与磁盘" name="volumes">
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
        <ApiProblemAlert
          v-if="canReadGateways && gatewayPoolsQuery.error.value"
          :error="gatewayPoolsQuery.error.value"
          :retrying="gatewayPoolsQuery.isFetching.value"
          @retry="gatewayPoolsQuery.refetch"
        />

        <section class="cluster-browser" aria-label="Gateway 集群与磁盘">
          <el-skeleton
            v-if="storageVolumesQuery.isPending.value"
            class="cluster-browser__loading"
            :rows="7"
            animated
          />
          <el-empty
            v-else-if="!storageClusters.length"
            class="cluster-browser__empty"
            description="当前筛选下没有 StorageVolume"
            :image-size="78"
          />
          <template v-else>
            <div class="gateway-cluster-list">
              <section
                v-for="cluster in storageClusters"
                :key="cluster.edgeClusterId"
                class="gateway-cluster"
                :aria-labelledby="`gateway-cluster-${cluster.edgeClusterId}`"
              >
                <header class="gateway-cluster__header">
                  <div class="gateway-cluster__identity">
                    <span class="gateway-cluster__icon" aria-hidden="true"><Connection /></span>
                    <span>
                      <small>Gateway 集群</small>
                      <strong :id="`gateway-cluster-${cluster.edgeClusterId}`">
                        {{ cluster.gatewayPool?.display_name ?? cluster.edgeClusterId }}
                      </strong>
                      <span class="gateway-cluster__ids">
                        <code>{{ cluster.edgeClusterId }}</code>
                        <code v-if="cluster.gatewayPool">
                          {{ cluster.gatewayPool.gateway_pool_id }}
                        </code>
                      </span>
                    </span>
                  </div>

                  <dl class="gateway-cluster__facts">
                    <div>
                      <dt>Agent 入口</dt>
                      <dd>
                        <code>{{ cluster.gatewayPool?.agent_endpoint ?? '未公开' }}</code>
                      </dd>
                    </div>
                    <div>
                      <dt>磁盘状态</dt>
                      <dd>
                        <strong>{{ cluster.readyVolumeCount }}/{{ cluster.volumes.length }}</strong>
                        <span>就绪</span>
                      </dd>
                    </div>
                  </dl>

                  <div class="gateway-cluster__state">
                    <el-tag
                      v-if="cluster.gatewayPool"
                      :type="gatewayPoolStateType(cluster.gatewayPool.state)"
                      effect="plain"
                    >
                      {{ gatewayPoolStateLabel(cluster.gatewayPool.state) }}
                    </el-tag>
                    <el-tag v-else type="info" effect="plain">EdgeCluster</el-tag>
                    <small v-if="cluster.gatewayPool">
                      副本 {{ cluster.gatewayPool.desired_replicas }} · 最低
                      {{ cluster.gatewayPool.minimum_ready_replicas }}
                    </small>
                  </div>
                </header>

                <div class="gateway-cluster__volume-heading">
                  <Coin aria-hidden="true" />
                  <strong>磁盘</strong>
                  <span>{{ cluster.regions.join(' · ') }}</span>
                </div>

                <el-table :data="cluster.volumes" class="resource-table desktop-table">
                  <el-table-column label="StorageVolume" min-width="230">
                    <template #default="scope">
                      <div class="resource-identity">
                        <strong>{{ scope.row.display_name }}</strong>
                        <code>{{ scope.row.storage_volume_id }}</code>
                      </div>
                    </template>
                  </el-table-column>
                  <el-table-column prop="region" label="Region" min-width="125" />
                  <el-table-column label="后端" min-width="180">
                    <template #default="scope">
                      <strong>{{ scope.row.backend_type.toUpperCase() }}</strong>
                      <small v-if="scope.row.pvc_reference" class="table-secondary">
                        {{ scope.row.pvc_reference.namespace }}/{{
                          scope.row.pvc_reference.claim_name
                        }}
                      </small>
                    </template>
                  </el-table-column>
                  <el-table-column prop="access_mode" label="访问模式" min-width="155" />
                  <el-table-column label="状态" width="105">
                    <template #default="scope">
                      <el-tag :type="volumeStateType(scope.row.state)" effect="plain">
                        {{ scope.row.state }}
                      </el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column label="更新时间" min-width="155">
                    <template #default="scope">
                      {{ formatTime(scope.row.updated_at_unix_ms) }}
                    </template>
                  </el-table-column>
                  <el-table-column v-if="lifecycleEnabled" label="操作" width="70" align="right">
                    <template #default="scope">
                      <ResourceDeletionDialog
                        :tenant-id="tenantId"
                        :resource="{
                          type: 'storage_volume',
                          storage_volume_id: scope.row.storage_volume_id,
                        }"
                        :resource-version="lifecycleResourceVersion(scope.row)"
                        :display-name="scope.row.display_name"
                      />
                    </template>
                  </el-table-column>
                </el-table>

                <div class="mobile-resource-list">
                  <div
                    v-for="storageVolume in cluster.volumes"
                    :key="storageVolume.storage_volume_id"
                    class="mobile-resource-item mobile-resource-item--static"
                  >
                    <span>
                      <strong>{{ storageVolume.display_name }}</strong>
                      <code>{{ storageVolume.storage_volume_id }}</code>
                    </span>
                    <span>
                      <small>
                        {{ storageVolume.region }} · {{ storageVolume.backend_type.toUpperCase() }}
                      </small>
                      <el-tag
                        :type="volumeStateType(storageVolume.state)"
                        size="small"
                        effect="plain"
                      >
                        {{ storageVolume.state }}
                      </el-tag>
                      <ResourceDeletionDialog
                        v-if="lifecycleEnabled"
                        :tenant-id="tenantId"
                        :resource="{
                          type: 'storage_volume',
                          storage_volume_id: storageVolume.storage_volume_id,
                        }"
                        :resource-version="lifecycleResourceVersion(storageVolume)"
                        :display-name="storageVolume.display_name"
                      />
                    </span>
                  </div>
                </div>
              </section>
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
              <el-input
                v-model="enrollmentForm.edgeClusterId"
                placeholder="cluster-cn-east-1"
                :disabled="Boolean(runtimeConfig.gatewayEdgeClusterId)"
              />
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
          <div>
            <dt>Descriptor digest</dt>
            <dd>
              <code>{{ tokenResult.volume_descriptor_digest }}</code>
            </dd>
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
        <el-alert
          v-if="deploymentConfigResult.error"
          :title="deploymentConfigResult.error"
          type="error"
          :closable="false"
        />
        <pre v-else class="deployment-config"><code>{{ deploymentConfig }}</code></pre>
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

.cluster-browser {
  margin-top: 22px;
}

.cluster-browser__loading,
.cluster-browser__empty {
  padding: 28px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
}

.gateway-cluster-list {
  display: grid;
  gap: 16px;
}

.gateway-cluster {
  min-width: 0;
  overflow: hidden;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
}

.gateway-cluster__header {
  display: grid;
  grid-template-columns: minmax(260px, 1.15fr) minmax(360px, 1fr) auto;
  align-items: center;
  gap: 22px;
  padding: 18px 20px;
  border-bottom: 1px solid var(--line);
  background: #f1f5f3;
}

.gateway-cluster__identity {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 12px;
}

.gateway-cluster__identity > span:last-child,
.gateway-cluster__identity strong,
.gateway-cluster__identity small {
  min-width: 0;
  display: block;
}

.gateway-cluster__identity small {
  color: var(--muted);
  font-size: 11px;
}

.gateway-cluster__identity strong {
  margin-top: 3px;
  overflow: hidden;
  font-size: 16px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.gateway-cluster__icon {
  flex: 0 0 auto;
  width: 38px;
  height: 38px;
  display: grid;
  place-items: center;
  border-radius: 6px;
  color: var(--green-dark);
  background: #dceae3;
}

.gateway-cluster__icon svg {
  width: 19px;
  height: 19px;
}

.gateway-cluster__ids {
  min-width: 0;
  display: flex;
  gap: 8px;
  margin-top: 5px;
}

.gateway-cluster__ids code {
  min-width: 0;
  overflow: hidden;
  color: var(--muted);
  font-size: 11px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.gateway-cluster__ids code + code::before {
  content: '/';
  margin-right: 8px;
  color: #a0aaa5;
}

.gateway-cluster__facts {
  min-width: 0;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 18px;
  margin: 0;
}

.gateway-cluster__facts div {
  min-width: 0;
}

.gateway-cluster__facts dt {
  margin-bottom: 4px;
  color: var(--muted);
  font-size: 11px;
}

.gateway-cluster__facts dd {
  min-width: 0;
  margin: 0;
}

.gateway-cluster__facts code {
  display: block;
  overflow: hidden;
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.gateway-cluster__facts dd span {
  margin-left: 4px;
  color: var(--muted);
  font-size: 11px;
}

.gateway-cluster__state {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 6px;
}

.gateway-cluster__state small {
  color: var(--muted);
  font-size: 10px;
  white-space: nowrap;
}

.gateway-cluster__volume-heading {
  min-height: 42px;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 0 20px;
  border-bottom: 1px solid var(--line);
  color: var(--ink);
}

.gateway-cluster__volume-heading svg {
  width: 16px;
  height: 16px;
  color: var(--green);
}

.gateway-cluster__volume-heading span {
  margin-left: auto;
  color: var(--muted);
  font-size: 11px;
}

.cluster-browser > .page-cursor {
  margin-top: 2px;
  border-top: 0;
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
  .gateway-cluster__header {
    grid-template-columns: 1fr;
    gap: 14px;
    padding: 16px;
  }

  .gateway-cluster__facts {
    grid-template-columns: minmax(0, 1fr) auto;
  }

  .gateway-cluster__state {
    flex-direction: row;
    align-items: center;
  }

  .gateway-cluster__volume-heading {
    padding: 0 16px;
  }

  .gateway-cluster .mobile-resource-item:last-child {
    border-bottom: 0;
  }

  .gateway-cluster .mobile-resource-item {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
  }

  .gateway-cluster .mobile-resource-item > span:first-child {
    padding-right: 4px;
  }

  .gateway-cluster .mobile-resource-item > span:last-child {
    max-width: 150px;
    justify-content: flex-end;
    flex-wrap: wrap;
  }

  .gateway-cluster .mobile-resource-item > span:last-child small {
    width: 100%;
    text-align: right;
  }

  .gateway-cluster .mobile-resource-item strong,
  .gateway-cluster .mobile-resource-item code {
    max-width: none;
    overflow: visible;
    text-overflow: clip;
    white-space: normal;
    overflow-wrap: anywhere;
  }

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

@media (min-width: 721px) and (max-width: 1050px) {
  .gateway-cluster__header {
    grid-template-columns: minmax(240px, 1fr) auto;
  }

  .gateway-cluster__facts {
    grid-column: 1 / -1;
    grid-row: 2;
  }
}
</style>
