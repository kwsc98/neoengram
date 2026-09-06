<script setup lang="ts">
import {
  Back,
  CircleCheck,
  Delete,
  FolderOpened,
  Lock,
  RefreshRight,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, ref, watch, watchEffect } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  deleteSnapshotDelivery,
  queryApiVersion,
  queryGatewayPoolList,
  querySnapshot,
  querySnapshotDeliveryList,
  queryStorageVolume,
  queryStorageVolumeList,
  retrySnapshotDelivery,
} from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  isMaterializationActive,
  materializationRequestId,
  useCommitMaterialization,
} from '@/features/materialization';
import {
  supportsCommitMaterializationV2,
  supportsS3ReadonlyAccessPoint,
  supportsSnapshotDelivery,
} from '@/features/capabilities';
import {
  groupStorageVolumesByCluster,
  isReplicationTargetSelectable,
  selectableReplicationVolumes,
} from '@/features/storage/cluster-groups';
import {
  snapshotIntegrityLabel,
  snapshotIntegrityTagType,
  snapshotPollInterval,
  snapshotStateLabel,
  snapshotStateTagType,
} from '@/features/snapshots/status';
import { useTenantsStore } from '@/stores/tenants';
import { commitTagNames } from '@/utils/commit';
import { formatBytes, formatCount, formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const snapshotId = computed(() => String(route.params.snapshotId ?? ''));

const versionQuery = useQuery({
  queryKey: ['system', 'version'],
  queryFn: queryApiVersion,
  staleTime: Number.POSITIVE_INFINITY,
});
const s3ReadonlyEnabled = computed(() =>
  Boolean(
    supportsS3ReadonlyAccessPoint(versionQuery.data.value?.data.capabilities) &&
    tenants.byId(tenantId.value)?.permissions.includes('s3.access.read'),
  ),
);

const snapshotQuery = useQuery({
  queryKey: computed(() => ['snapshot', tenantId.value, snapshotId.value]),
  queryFn: async () => {
    const result = await querySnapshot(tenantId.value, snapshotId.value);
    const snapshot = result.data.snapshot;
    if (snapshot.project_id !== projectId.value || snapshot.artifact_id !== artifactId.value) {
      throw new Error('Snapshot 不属于当前 Artifact');
    }
    return result;
  },
  refetchInterval: (query) => snapshotPollInterval(query.state.data?.data.snapshot.state),
});
const snapshot = computed(() => snapshotQuery.data.value?.data.snapshot);
const tags = computed(() => commitTagNames(snapshot.value?.tag_names ?? []));
const targetVolumeId = ref('');
const replicationTargetVolumeId = ref('');
const replicationTargetTouched = ref(false);
const deliveryCapabilityEnabled = computed(() =>
  supportsSnapshotDelivery(versionQuery.data.value?.data.capabilities),
);
const replicationCapabilityEnabled = computed(
  () =>
    supportsCommitMaterializationV2(versionQuery.data.value?.data.capabilities) &&
    (tenants.byId(tenantId.value)?.permissions.includes('artifact.commit.replicate') ?? false),
);
const gatewayInventoryEnabled = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('gateway.read') ?? false,
);
const volumeQuery = useQuery({
  queryKey: computed(() => [
    'storage-volume',
    tenantId.value,
    targetVolumeId.value,
    snapshot.value?.snapshot_id,
  ]),
  queryFn: () =>
    queryStorageVolume(tenantId.value, targetVolumeId.value, snapshot.value?.snapshot_id),
  enabled: computed(() => Boolean(targetVolumeId.value && deliveryCapabilityEnabled.value)),
  staleTime: 30_000,
  // This state is live Agent availability, not a durable lifecycle field.
  refetchInterval: 5_000,
  refetchIntervalInBackground: false,
});
const volumeListQuery = useQuery({
  queryKey: computed(() => ['storage-volumes', tenantId.value, 'snapshot-detail']),
  queryFn: () => queryStorageVolumeList({ tenant_id: tenantId.value, page_size: 100 }),
  // Snapshot readers obtain only their immutable target through the scoped single-volume query.
  // The inventory list is reserved for the explicit materialization/replication workflow.
  enabled: computed(() => Boolean(snapshot.value && replicationCapabilityEnabled.value)),
  refetchInterval: 5_000,
  refetchIntervalInBackground: false,
});
const targetVolumes = computed(() => volumeListQuery.data.value?.data.items ?? []);
const gatewayPoolQuery = useQuery({
  queryKey: computed(() => ['gateway-pools', tenantId.value, 'snapshot-commit-replication']),
  queryFn: () => queryGatewayPoolList({}),
  enabled: computed(
    () =>
      Boolean(snapshot.value && replicationCapabilityEnabled.value) &&
      gatewayInventoryEnabled.value,
  ),
  staleTime: 15_000,
  refetchInterval: 5_000,
  refetchIntervalInBackground: false,
});
const storageClusters = computed(() =>
  groupStorageVolumesByCluster(targetVolumes.value, gatewayPoolQuery.data.value?.data.items ?? [], {
    gatewayInventoryAvailable: gatewayPoolQuery.isSuccess.value,
  }),
);
const replicationTargetVolumes = computed(() =>
  selectableReplicationVolumes(storageClusters.value),
);
const selectedReplicationTargetGroup = computed(() =>
  storageClusters.value.find((group) =>
    group.volumes.some((volume) => volume.storage_volume_id === replicationTargetVolumeId.value),
  ),
);
const commitMaterialization = useCommitMaterialization(
  {
    tenantId,
    projectId,
    artifactId,
    commitId: computed(() => snapshot.value?.commit_id ?? ''),
  },
  {
    enabled: computed(
      () => replicationCapabilityEnabled.value && Boolean(snapshot.value?.commit_id),
    ),
  },
);
const {
  materializations: commitReplications,
  materializationsQuery: replicationListQuery,
  availabilityQuery: availabilityQuery,
  materializeOrRepair,
  repairMaterialization,
  cancelMaterialization,
  createMutation: replicationMutation,
  retryMutation: retryReplicationMutation,
  cancelMutation: cancelReplicationMutation,
} = commitMaterialization;
const replication = computed(() =>
  commitMaterialization.targetMaterialization(replicationTargetVolumeId.value),
);
const activeReplication = computed(() =>
  commitReplications.value.find((item) => isMaterializationActive(item.state)),
);
watchEffect(() => {
  if (snapshot.value?.storage_volume_id) {
    targetVolumeId.value = snapshot.value.storage_volume_id;
  } else if (!targetVolumeId.value) {
    targetVolumeId.value =
      targetVolumes.value.find((volume) => volume.state === 'ready')?.storage_volume_id ?? '';
  }
  const targetIsSelectable = replicationTargetVolumes.value.some(
    (volume) => volume.storage_volume_id === replicationTargetVolumeId.value,
  );
  const activeTargetIsSelectable = replicationTargetVolumes.value.some(
    (volume) => volume.storage_volume_id === activeReplication.value?.target_storage_volume_id,
  );
  if (!replicationTargetTouched.value && activeReplication.value && activeTargetIsSelectable) {
    replicationTargetVolumeId.value = activeReplication.value.target_storage_volume_id;
  } else if (!targetIsSelectable) {
    replicationTargetVolumeId.value = replicationTargetVolumes.value[0]?.storage_volume_id ?? '';
  }
});
watch(
  commitReplications,
  (next, previous) => {
    if (
      previous?.some((item) => isMaterializationActive(item.state)) &&
      !next.some((item) => isMaterializationActive(item.state))
    ) {
      void availabilityQuery.refetch();
    }
  },
  { deep: true },
);
const replicationActionLabel = computed(() => {
  if (replication.value?.state === 'complete') return '副本已发布';
  if (replication.value && isMaterializationActive(replication.value.state)) return '复制进行中';
  if (replication.value?.state === 'failed' || replication.value?.state === 'cancelled') {
    return '请重试任务';
  }
  return '复制 Commit';
});
const replicationTargetBlocked = computed(
  () => !replicationTargetVolumeId.value || Boolean(replication.value),
);
const deliveryQuery = useQuery({
  queryKey: computed(() => ['snapshot-deliveries', tenantId.value, snapshotId.value]),
  queryFn: () =>
    querySnapshotDeliveryList({
      tenant_id: tenantId.value,
      snapshot_id: snapshotId.value,
      page_size: 100,
    }),
  enabled: computed(() => Boolean(snapshot.value && deliveryCapabilityEnabled.value)),
});
const deliveries = computed(() => deliveryQuery.data.value?.data.items ?? []);
const boundDelivery = computed(() =>
  deliveries.value.find((delivery) => delivery.delivery_id === snapshot.value?.delivery_id),
);
const targetVolumeReady = computed(
  () => volumeQuery.data.value?.data.storage_volume.state === 'ready',
);
const snapshotReadable = computed(
  () =>
    snapshot.value?.state === 'ready' &&
    boundDelivery.value?.state === 'ready' &&
    targetVolumeReady.value,
);
const deliveryModeAvailability = computed(() => {
  const mode = snapshot.value?.delivery_mode;
  const ready = boundDelivery.value?.state === 'ready';
  return {
    fuse: mode === 'fuse' && ready,
    copy: mode === 'copy' && ready,
    hardlink: mode === 'hardlink' && ready,
  };
});
const deleteDeliveryMutation = useMutation({
  mutationFn: deleteSnapshotDelivery,
  onSuccess: async () => {
    await queryClient.invalidateQueries({
      queryKey: ['snapshot-deliveries', tenantId.value, snapshotId.value],
    });
    ElMessage.success('只读交付已删除');
  },
});
const retryDeliveryMutation = useMutation({
  mutationFn: retrySnapshotDelivery,
  onSuccess: async () => {
    await queryClient.invalidateQueries({
      queryKey: ['snapshot-deliveries', tenantId.value, snapshotId.value],
    });
    ElMessage.success('只读交付已重新提交');
  },
});
function deliveryModeReason(mode: 'fuse' | 'copy' | 'hardlink'): string | undefined {
  if (snapshot.value?.delivery_mode !== mode) return 'Snapshot 创建时已固定其他交付模式';
  if (!boundDelivery.value) return '唯一 SnapshotDelivery 尚未创建';
  if (boundDelivery.value.state !== 'ready') {
    return `唯一 Delivery 当前为 ${deliveryStateLabel(boundDelivery.value.state)}`;
  }
  return undefined;
}

function deliveryModeLabel(mode: 'fuse' | 'copy' | 'hardlink'): string {
  return { fuse: 'FUSE', copy: '全部复制', hardlink: '硬链接' }[mode];
}

function operationRequestId(prefix: string): string {
  const random = globalThis.crypto?.randomUUID?.();
  return `${prefix}-${random ?? `${Date.now()}-${Math.random().toString(16).slice(2)}`}`;
}

function deliveryStateLabel(state: string): string {
  return (
    {
      requested: '等待调度',
      validating: '校验中',
      materializing: '物化中',
      ready: '就绪',
      failed: '失败',
      deleting: '删除中',
      deleted: '已删除',
    }[state] ?? state
  );
}

async function replicateSnapshot(): Promise<void> {
  if (
    !snapshot.value ||
    snapshot.value.state !== 'ready' ||
    !replicationTargetVolumeId.value ||
    replicationMutation.isPending.value ||
    replicationTargetBlocked.value
  ) {
    return;
  }
  const result = await materializeOrRepair(replicationTargetVolumeId.value, {
    requestId: materializationRequestId({
      tenantId: tenantId.value,
      projectId: snapshot.value.project_id,
      artifactId: snapshot.value.artifact_id,
      commitId: snapshot.value.commit_id,
      targetStorageVolumeId: replicationTargetVolumeId.value,
    }),
  });
  if (result.mode === 'noop') {
    ElMessage.info('目标副本已经完整，无需复制');
  } else if (result.mode === 'in_flight') {
    ElMessage.info('该目标已有物化任务在执行');
  } else {
    ElMessage.success(
      result.result && (result.result.data.request_replayed || result.result.data.execution_reused)
        ? '已返回同一复制任务'
        : 'Commit 复制已排队',
    );
  }
  await replicationListQuery.refetch();
}

async function retryReplication(): Promise<void> {
  const current = replication.value;
  if (!current || retryReplicationMutation.isPending.value) return;
  await repairMaterialization(current);
  ElMessage.success('复制任务已重新提交');
  await replicationListQuery.refetch();
}

async function cancelReplication(): Promise<void> {
  const current = replication.value;
  if (!current || cancelReplicationMutation.isPending.value) return;
  await cancelMaterialization(current);
  ElMessage.success('复制任务已取消');
  await replicationListQuery.refetch();
}

function changeReplicationTarget(): void {
  replicationTargetTouched.value = true;
  replicationMutation.reset();
}

function replicationRouteLabel(state: 'ready' | 'unavailable' | 'unknown'): string {
  return {
    ready: '路由 Ready',
    unavailable: '路由不可用',
    unknown: '路由由 Central 校验',
  }[state];
}

async function retryDelivery(deliveryId: string): Promise<void> {
  if (retryDeliveryMutation.isPending.value) return;
  await retryDeliveryMutation.mutateAsync({
    tenant_id: tenantId.value,
    delivery_id: deliveryId,
    request_id: operationRequestId(`retry-${deliveryId}`),
  });
}

async function deleteDelivery(deliveryId: string): Promise<void> {
  try {
    await ElMessageBox.confirm('删除后只读视图将立即不可用。确认继续？', '删除只读交付', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消',
    });
    await deleteDeliveryMutation.mutateAsync({
      tenant_id: tenantId.value,
      delivery_id: deliveryId,
      request_id: operationRequestId(`delete-${deliveryId}`),
    });
  } catch {
    // User cancellation is intentionally silent.
  }
}

async function backToArtifact(): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: projectId.value, artifactId: artifactId.value },
    query: { tab: 'snapshots' },
  });
}

async function openObjectStorage(): Promise<void> {
  if (!snapshotReadable.value) return;
  await router.push({
    name: 'object-storage-list',
    params: { tenantId: tenantId.value },
    query: {
      snapshotId: snapshotId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
    },
  });
}
</script>

<template>
  <div class="page snapshot-detail-page">
    <PageHeading
      :title="snapshot?.message ?? snapshotId"
      :description="`${projectId} / ${artifactId}`"
    >
      <template #actions>
        <el-button :icon="Back" @click="backToArtifact">返回 Artifact</el-button>
        <el-button
          v-if="snapshotReadable && s3ReadonlyEnabled"
          :icon="FolderOpened"
          @click="openObjectStorage"
          >对象存储</el-button
        >
        <el-button
          :icon="RefreshRight"
          :loading="snapshotQuery.isFetching.value"
          @click="snapshotQuery.refetch"
          >刷新</el-button
        >
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="snapshotQuery.error.value"
      :error="snapshotQuery.error.value"
      :retrying="snapshotQuery.isFetching.value"
      @retry="snapshotQuery.refetch"
    />
    <el-skeleton v-if="snapshotQuery.isPending.value" :rows="8" animated />

    <template v-else-if="snapshot">
      <section class="snapshot-state-band">
        <span :class="['snapshot-state-icon', `snapshot-state-icon--${snapshot.state}`]">
          <CircleCheck v-if="snapshot.state === 'ready'" />
          <WarningFilled v-else-if="snapshot.state === 'abnormal'" />
          <RefreshRight v-else />
        </span>
        <div>
          <small>{{ snapshotStateLabel(snapshot.state) }}</small>
          <h2>只读 Snapshot</h2>
          <p v-if="snapshot.state === 'creating'">正在冻结不可变 Commit。</p>
          <p v-else-if="snapshot.state === 'ready' && snapshotReadable">
            Snapshot 与唯一 Delivery 均已就绪，可浏览对象存储。
          </p>
          <p v-else-if="snapshot.state === 'ready'">
            Snapshot 已固定，但绑定的唯一 Delivery 尚未就绪。
          </p>
          <p v-else>Snapshot 当前不可用于创建只读交付。</p>
        </div>
        <el-tag :type="snapshotStateTagType(snapshot.state)" effect="plain">
          {{ snapshotStateLabel(snapshot.state) }}
        </el-tag>
      </section>

      <el-alert
        v-if="snapshot.issue"
        :title="snapshot.issue.message"
        :description="snapshot.issue.code"
        type="error"
        :closable="false"
      />

      <section class="content-section snapshot-detail-section">
        <header class="section-heading">
          <div>
            <span>IMMUTABLE SNAPSHOT</span>
            <h2>固定数据版本</h2>
          </div>
          <Lock />
        </header>
        <dl class="snapshot-facts">
          <div>
            <dt>Snapshot ID</dt>
            <dd>
              <code>{{ snapshot.snapshot_id }}</code>
            </dd>
          </div>
          <div>
            <dt>访问模式</dt>
            <dd><el-tag type="success" effect="plain">只读</el-tag></dd>
          </div>
          <div class="snapshot-facts__wide">
            <dt>Artifact Commit</dt>
            <dd>
              <code>{{ snapshot.commit_id }}</code>
            </dd>
          </div>
          <div>
            <dt>目标 EdgeCluster</dt>
            <dd>
              <code>{{ snapshot.edge_cluster_id }}</code>
            </dd>
          </div>
          <div>
            <dt>目标 StorageVolume</dt>
            <dd>
              <code>{{ snapshot.storage_volume_id }}</code>
            </dd>
          </div>
          <div>
            <dt>固定交付模式</dt>
            <dd>{{ deliveryModeLabel(snapshot.delivery_mode) }}</dd>
          </div>
          <div>
            <dt>唯一 Delivery</dt>
            <dd>
              <code>{{ snapshot.delivery_id }}</code>
            </dd>
          </div>
          <div>
            <dt>完整性</dt>
            <dd>
              <el-tag :type="snapshotIntegrityTagType(snapshot.integrity.state)" effect="plain">{{
                snapshotIntegrityLabel(snapshot.integrity.state)
              }}</el-tag>
            </dd>
          </div>
          <div>
            <dt>数据健康</dt>
            <dd>
              <el-tag
                :type="
                  snapshot.data_health === 'available'
                    ? 'success'
                    : snapshot.data_health === 'degraded'
                      ? 'warning'
                      : 'danger'
                "
                effect="plain"
              >
                {{
                  snapshot.data_health === 'available'
                    ? '可用'
                    : snapshot.data_health === 'degraded'
                      ? '降级'
                      : '不可用'
                }}
              </el-tag>
            </dd>
          </div>
          <div>
            <dt>文件</dt>
            <dd>{{ formatCount(snapshot.logical_file_count) }}</dd>
          </div>
          <div>
            <dt>逻辑大小</dt>
            <dd>{{ formatBytes(snapshot.logical_size_bytes) }}</dd>
          </div>
          <div>
            <dt>已校验</dt>
            <dd>{{ formatBytes(snapshot.integrity.bytes_verified) }}</dd>
          </div>
          <div>
            <dt>创建时间</dt>
            <dd>{{ formatTime(snapshot.created_at_unix_ms) }}</dd>
          </div>
          <div>
            <dt>更新时间</dt>
            <dd>{{ formatTime(snapshot.updated_at_unix_ms) }}</dd>
          </div>
          <div class="snapshot-facts__wide">
            <dt>Commit Tags</dt>
            <dd class="tag-list">
              <el-tag v-for="tag in tags" :key="tag" size="small" effect="plain">{{ tag }}</el-tag>
              <span v-if="tags.length === 0">暂无 Tag</span>
            </dd>
          </div>
          <div>
            <dt>Commit 数据布局</dt>
            <dd>
              <el-tag effect="plain">{{
                snapshot.data_layout === 'whole_file' ? 'WholeFile' : 'FastCDC'
              }}</el-tag>
            </dd>
          </div>
        </dl>
      </section>

      <section
        v-if="deliveryCapabilityEnabled || replicationCapabilityEnabled"
        class="content-section delivery-section"
      >
        <section
          v-if="replicationCapabilityEnabled"
          class="replication-panel"
          aria-label="Commit 复制"
        >
          <header class="section-heading">
            <div>
              <span>COMMIT PLACEMENT</span>
              <h2>补齐 Commit 对象副本</h2>
            </div>
            <RefreshRight />
          </header>
          <p class="replication-explanation">
            Snapshot 创建时已经固定目标 Volume 和唯一 Delivery；此处可为该 Commit 补齐其他 Volume
            的对象 Placement，但不会改变 Snapshot 的交付目标。
          </p>
          <ApiProblemAlert
            v-if="volumeListQuery.error.value"
            :error="volumeListQuery.error.value"
            :retrying="volumeListQuery.isFetching.value"
            @retry="volumeListQuery.refetch"
          />
          <ApiProblemAlert
            v-if="gatewayPoolQuery.error.value"
            :error="gatewayPoolQuery.error.value"
            :retrying="gatewayPoolQuery.isFetching.value"
            @retry="gatewayPoolQuery.refetch"
          />
          <div class="delivery-toolbar">
            <el-select
              v-model="replicationTargetVolumeId"
              placeholder="选择 Gateway 集群 / StorageVolume"
              :loading="
                volumeListQuery.isPending.value ||
                (gatewayInventoryEnabled && gatewayPoolQuery.isPending.value)
              "
              :disabled="replicationTargetVolumes.length === 0"
              style="min-width: min(100%, 360px)"
              @change="changeReplicationTarget"
            >
              <el-option-group
                v-for="group in storageClusters"
                :key="group.edgeClusterId"
                :label="`${group.gatewayPool?.display_name ?? group.edgeClusterId} · ${group.edgeClusterId} · ${replicationRouteLabel(group.routeState)}`"
              >
                <el-option
                  v-for="volume in group.volumes"
                  :key="volume.storage_volume_id"
                  :label="`${volume.display_name} · ${volume.region}`"
                  :value="volume.storage_volume_id"
                  :disabled="!isReplicationTargetSelectable(group, volume)"
                />
              </el-option-group>
            </el-select>
            <el-button
              type="primary"
              :loading="replicationMutation.isPending.value"
              :disabled="replicationTargetBlocked || snapshot.state !== 'ready'"
              @click="replicateSnapshot"
            >
              {{ replicationActionLabel }}
            </el-button>
          </div>
          <div v-if="selectedReplicationTargetGroup" class="tag-list">
            <el-tag effect="plain">
              Gateway 集群：{{
                selectedReplicationTargetGroup.gatewayPool?.display_name ??
                selectedReplicationTargetGroup.edgeClusterId
              }}
            </el-tag>
            <el-tag effect="plain">
              Region：{{ selectedReplicationTargetGroup.regions.join(', ') }}
            </el-tag>
            <el-tag
              :type="
                selectedReplicationTargetGroup.routeState === 'ready'
                  ? 'success'
                  : selectedReplicationTargetGroup.routeState === 'unavailable'
                    ? 'danger'
                    : 'info'
              "
              effect="plain"
            >
              {{ replicationRouteLabel(selectedReplicationTargetGroup.routeState) }}
            </el-tag>
          </div>
          <div v-else-if="storageClusters.length" class="tag-list" aria-label="Gateway 集群路由">
            <el-tag
              v-for="group in storageClusters"
              :key="group.edgeClusterId"
              :type="group.routeState === 'unavailable' ? 'danger' : 'info'"
              effect="plain"
            >
              {{ group.gatewayPool?.display_name ?? group.edgeClusterId }} ·
              {{ group.regions.join(', ') }} · {{ replicationRouteLabel(group.routeState) }}
            </el-tag>
          </div>
          <ApiProblemAlert
            v-if="replicationMutation.error.value"
            :error="replicationMutation.error.value"
          />
          <ApiProblemAlert
            v-if="replicationListQuery.error.value"
            :error="replicationListQuery.error.value"
            :retrying="replicationListQuery.isFetching.value"
            @retry="replicationListQuery.refetch"
          />
          <ApiProblemAlert
            v-if="availabilityQuery.error.value"
            :error="availabilityQuery.error.value"
            :retrying="availabilityQuery.isFetching.value"
            @retry="availabilityQuery.refetch"
          />
          <ApiProblemAlert
            v-if="retryReplicationMutation.error.value"
            :error="retryReplicationMutation.error.value"
          />
          <ApiProblemAlert
            v-if="cancelReplicationMutation.error.value"
            :error="cancelReplicationMutation.error.value"
          />
          <div v-if="replication" class="replication-status">
            <span
              >任务 <code>{{ replication.materialization_id }}</code></span
            >
            <el-tag
              :type="
                replication.state === 'complete'
                  ? 'success'
                  : replication.state === 'failed'
                    ? 'danger'
                    : 'warning'
              "
              effect="plain"
              >{{ replication.state }}</el-tag
            >
            <span
              >{{ replication.verified_objects }} / {{ replication.total_objects }} objects</span
            >
            <span
              >{{ formatBytes(replication.verified_bytes) }} /
              {{ formatBytes(replication.total_bytes) }}</span
            >
            <span v-if="replication.issue">{{ replication.issue.message }}</span>
            <el-button
              v-if="['failed', 'cancelled'].includes(replication.state)"
              size="small"
              :loading="retryReplicationMutation.isPending.value"
              @click="retryReplication"
              >重试</el-button
            >
            <el-button
              v-else-if="isMaterializationActive(replication.state)"
              size="small"
              type="danger"
              plain
              :loading="cancelReplicationMutation.isPending.value"
              @click="cancelReplication"
              >取消</el-button
            >
          </div>
        </section>
        <header v-if="deliveryCapabilityEnabled" class="section-heading delivery-section__header">
          <div>
            <span>SNAPSHOT DELIVERY</span>
            <h2>只读交付</h2>
          </div>
          <Lock />
        </header>
        <ApiProblemAlert
          v-if="deliveryCapabilityEnabled && volumeQuery.error.value"
          :error="volumeQuery.error.value"
          :retrying="volumeQuery.isFetching.value"
          @retry="volumeQuery.refetch"
        />
        <div v-if="deliveryCapabilityEnabled" class="delivery-binding-summary">
          <span
            >目标 EdgeCluster：<code>{{ snapshot.edge_cluster_id }}</code></span
          >
          <span
            >目标 StorageVolume：<code>{{ snapshot.storage_volume_id }}</code></span
          >
          <span>固定模式：{{ deliveryModeLabel(snapshot.delivery_mode) }}</span>
          <span v-if="boundDelivery"
            >唯一 Delivery：<code>{{ boundDelivery.delivery_id }}</code></span
          >
        </div>
        <el-alert
          v-if="deliveryCapabilityEnabled && !snapshotReadable"
          title="对象存储尚未就绪"
          description="只有 Snapshot 与其唯一 SnapshotDelivery 均为 Ready 时才可浏览或启用 S3。"
          type="warning"
          :closable="false"
        />
        <div class="delivery-mode-status" aria-label="交付模式状态">
          <div v-for="mode in ['fuse', 'copy', 'hardlink'] as const" :key="mode">
            <strong>{{ deliveryModeLabel(mode) }}</strong>
            <el-tag
              :type="deliveryModeAvailability[mode] ? 'success' : 'info'"
              size="small"
              effect="plain"
            >
              {{ deliveryModeAvailability[mode] ? '就绪' : '未就绪' }}
            </el-tag>
            <span v-if="deliveryModeReason(mode)">{{ deliveryModeReason(mode) }}</span>
          </div>
        </div>
        <ApiProblemAlert
          v-if="retryDeliveryMutation.error.value"
          :error="retryDeliveryMutation.error.value"
        />
        <ApiProblemAlert
          v-if="deleteDeliveryMutation.error.value"
          :error="deleteDeliveryMutation.error.value"
        />
        <ApiProblemAlert
          v-if="deliveryQuery.error.value"
          :error="deliveryQuery.error.value"
          :retrying="deliveryQuery.isFetching.value"
          @retry="deliveryQuery.refetch"
        />
        <el-skeleton v-if="deliveryQuery.isPending.value" :rows="3" animated />
        <el-empty v-else-if="deliveries.length === 0" description="尚未创建只读交付" />
        <el-table v-else :data="deliveries" size="small" row-key="delivery_id">
          <el-table-column label="模式" min-width="120">
            <template #default="scope">{{ deliveryModeLabel(scope.row.mode) }}</template>
          </el-table-column>
          <el-table-column label="状态" min-width="110">
            <template #default="scope">{{ deliveryStateLabel(scope.row.state) }}</template>
          </el-table-column>
          <el-table-column prop="target_relative_root" label="目标目录" min-width="260" />
          <el-table-column prop="file_count" label="文件" width="90" />
          <el-table-column label="大小" width="110">
            <template #default="scope">{{ formatBytes(scope.row.size_bytes) }}</template>
          </el-table-column>
          <el-table-column label="状态详情" min-width="200">
            <template #default="scope">
              <span v-if="scope.row.issue" class="delivery-issue">
                {{ scope.row.issue.message }}
                <code>{{ scope.row.issue.code }}</code>
              </span>
              <span v-else>--</span>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="150" fixed="right">
            <template #default="scope">
              <el-button
                v-if="scope.row.state === 'failed' && scope.row.issue?.retryable"
                text
                type="primary"
                :icon="RefreshRight"
                :loading="retryDeliveryMutation.isPending.value"
                @click="retryDelivery(scope.row.delivery_id)"
                >重试</el-button
              >
              <el-button
                text
                type="danger"
                :icon="Delete"
                title="删除只读交付"
                :disabled="scope.row.state === 'deleted' || deleteDeliveryMutation.isPending.value"
                @click="deleteDelivery(scope.row.delivery_id)"
              />
            </template>
          </el-table-column>
        </el-table>
      </section>
    </template>
  </div>
</template>

<style scoped>
.snapshot-detail-page {
  max-width: 1120px;
}

.replication-panel {
  margin-bottom: 18px;
  padding: 16px;
  border: 1px solid var(--border);
  background: #fff;
}

.replication-explanation {
  margin: 12px 0;
  color: var(--muted);
  line-height: 1.6;
}

.replication-status {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  margin-top: 12px;
  color: var(--muted);
}

.snapshot-state-band {
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  padding: 18px;
  border: 1px solid var(--border);
  background: #fff;
}

.delivery-section {
  margin-top: 18px;
}

.delivery-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 14px;
}

.delivery-policy-summary {
  min-height: 20px;
  margin: 10px 0 14px;
  color: var(--muted);
  font-size: 12px;
}

.delivery-mode-status {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 1px;
  margin-bottom: 12px;
  background: var(--border);
  border: 1px solid var(--border);
}

.delivery-mode-status > div {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 6px;
  min-width: 0;
  padding: 10px;
  background: #fff;
}

.delivery-mode-status span {
  grid-column: 1 / -1;
  min-height: 17px;
  color: var(--muted);
  font-size: 11px;
  overflow-wrap: anywhere;
}

.delivery-issue {
  display: inline-flex;
  flex-direction: column;
  gap: 2px;
  color: #b5473c;
  overflow-wrap: anywhere;
}

.delivery-issue code {
  color: inherit;
  font-size: 10px;
}

@media (max-width: 700px) {
  .delivery-toolbar {
    align-items: stretch;
    flex-direction: column;
  }

  .delivery-mode-status {
    grid-template-columns: 1fr;
  }
}

.snapshot-state-band h2 {
  margin: 2px 0 4px;
  font-size: 22px;
}
.snapshot-state-band p {
  margin: 0;
  color: var(--muted);
}
.snapshot-state-icon {
  display: grid;
  width: 54px;
  height: 54px;
  place-items: center;
  background: #eef3f1;
  color: #7a8581;
}
.snapshot-state-icon svg {
  width: 28px;
}
.snapshot-state-icon--ready {
  background: #eaf6f0;
  color: #167450;
}
.snapshot-state-icon--abnormal {
  background: #fff0ef;
  color: #c33f35;
}
.snapshot-state-icon--creating svg {
  animation: spin 1.2s linear infinite;
}

.snapshot-detail-section {
  margin-top: 16px;
}
.section-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.section-heading span {
  color: var(--muted);
  font-size: 11px;
}
.section-heading h2 {
  margin: 3px 0 0;
  font-size: 18px;
}
.section-heading svg {
  width: 24px;
  color: #167450;
}

.snapshot-facts {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1px;
  padding: 1px;
  background: var(--border);
}
.snapshot-facts > div {
  min-width: 0;
  padding: 14px;
  background: #fff;
}
.snapshot-facts__wide {
  grid-column: 1 / -1;
}
.snapshot-facts dt {
  margin-bottom: 6px;
  color: var(--muted);
  font-size: 11px;
}
.snapshot-facts dd {
  min-width: 0;
  margin: 0;
  overflow-wrap: anywhere;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 640px) {
  .snapshot-state-band {
    grid-template-columns: 48px minmax(0, 1fr);
  }
  .snapshot-state-band > .el-tag {
    grid-column: 1 / -1;
    justify-self: start;
  }
  .snapshot-state-icon {
    width: 44px;
    height: 44px;
  }
  .snapshot-facts {
    grid-template-columns: 1fr;
  }
  .snapshot-facts__wide {
    grid-column: auto;
  }
}
</style>
