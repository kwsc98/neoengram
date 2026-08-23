<script setup lang="ts">
import {
  Back,
  CircleCheck,
  Delete,
  FolderOpened,
  Lock,
  Plus,
  RefreshRight,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, ref, watch, watchEffect } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  createSnapshotDelivery,
  cancelCommitReplication,
  deleteSnapshotDelivery,
  queryCommitAvailability,
  queryCommitReplicationList,
  queryApiVersion,
  queryGatewayPoolList,
  querySnapshot,
  querySnapshotDeliveryList,
  queryStorageVolume,
  queryStorageVolumeList,
  replicateCommit,
  retryCommitReplication,
  retrySnapshotDelivery,
} from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  commitReplicationRequestId,
  findActiveCommitReplication,
  findCommitReplicationForTarget,
  isCommitReplicationActive,
} from '@/features/commit-replication';
import {
  supportsArtifactCommitReplication,
  supportsS3ReadonlyAccessPoint,
  supportsSnapshotDelivery,
  supportsSnapshotDeliveryMode,
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
const selectedDeliveryMode = ref<'fuse' | 'copy' | 'hardlink'>('fuse');
const targetVolumeId = ref('');
const replicationTargetVolumeId = ref('');
const replicationTargetTouched = ref(false);
const deliveryCapabilityEnabled = computed(() =>
  supportsSnapshotDelivery(versionQuery.data.value?.data.capabilities),
);
const replicationCapabilityEnabled = computed(
  () =>
    supportsArtifactCommitReplication(versionQuery.data.value?.data.capabilities) &&
    (tenants.byId(tenantId.value)?.permissions.includes('artifact.commit.replicate') ?? false),
);
const gatewayInventoryEnabled = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('gateway.read') ?? false,
);
const volumeQuery = useQuery({
  queryKey: computed(() => ['storage-volume', tenantId.value, targetVolumeId.value]),
  queryFn: () => queryStorageVolume(tenantId.value, targetVolumeId.value),
  enabled: computed(() => Boolean(targetVolumeId.value && deliveryCapabilityEnabled.value)),
  staleTime: 30_000,
});
const volumeListQuery = useQuery({
  queryKey: computed(() => ['storage-volumes', tenantId.value, 'snapshot-detail']),
  queryFn: () => queryStorageVolumeList({ tenant_id: tenantId.value, page_size: 100 }),
  enabled: computed(() =>
    Boolean(
      snapshot.value && (deliveryCapabilityEnabled.value || replicationCapabilityEnabled.value),
    ),
  ),
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
const replicationMutation = useMutation({ mutationFn: replicateCommit });
const retryReplicationMutation = useMutation({ mutationFn: retryCommitReplication });
const cancelReplicationMutation = useMutation({ mutationFn: cancelCommitReplication });
const replicationListQuery = useQuery({
  queryKey: computed(() => [
    'commit-replications',
    tenantId.value,
    snapshot.value?.commit_id,
    'snapshot-detail',
  ]),
  queryFn: () =>
    queryCommitReplicationList({
      tenant_id: tenantId.value,
      commit_id: snapshot.value!.commit_id,
    }),
  enabled: computed(() => replicationCapabilityEnabled.value && Boolean(snapshot.value?.commit_id)),
  refetchInterval: (query) => {
    const items = query.state.data?.data.replications ?? [];
    return items.some((item) => isCommitReplicationActive(item.state)) ? 1_000 : false;
  },
});
const commitReplications = computed(() => replicationListQuery.data.value?.data.replications ?? []);
const replication = computed(() =>
  findCommitReplicationForTarget(commitReplications.value, replicationTargetVolumeId.value),
);
const activeReplication = computed(() => findActiveCommitReplication(commitReplications.value));
watchEffect(() => {
  if (!targetVolumeId.value) {
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
const availabilityQuery = useQuery({
  queryKey: computed(() => ['commit-availability', tenantId.value, snapshot.value?.commit_id]),
  queryFn: () =>
    queryCommitAvailability({ tenant_id: tenantId.value, commit_id: snapshot.value!.commit_id }),
  enabled: computed(() => Boolean(snapshot.value?.commit_id)),
  refetchInterval: () =>
    commitReplications.value.some((item) => isCommitReplicationActive(item.state)) ? 1_000 : false,
});
watch(
  commitReplications,
  (next, previous) => {
    if (
      previous?.some((item) => isCommitReplicationActive(item.state)) &&
      !next.some((item) => isCommitReplicationActive(item.state))
    ) {
      void availabilityQuery.refetch();
    }
  },
  { deep: true },
);
const availableVolumeIds = computed(
  () => availabilityQuery.data.value?.data.availability.verified_storage_volume_ids ?? [],
);
const targetPlacementPublished = computed(
  () =>
    (replication.value?.state === 'published' &&
      replication.value.target_storage_volume_id === targetVolumeId.value) ||
    availableVolumeIds.value.includes(targetVolumeId.value),
);
const replicationActionLabel = computed(() => {
  if (replication.value?.state === 'published') return '副本已发布';
  if (replication.value && isCommitReplicationActive(replication.value.state)) return '复制进行中';
  if (replication.value?.state === 'failed' || replication.value?.state === 'cancelled') {
    return '请重试任务';
  }
  return '复制 Commit';
});
const replicationTargetBlocked = computed(
  () => !replicationTargetVolumeId.value || Boolean(replication.value),
);
const storageVolume = computed(() => volumeQuery.data.value?.data.storage_volume);
const deliveryModeAvailability = computed(() => {
  const capabilities = versionQuery.data.value?.data.capabilities;
  const volume = storageVolume.value;
  const layout = snapshot.value?.data_layout;
  return {
    fuse:
      targetPlacementPublished.value &&
      supportsSnapshotDeliveryMode(capabilities, 'fuse') &&
      Boolean(volume?.allowed_delivery_modes.includes('fuse')),
    copy:
      targetPlacementPublished.value &&
      supportsSnapshotDeliveryMode(capabilities, 'copy') &&
      Boolean(volume?.allowed_delivery_modes.includes('copy')),
    hardlink:
      targetPlacementPublished.value &&
      supportsSnapshotDeliveryMode(capabilities, 'hardlink') &&
      Boolean(volume?.allowed_delivery_modes.includes('hardlink')) &&
      layout === 'whole_file' &&
      volume?.hardlink_policy !== 'disabled',
  };
});
const deliveryQuery = useQuery({
  queryKey: computed(() => ['snapshot-deliveries', tenantId.value, snapshotId.value]),
  queryFn: () =>
    querySnapshotDeliveryList({
      tenant_id: tenantId.value,
      snapshot_id: snapshotId.value,
      page_size: 100,
    }),
  enabled: computed(() =>
    Boolean(
      snapshot.value?.state === 'ready' &&
      snapshot.value.data_health !== 'unavailable' &&
      deliveryCapabilityEnabled.value,
    ),
  ),
});
const deliveryMutation = useMutation({
  mutationFn: createSnapshotDelivery,
  onSuccess: async () => {
    await queryClient.invalidateQueries({
      queryKey: ['snapshot-deliveries', tenantId.value, snapshotId.value],
    });
    ElMessage.success('只读交付已创建');
  },
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
const deliveries = computed(() => deliveryQuery.data.value?.data.items ?? []);

const copyRequiredBytes = computed(() => {
  try {
    const size = BigInt(snapshot.value?.logical_size_bytes ?? '0');
    const reserve = BigInt(storageVolume.value?.copy_reserve_bytes ?? '0');
    return (size + reserve).toString();
  } catch {
    return undefined;
  }
});

const deliveryModeOptions = computed(() => [
  {
    label: 'FUSE',
    value: 'fuse',
    disabled: !deliveryModeAvailability.value.fuse,
  },
  {
    label: '全部复制',
    value: 'copy',
    disabled: !deliveryModeAvailability.value.copy,
  },
  {
    label: '硬链接',
    value: 'hardlink',
    disabled: !deliveryModeAvailability.value.hardlink,
  },
]);

watchEffect(() => {
  if (!storageVolume.value || deliveryModeAvailability.value[selectedDeliveryMode.value]) return;
  const firstAvailable = (['fuse', 'copy', 'hardlink'] as const).find(
    (mode) => deliveryModeAvailability.value[mode],
  );
  if (firstAvailable) selectedDeliveryMode.value = firstAvailable;
});

function deliveryModeReason(mode: 'fuse' | 'copy' | 'hardlink'): string | undefined {
  if (snapshot.value?.data_health === 'unavailable') return 'Snapshot 当前没有可用对象副本';
  if (!targetPlacementPublished.value)
    return '请先将 Commit 复制到当前目标 Volume，并等待 PlacementSet published';
  const capabilities = versionQuery.data.value?.data.capabilities;
  if (!supportsSnapshotDeliveryMode(capabilities, mode)) {
    return 'Central 未声明该交付能力';
  }
  const volume = storageVolume.value;
  if (!volume) return '正在读取 StorageVolume 策略';
  if (!volume.allowed_delivery_modes.includes(mode)) return 'StorageVolume 策略未允许该模式';
  if (mode === 'hardlink' && snapshot.value?.data_layout !== 'whole_file') {
    return '硬链接要求 WholeFile Commit，系统不会自动转换布局';
  }
  if (mode === 'hardlink' && volume.hardlink_policy === 'disabled') {
    return 'StorageVolume 未配置 sealed ACL 或 trusted-local 策略';
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

async function createDelivery(): Promise<void> {
  if (
    snapshot.value?.state !== 'ready' ||
    snapshot.value.data_health === 'unavailable' ||
    deliveryMutation.isPending.value ||
    !deliveryModeAvailability.value[selectedDeliveryMode.value]
  )
    return;
  const requestId = operationRequestId(
    `delivery-${snapshotId.value}-${selectedDeliveryMode.value}`,
  );
  await deliveryMutation.mutateAsync({
    tenant_id: tenantId.value,
    snapshot_id: snapshotId.value,
    target_storage_volume_id: targetVolumeId.value,
    mode: selectedDeliveryMode.value,
    request_id: requestId,
  });
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
  const result = await replicationMutation.mutateAsync({
    tenant_id: tenantId.value,
    project_id: snapshot.value.project_id,
    artifact_id: snapshot.value.artifact_id,
    commit_id: snapshot.value.commit_id,
    target_storage_volume_id: replicationTargetVolumeId.value,
    request_id: commitReplicationRequestId({
      tenantId: tenantId.value,
      projectId: snapshot.value.project_id,
      artifactId: snapshot.value.artifact_id,
      commitId: snapshot.value.commit_id,
      targetStorageVolumeId: replicationTargetVolumeId.value,
    }),
  });
  ElMessage.success(result.data.replayed ? '已返回同一复制请求' : 'Commit 复制已排队');
  await replicationListQuery.refetch();
}

async function retryReplication(): Promise<void> {
  const current = replication.value;
  if (!current || retryReplicationMutation.isPending.value) return;
  await retryReplicationMutation.mutateAsync({
    tenant_id: tenantId.value,
    replication_id: current.replication_id,
    expected_attempt: current.attempt,
  });
  ElMessage.success('复制任务已重新提交');
  await replicationListQuery.refetch();
}

async function cancelReplication(): Promise<void> {
  const current = replication.value;
  if (!current || cancelReplicationMutation.isPending.value) return;
  await cancelReplicationMutation.mutateAsync({
    tenant_id: tenantId.value,
    replication_id: current.replication_id,
    expected_attempt: current.attempt,
  });
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
  if (snapshot.value?.state !== 'ready') return;
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
          v-if="snapshot?.state === 'ready' && s3ReadonlyEnabled"
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
          <p v-else-if="snapshot.state === 'ready' && snapshot.data_health !== 'unavailable'">
            Snapshot 已固定，可按需创建独立只读交付。
          </p>
          <p v-else-if="snapshot.state === 'ready'">Snapshot 元数据仍在，但当前没有可用副本。</p>
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
              <h2>先复制 Commit，再创建交付</h2>
            </div>
            <RefreshRight />
          </header>
          <p class="replication-explanation">
            Snapshot 不绑定磁盘。选择目标 Volume 后显式复制完整 ObjectSet；目标 PlacementSet
            发布前不会对 Delivery 或 S3 可见。
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
              >任务 <code>{{ replication.replication_id }}</code></span
            >
            <el-tag
              :type="
                replication.state === 'published'
                  ? 'success'
                  : replication.state === 'failed'
                    ? 'danger'
                    : 'warning'
              "
              effect="plain"
              >{{ replication.state }}</el-tag
            >
            <span
              >{{ replication.completed_objects }} / {{ replication.total_objects }} objects</span
            >
            <span
              >{{ formatBytes(replication.completed_bytes) }} /
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
              v-else-if="isCommitReplicationActive(replication.state)"
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
        <div v-if="deliveryCapabilityEnabled" class="delivery-toolbar">
          <el-select
            v-model="targetVolumeId"
            placeholder="选择目标 StorageVolume"
            style="min-width: 240px"
          >
            <el-option
              v-for="volume in targetVolumes"
              :key="volume.storage_volume_id"
              :label="`${volume.display_name} (${volume.region})`"
              :value="volume.storage_volume_id"
              :disabled="volume.state !== 'ready'"
            />
          </el-select>
          <el-segmented v-model="selectedDeliveryMode" :options="deliveryModeOptions" />
          <el-button
            type="primary"
            :icon="Plus"
            :loading="deliveryMutation.isPending.value"
            :disabled="
              volumeQuery.isPending.value ||
              !targetVolumeId ||
              !deliveryModeAvailability[selectedDeliveryMode]
            "
            @click="createDelivery"
            >创建交付</el-button
          >
        </div>
        <div class="delivery-mode-status" aria-label="交付模式可用性">
          <div v-for="mode in ['fuse', 'copy', 'hardlink'] as const" :key="mode">
            <strong>{{ deliveryModeLabel(mode) }}</strong>
            <el-tag
              :type="deliveryModeAvailability[mode] ? 'success' : 'info'"
              size="small"
              effect="plain"
            >
              {{ deliveryModeAvailability[mode] ? '可用' : '不可用' }}
            </el-tag>
            <span v-if="deliveryModeReason(mode)">{{ deliveryModeReason(mode) }}</span>
          </div>
        </div>
        <el-alert
          v-if="deliveryModeReason(selectedDeliveryMode)"
          :title="`${deliveryModeLabel(selectedDeliveryMode)} 当前不可用`"
          :description="deliveryModeReason(selectedDeliveryMode)"
          type="warning"
          :closable="false"
        />
        <div v-else class="delivery-policy-summary">
          <span v-if="selectedDeliveryMode === 'copy'">
            预计需要
            {{ copyRequiredBytes === undefined ? '未知' : formatBytes(copyRequiredBytes) }}
            可用空间（含 {{ formatBytes(storageVolume?.copy_reserve_bytes ?? '0') }} 预留）。
          </span>
          <span v-else-if="selectedDeliveryMode === 'hardlink'">
            Volume 策略：{{ storageVolume?.hardlink_policy }}；创建时仍会校验文件系统、inode、BLAKE3
            与对象封存状态。
          </span>
          <span v-else>FUSE 可用性将在创建时由运行环境做最终校验。</span>
        </div>
        <ApiProblemAlert
          v-if="deliveryMutation.error.value"
          :error="deliveryMutation.error.value"
        />
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
