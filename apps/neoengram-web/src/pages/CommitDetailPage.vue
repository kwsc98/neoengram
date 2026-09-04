<script setup lang="ts">
import { ArrowRight, Back, CircleCheck, DocumentCopy, RefreshRight } from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  queryApiVersion,
  queryArtifact,
  queryArtifactCommitDiff,
  queryArtifactCommitGraph,
  queryGatewayPoolList,
  queryStorageVolumeList,
} from '@/api/operations';
import type { CommitNode, MaterializationView, VolumeCommitCoverageView } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  isMaterializationActive,
  materializationRequestId,
  useCommitMaterialization,
} from '@/features/materialization';
import {
  groupStorageVolumesByCluster,
  isReplicationTargetSelectable,
  selectableReplicationVolumes,
} from '@/features/storage/cluster-groups';
import {
  supportsArtifactCommitDiff,
  supportsArtifactCommitGraph,
  supportsCommitMaterializationV2,
} from '@/features/capabilities';
import { useTenantsStore } from '@/stores/tenants';
import { commitDataLayoutLabel, commitTagNames } from '@/utils/commit';
import { formatBytes, formatCount, formatTime, shortId } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenants = useTenantsStore();

const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const commitId = computed(() => String(route.params.commitId ?? ''));

const versionQuery = useQuery({
  queryKey: ['system', 'version'],
  queryFn: queryApiVersion,
  staleTime: Number.POSITIVE_INFINITY,
});
const capabilities = computed(() => versionQuery.data.value?.data.capabilities);
const materializationEnabled = computed(() => supportsCommitMaterializationV2(capabilities.value));
const commitGraphEnabled = computed(() => supportsArtifactCommitGraph(capabilities.value));
const commitDiffEnabled = computed(() => supportsArtifactCommitDiff(capabilities.value));
const canReplicate = computed(
  () =>
    materializationEnabled.value &&
    (tenants.byId(tenantId.value)?.permissions.includes('artifact.commit.replicate') ?? false),
);

const artifactQuery = useQuery({
  queryKey: computed(() => ['artifact', tenantId.value, projectId.value, artifactId.value]),
  queryFn: () => queryArtifact(tenantId.value, projectId.value, artifactId.value),
  enabled: computed(() => Boolean(tenantId.value && projectId.value && artifactId.value)),
});
const artifact = computed(() => artifactQuery.data.value?.data.artifact);

const commitGraphQuery = useQuery({
  queryKey: computed(() => [
    'artifact-commits',
    tenantId.value,
    projectId.value,
    artifactId.value,
    'commit-detail',
  ]),
  queryFn: () => queryArtifactCommitGraph(tenantId.value, projectId.value, artifactId.value),
  enabled: computed(
    () => Boolean(commitId.value) && (commitGraphEnabled.value || !versionQuery.data.value),
  ),
});
const commitDiffQuery = useQuery({
  queryKey: computed(() => [
    'artifact-commit-diff',
    tenantId.value,
    projectId.value,
    artifactId.value,
    commitId.value,
    'commit-detail',
  ]),
  queryFn: () =>
    queryArtifactCommitDiff(tenantId.value, projectId.value, artifactId.value, commitId.value),
  enabled: computed(() => commitDiffEnabled.value && Boolean(commitId.value)),
});
const commit = computed<CommitNode | undefined>(() => {
  const target = commitDiffQuery.data.value?.data.diff.target_commit;
  return (
    target ??
    commitGraphQuery.data.value?.data.graph.nodes.find((node) => node.commit_id === commitId.value)
  );
});
const commitDiff = computed(() => commitDiffQuery.data.value?.data.diff);

const {
  coverageQuery,
  availabilityQuery,
  materializationsQuery: materializationQuery,
  coverage,
  availability,
  materializations,
  materializeOrRepair,
  hasCompleteCoverage,
  refresh: refreshMaterialization,
  checkReplica: refreshReplica,
  createMutation: materializeMutation,
  retryMutation,
  actionError,
  isBusy: materializationBusy,
} = useCommitMaterialization(
  { tenantId, projectId, artifactId, commitId },
  { enabled: canReplicate },
);

const storageVolumeQuery = useQuery({
  queryKey: computed(() => ['storage-volumes', tenantId.value, 'commit-detail']),
  queryFn: () => queryStorageVolumeList({ tenant_id: tenantId.value, page_size: 100 }),
  enabled: computed(() => canReplicate.value),
  staleTime: 15_000,
  refetchInterval: 5_000,
  refetchIntervalInBackground: false,
});
const gatewayPoolQuery = useQuery({
  queryKey: computed(() => ['gateway-pools', tenantId.value, 'commit-detail']),
  queryFn: () => queryGatewayPoolList({}),
  enabled: computed(
    () =>
      canReplicate.value &&
      (tenants.byId(tenantId.value)?.permissions.includes('gateway.read') ?? false),
  ),
  staleTime: 15_000,
  refetchInterval: 5_000,
  refetchIntervalInBackground: false,
});
const storageClusters = computed(() =>
  groupStorageVolumesByCluster(
    storageVolumeQuery.data.value?.data.items ?? [],
    gatewayPoolQuery.data.value?.data.items ?? [],
    { gatewayInventoryAvailable: gatewayPoolQuery.isSuccess.value },
  ),
);
const replicationTargetVolumes = computed(() =>
  selectableReplicationVolumes(storageClusters.value),
);
const selectedTargetVolumeId = ref('');
watch(
  replicationTargetVolumes,
  (volumes) => {
    if (!volumes.some((volume) => volume.storage_volume_id === selectedTargetVolumeId.value)) {
      selectedTargetVolumeId.value = volumes[0]?.storage_volume_id ?? '';
    }
  },
  { immediate: true },
);

const checkingVolumeId = ref('');
const replicaCheckError = ref<unknown>();
const replicaCheckVolumeId = ref('');
const lastCheckedAt = ref<Record<string, string>>({});

const isRefreshing = computed(
  () =>
    artifactQuery.isFetching.value ||
    commitGraphQuery.isFetching.value ||
    commitDiffQuery.isFetching.value ||
    coverageQuery.isFetching.value ||
    availabilityQuery.isFetching.value ||
    materializationQuery.isFetching.value ||
    materializationBusy.value,
);
const objectSetDigest = computed(
  () => materializations.value[0]?.object_set_digest ?? coverage.value[0]?.object_set_digest,
);
const completeVolumeCount = computed(() => {
  const count = availability.value?.complete_volume_count;
  if (count !== undefined) return count;
  return String(coverage.value.filter((item) => item.state === 'complete').length);
});

function materializationStateLabel(state: MaterializationView['state']): string {
  return (
    {
      queued: '排队中',
      planning: '规划中',
      waiting_for_sources: '等待来源',
      materializing: '传输中',
      verifying: '校验中',
      complete: '已完成',
      stalled: '已停滞',
      failed: '失败',
      cancelled: '已取消',
    }[state] ?? state
  );
}

function coverageStateLabel(state: VolumeCommitCoverageView['state']): string {
  return { partial: '部分覆盖', complete: '完整覆盖', retiring: '回收中', deleted: '已删除' }[
    state
  ];
}

function statusLabel(value: string): string {
  return (
    {
      available: '可用',
      degraded: '降级',
      unavailable: '不可用',
      satisfied: '满足策略',
      under_replicated: '副本不足',
      not_requested: '未请求目标',
      partial: '部分覆盖',
      complete: '完整覆盖',
      ready: '可读',
      not_ready: '未就绪',
    }[value] ?? value
  );
}

function statusTagType(value: string): 'success' | 'warning' | 'danger' | 'info' {
  if (['available', 'satisfied', 'complete', 'ready'].includes(value)) return 'success';
  if (['degraded', 'under_replicated', 'partial', 'not_ready'].includes(value)) return 'warning';
  if (['unavailable', 'deleted'].includes(value)) return 'danger';
  return 'info';
}

function materializationTagType(
  state: MaterializationView['state'],
): 'success' | 'warning' | 'danger' | 'info' {
  if (state === 'complete') return 'success';
  if (['failed', 'cancelled'].includes(state)) return 'danger';
  if (['stalled', 'waiting_for_sources'].includes(state)) return 'warning';
  return 'info';
}

function diffTypeLabel(changeType: string): string {
  return (
    { added: '新增', modified: '修改', deleted: '删除', renamed: '重命名' }[changeType] ??
    changeType
  );
}

function diffTagType(changeType: string): 'success' | 'warning' | 'danger' | 'info' {
  if (changeType === 'added') return 'success';
  if (changeType === 'modified') return 'warning';
  if (changeType === 'deleted') return 'danger';
  return 'info';
}

function coverageTagType(
  state: VolumeCommitCoverageView['state'],
): 'success' | 'warning' | 'danger' | 'info' {
  if (state === 'complete') return 'success';
  if (state === 'deleted') return 'danger';
  if (state === 'partial' || state === 'retiring') return 'warning';
  return 'info';
}

function volumeName(volumeId: string): string {
  return (
    storageVolumeQuery.data.value?.data.items.find(
      (volume) => volume.storage_volume_id === volumeId,
    )?.display_name ?? volumeId
  );
}

async function refresh(): Promise<void> {
  await Promise.all([
    artifactQuery.refetch(),
    commitGraphQuery.refetch(),
    commitDiffQuery.refetch(),
    refreshMaterialization(),
    storageVolumeQuery.refetch(),
    gatewayPoolQuery.refetch(),
  ]);
  ElMessage.success('已刷新副本完整性观测');
}

async function checkReplica(volumeId: string): Promise<void> {
  if (checkingVolumeId.value) return;
  checkingVolumeId.value = volumeId;
  replicaCheckVolumeId.value = volumeId;
  replicaCheckError.value = undefined;
  try {
    await refreshReplica(volumeId);
    lastCheckedAt.value = { ...lastCheckedAt.value, [volumeId]: String(Date.now()) };
    ElMessage.success(`${volumeName(volumeId)} 副本状态已更新`);
  } catch (error) {
    replicaCheckError.value = error;
  } finally {
    checkingVolumeId.value = '';
  }
}

async function retryReplicaCheck(): Promise<void> {
  if (replicaCheckVolumeId.value) await checkReplica(replicaCheckVolumeId.value);
}

async function materializeTo(volumeId: string): Promise<void> {
  if (!canReplicate.value || !commit.value || !volumeId || materializationBusy.value) return;
  const result = await materializeOrRepair(volumeId, {
    requestId: materializationRequestId({
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      commitId: commitId.value,
      targetStorageVolumeId: volumeId,
    }),
  });
  if (result.mode === 'noop') {
    ElMessage.info('目标副本已经完整，无需复制');
  } else if (result.mode === 'in_flight') {
    ElMessage.info('该目标已有物化任务在执行');
  } else {
    ElMessage.success(result.result?.data.replayed ? '已返回同一物化任务' : '副本物化已排队');
  }
}

async function repairVolume(volumeId: string): Promise<void> {
  if (!canReplicate.value || !volumeId || materializationBusy.value) return;
  const result = await materializeOrRepair(volumeId);
  if (result.mode === 'noop') {
    ElMessage.info('目标副本已经完整，无需修复');
  } else if (result.mode === 'in_flight') {
    ElMessage.info('该副本已有物化任务在执行');
  } else {
    ElMessage.success(result.result?.data.replayed ? '已返回同一修复任务' : '副本修复已重新排队');
  }
}

async function showParentCommit(parentId: string): Promise<void> {
  await router.push({
    name: 'commit-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      commitId: parentId,
    },
  });
}

async function backToArtifact(): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: projectId.value, artifactId: artifactId.value },
    query: { tab: 'commits' },
  });
}
</script>

<template>
  <div class="page commit-detail-page">
    <PageHeading
      :title="commit?.message ?? `Commit ${shortId(commitId)}`"
      :description="`${projectId} / ${artifact?.display_name ?? artifactId}`"
    >
      <template #actions>
        <el-button :icon="Back" @click="backToArtifact">返回 Artifact</el-button>
        <el-button :icon="RefreshRight" :loading="isRefreshing" @click="refresh"
          >检查副本</el-button
        >
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="artifactQuery.error.value"
      :error="artifactQuery.error.value"
      :retrying="artifactQuery.isFetching.value"
      @retry="artifactQuery.refetch"
    />
    <ApiProblemAlert
      v-if="commitGraphQuery.error.value || commitDiffQuery.error.value"
      :error="commitGraphQuery.error.value ?? commitDiffQuery.error.value"
      :retrying="commitGraphQuery.isFetching.value || commitDiffQuery.isFetching.value"
      @retry="refresh"
    />
    <ApiProblemAlert
      v-if="coverageQuery.error.value || availabilityQuery.error.value"
      :error="coverageQuery.error.value ?? availabilityQuery.error.value"
      :retrying="coverageQuery.isFetching.value || availabilityQuery.isFetching.value"
      @retry="refresh"
    />
    <ApiProblemAlert
      v-if="replicaCheckError"
      :error="replicaCheckError"
      :retrying="Boolean(checkingVolumeId)"
      @retry="retryReplicaCheck"
    />

    <el-skeleton
      v-if="artifactQuery.isPending.value || (commitGraphQuery.isPending.value && !commit)"
      :rows="8"
      animated
    />

    <template v-else>
      <el-alert
        v-if="!commit"
        title="Commit 不存在或尚未加载"
        description="请返回 Artifact 列表重新打开该 Commit。"
        type="warning"
        :closable="false"
      />

      <template v-else>
        <section class="content-section commit-detail-section">
          <div class="section-heading section-heading--inline">
            <div>
              <span>IMMUTABLE COMMIT</span>
              <h2>Commit 基本信息</h2>
            </div>
            <CircleCheck class="commit-detail-section__icon" />
          </div>
          <dl class="definition-grid definition-grid--scope">
            <div class="definition-grid__wide">
              <dt>Commit ID</dt>
              <dd>
                <code>{{ commit.commit_id }}</code>
              </dd>
            </div>
            <div>
              <dt>创建时间</dt>
              <dd>{{ formatTime(commit.created_at_unix_ms) }}</dd>
            </div>
            <div>
              <dt>Parent</dt>
              <dd>
                <code>{{ commit.parent_commit_id ?? '—' }}</code>
              </dd>
            </div>
            <div>
              <dt>数据布局</dt>
              <dd>
                <el-tag effect="plain">{{ commitDataLayoutLabel(commit.data_layout) }}</el-tag>
              </dd>
            </div>
            <div>
              <dt>Object 数量</dt>
              <dd>{{ formatCount(availability?.object_count) }}</dd>
            </div>
            <div>
              <dt>ObjectSet Digest</dt>
              <dd>
                <code>{{ objectSetDigest ?? '等待覆盖证据' }}</code>
              </dd>
            </div>
            <div>
              <dt>Namespace</dt>
              <dd>
                <code>{{ artifactId }}</code>
              </dd>
            </div>
            <div class="definition-grid__wide">
              <dt>Tags</dt>
              <dd class="tag-list">
                <el-tag v-for="tag in commitTagNames(commit.tag_names)" :key="tag" effect="plain">{{
                  tag
                }}</el-tag>
                <span v-if="commit.tag_names.length === 0">暂无 Tag</span>
              </dd>
            </div>
            <div class="definition-grid__wide">
              <dt>描述</dt>
              <dd>{{ commit.description ?? '—' }}</dd>
            </div>
          </dl>
        </section>

        <template v-if="commitDiffEnabled">
          <section class="content-section commit-detail-section commit-parent-section">
            <div class="section-heading section-heading--inline">
              <div>
                <span>LINEAGE</span>
                <h2>父 Commit</h2>
                <p v-if="commitDiff?.base_commit">{{ commitDiff.base_commit.message }}</p>
                <p v-else-if="commitDiff">根 Commit，无父版本</p>
              </div>
              <el-button
                v-if="commitDiff?.base_commit"
                text
                type="primary"
                :icon="ArrowRight"
                @click="showParentCommit(commitDiff.base_commit.commit_id)"
              >
                查看父 Commit
              </el-button>
            </div>
            <el-skeleton v-if="commitDiffQuery.isPending.value" :rows="4" animated />
            <dl v-else-if="commitDiff?.base_commit" class="definition-grid definition-grid--scope">
              <div>
                <dt>Commit ID</dt>
                <dd>
                  <code>{{ commitDiff.base_commit.commit_id }}</code>
                </dd>
              </div>
              <div>
                <dt>创建时间</dt>
                <dd>{{ formatTime(commitDiff.base_commit.created_at_unix_ms) }}</dd>
              </div>
              <div>
                <dt>数据布局</dt>
                <dd>
                  <el-tag effect="plain">
                    {{ commitDataLayoutLabel(commitDiff.base_commit.data_layout) }}
                  </el-tag>
                </dd>
              </div>
              <div class="definition-grid__wide">
                <dt>Tags</dt>
                <dd class="tag-list">
                  <el-tag
                    v-for="tagName in commitTagNames(commitDiff.base_commit.tag_names)"
                    :key="tagName"
                    effect="plain"
                  >
                    {{ tagName }}
                  </el-tag>
                  <span v-if="commitTagNames(commitDiff.base_commit.tag_names).length === 0">
                    暂无 Tag
                  </span>
                </dd>
              </div>
              <div class="definition-grid__wide">
                <dt>描述</dt>
                <dd>{{ commitDiff.base_commit.description ?? '—' }}</dd>
              </div>
            </dl>
          </section>

          <section class="content-section commit-detail-section commit-diff-section">
            <div class="section-heading">
              <span>CHANGESET</span>
              <h2>文件 Diff</h2>
            </div>
            <el-skeleton v-if="commitDiffQuery.isPending.value" :rows="6" animated />
            <template v-else-if="commitDiff">
              <div class="diff-summary">
                <div>
                  <span>新增</span
                  ><strong>{{ formatCount(commitDiff.summary.files_added) }}</strong>
                </div>
                <div>
                  <span>修改</span
                  ><strong>{{ formatCount(commitDiff.summary.files_modified) }}</strong>
                </div>
                <div>
                  <span>删除</span
                  ><strong>{{ formatCount(commitDiff.summary.files_deleted) }}</strong>
                </div>
                <div>
                  <span>重命名</span
                  ><strong>{{ formatCount(commitDiff.summary.files_renamed) }}</strong>
                </div>
                <div>
                  <span>新增数据</span
                  ><strong>{{ formatBytes(commitDiff.summary.bytes_added) }}</strong>
                </div>
                <div>
                  <span>移除数据</span
                  ><strong>{{ formatBytes(commitDiff.summary.bytes_removed) }}</strong>
                </div>
              </div>
              <div class="diff-list">
                <el-empty
                  v-if="commitDiff.changes.length === 0"
                  description="与基线没有文件变化"
                  :image-size="64"
                />
                <div
                  v-for="change in commitDiff.changes"
                  v-else
                  :key="`${change.change_type}:${change.path}`"
                >
                  <el-tag :type="diffTagType(change.change_type)" effect="plain">
                    {{ diffTypeLabel(change.change_type) }}
                  </el-tag>
                  <span class="diff-list__path">
                    <code>{{ change.path }}</code>
                    <small v-if="change.previous_path">原路径 {{ change.previous_path }}</small>
                  </span>
                  <span class="diff-list__size">
                    {{ formatBytes(change.old_size_bytes) }} →
                    {{ formatBytes(change.new_size_bytes) }}
                  </span>
                </div>
              </div>
            </template>
          </section>
        </template>

        <section class="content-section commit-detail-section">
          <div class="section-heading section-heading--inline">
            <div>
              <span>AVAILABILITY</span>
              <h2>Commit 可用性</h2>
              <p>状态由当前已验证的对象副本和可服务来源派生。</p>
            </div>
            <el-tag
              v-if="availability"
              :type="statusTagType(availability.view_readiness)"
              effect="plain"
            >
              {{ statusLabel(availability.view_readiness) }}
            </el-tag>
          </div>
          <div v-if="availability" class="commit-availability-grid">
            <div>
              <span>内容存在</span
              ><el-tag :type="statusTagType(availability.content_presence)" effect="plain">{{
                statusLabel(availability.content_presence)
              }}</el-tag>
            </div>
            <div>
              <span>来源服务</span
              ><el-tag :type="statusTagType(availability.source_serving)" effect="plain">{{
                statusLabel(availability.source_serving)
              }}</el-tag>
            </div>
            <div>
              <span>耐久性</span
              ><el-tag :type="statusTagType(availability.durability)" effect="plain">{{
                statusLabel(availability.durability)
              }}</el-tag>
            </div>
            <div>
              <span>目标覆盖</span
              ><el-tag :type="statusTagType(availability.target_coverage)" effect="plain">{{
                statusLabel(availability.target_coverage)
              }}</el-tag>
            </div>
            <div>
              <span>完整副本数</span><strong>{{ completeVolumeCount }}</strong>
            </div>
            <div>
              <span>缺失对象</span
              ><strong>{{ formatCount(String(availability.missing_objects.length)) }}</strong>
            </div>
          </div>
          <el-empty
            v-else-if="!availabilityQuery.isPending.value"
            description="暂无可用性观测"
            :image-size="72"
          />
        </section>

        <section class="content-section commit-detail-section">
          <div class="section-heading section-heading--inline">
            <div>
              <span>OBJECT PLACEMENTS</span>
              <h2>各 StorageVolume 副本</h2>
              <p>覆盖是对象级证据汇总；完整 Commit 视图不是单独存储的副本事实。</p>
            </div>
            <DocumentCopy />
          </div>
          <ApiProblemAlert
            v-if="coverageQuery.error.value"
            :error="coverageQuery.error.value"
            :retrying="coverageQuery.isFetching.value"
            @retry="coverageQuery.refetch"
          />
          <el-skeleton v-if="coverageQuery.isPending.value" :rows="4" animated />
          <el-empty
            v-else-if="coverage.length === 0"
            description="尚无已验证副本"
            :image-size="72"
          />
          <div v-else class="commit-coverage-list">
            <div
              v-for="item in coverage"
              :key="`${item.storage_volume_id}:${item.placement_generation}`"
              class="commit-coverage-row"
            >
              <div class="commit-coverage-row__identity">
                <strong>{{ volumeName(item.storage_volume_id) }}</strong>
                <code>{{ item.storage_volume_id }}</code>
                <small
                  >Generation {{ item.placement_generation }} · {{ item.verified_objects }} /
                  {{ item.total_objects }} objects · {{ formatBytes(item.verified_bytes) }} /
                  {{ formatBytes(item.total_bytes) }}</small
                >
                <small
                  v-if="lastCheckedAt[item.storage_volume_id]"
                  class="commit-coverage-row__checked"
                >
                  最近检查：{{ formatTime(lastCheckedAt[item.storage_volume_id]) }}
                </small>
              </div>
              <div class="commit-coverage-row__actions">
                <el-tag :type="coverageTagType(item.state)" effect="plain">{{
                  coverageStateLabel(item.state)
                }}</el-tag>
                <el-button
                  size="small"
                  :loading="checkingVolumeId === item.storage_volume_id"
                  @click="checkReplica(item.storage_volume_id)"
                  >检查</el-button
                >
                <el-button
                  v-if="canReplicate && item.state !== 'complete'"
                  size="small"
                  :loading="retryMutation.isPending.value || materializeMutation.isPending.value"
                  @click="repairVolume(item.storage_volume_id)"
                  >修复副本</el-button
                >
              </div>
            </div>
          </div>
        </section>

        <section
          v-if="canReplicate"
          class="content-section commit-detail-section commit-materialization-section"
        >
          <div class="section-heading section-heading--inline">
            <div>
              <span>MATERIALIZATION</span>
              <h2>复制 / 修复副本</h2>
              <p>复制新副本与修复已有副本共用 Materialization Job、checkpoint 和校验路径。</p>
            </div>
            <RefreshRight />
          </div>
          <ApiProblemAlert
            v-if="storageVolumeQuery.error.value || gatewayPoolQuery.error.value"
            :error="storageVolumeQuery.error.value ?? gatewayPoolQuery.error.value"
          />
          <ApiProblemAlert
            v-if="actionError"
            :error="actionError"
            :retrying="materializeMutation.isPending.value || retryMutation.isPending.value"
            @retry="refresh"
          />
          <div class="commit-materialization-target">
            <el-select
              v-model="selectedTargetVolumeId"
              placeholder="选择目标 Gateway 集群 / StorageVolume"
              :loading="storageVolumeQuery.isPending.value || gatewayPoolQuery.isPending.value"
              :disabled="replicationTargetVolumes.length === 0"
              style="min-width: min(100%, 420px)"
            >
              <el-option-group
                v-for="group in storageClusters"
                :key="group.edgeClusterId"
                :label="`${group.gatewayPool?.display_name ?? group.edgeClusterId} · ${group.edgeClusterId}`"
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
              :icon="DocumentCopy"
              :disabled="!selectedTargetVolumeId || hasCompleteCoverage(selectedTargetVolumeId)"
              :loading="materializeMutation.isPending.value"
              @click="materializeTo(selectedTargetVolumeId)"
              >复制到目标</el-button
            >
            <small class="commit-materialization-target__hint"
              >选择 Ready Volume；已完整覆盖的目标无需重复复制。</small
            >
          </div>

          <el-skeleton v-if="materializationQuery.isPending.value" :rows="4" animated />
          <el-empty
            v-else-if="materializations.length === 0"
            description="尚无物化任务"
            :image-size="72"
          />
          <div v-else class="commit-materialization-list">
            <div
              v-for="item in materializations"
              :key="item.materialization_id"
              class="commit-materialization-row"
            >
              <div class="commit-materialization-row__identity">
                <strong>{{ volumeName(item.target_storage_volume_id) }}</strong>
                <code>{{ item.materialization_id }}</code>
                <small
                  >{{ item.verified_objects }} / {{ item.total_objects }} objects ·
                  {{ formatBytes(item.verified_bytes) }} / {{ formatBytes(item.total_bytes) }} ·
                  {{ item.source_count }} 个来源</small
                >
                <small v-if="item.issue" class="commit-materialization-row__issue">{{
                  item.issue.message
                }}</small>
              </div>
              <div class="commit-materialization-row__actions">
                <el-tag :type="materializationTagType(item.state)" effect="plain">{{
                  materializationStateLabel(item.state)
                }}</el-tag>
                <el-button
                  v-if="!isMaterializationActive(item.state) && item.state !== 'complete'"
                  size="small"
                  :loading="retryMutation.isPending.value"
                  @click="repairVolume(item.target_storage_volume_id)"
                  >修复</el-button
                >
                <el-button
                  v-else-if="
                    item.state === 'complete' &&
                    coverage.some(
                      (entry) =>
                        entry.storage_volume_id === item.target_storage_volume_id &&
                        entry.state !== 'complete',
                    )
                  "
                  size="small"
                  :loading="retryMutation.isPending.value"
                  @click="repairVolume(item.target_storage_volume_id)"
                  >重新校验并修复</el-button
                >
              </div>
            </div>
          </div>
        </section>

        <el-alert
          class="commit-integrity-note"
          title="完整性检查说明"
          description="检查副本会刷新 Central 当前覆盖、可用性和物化任务观测；Agent 的物理对象 scrub 按启动和周期计划执行。发现缺失或损坏对象后，使用修复副本重新规划并从其他健康副本补齐。"
          type="info"
          :closable="false"
        />
      </template>
    </template>
  </div>
</template>

<style scoped>
.commit-detail-section__icon {
  color: var(--green);
}

.commit-availability-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  border: 1px solid var(--line);
}

.commit-availability-grid > div {
  min-width: 0;
  display: flex;
  min-height: 76px;
  flex-direction: column;
  justify-content: center;
  gap: 8px;
  padding: 14px;
  border-right: 1px solid var(--line);
}

.commit-availability-grid > div:last-child {
  border-right: 0;
}

.commit-availability-grid span {
  color: var(--muted);
  font-size: 11px;
}

.commit-availability-grid strong {
  font-size: 18px;
}

.commit-coverage-list,
.commit-materialization-list {
  border-top: 1px solid var(--line);
}

.commit-coverage-row,
.commit-materialization-row {
  display: flex;
  min-width: 0;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 14px 4px;
  border-bottom: 1px solid var(--line);
}

.commit-coverage-row__identity,
.commit-materialization-row__identity {
  min-width: 0;
  display: grid;
  gap: 4px;
}

.commit-coverage-row__identity code,
.commit-materialization-row__identity code,
.commit-coverage-row__identity small,
.commit-materialization-row__identity small {
  color: var(--muted);
  font-size: 11px;
  overflow-wrap: anywhere;
}

.commit-coverage-row__actions,
.commit-materialization-row__actions {
  display: inline-flex;
  flex: 0 0 auto;
  align-items: center;
  gap: 8px;
}

.commit-materialization-target {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  margin-bottom: 18px;
}

.commit-materialization-target__hint {
  flex-basis: 100%;
  color: var(--muted);
  font-size: 12px;
}

.commit-materialization-row__issue {
  color: var(--danger) !important;
}

.diff-summary {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  border: 1px solid var(--line);
}

.diff-summary > div {
  min-width: 0;
  display: flex;
  min-height: 72px;
  flex-direction: column;
  justify-content: center;
  gap: 6px;
  padding: 12px 14px;
  border-right: 1px solid var(--line);
}

.diff-summary > div:last-child {
  border-right: 0;
}

.diff-summary span {
  color: var(--muted);
  font-size: 11px;
}

.diff-summary strong {
  font-size: 17px;
}

.diff-list {
  border-top: 1px solid var(--line);
}

.diff-list > div {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  padding: 12px 4px;
  border-bottom: 1px solid var(--line);
}

.diff-list__path {
  min-width: 0;
  display: grid;
  gap: 3px;
}

.diff-list__path code,
.diff-list__path small,
.diff-list__size {
  overflow-wrap: anywhere;
}

.diff-list__path small,
.diff-list__size {
  color: var(--muted);
  font-size: 11px;
}

.commit-integrity-note {
  margin-top: 24px;
}

@media (max-width: 900px) {
  .commit-availability-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .commit-availability-grid > div:nth-child(3n) {
    border-right: 0;
  }

  .commit-coverage-row,
  .commit-materialization-row {
    align-items: flex-start;
    flex-direction: column;
  }

  .diff-summary {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .diff-summary > div:nth-child(3n) {
    border-right: 0;
  }

  .diff-list > div {
    grid-template-columns: auto minmax(0, 1fr);
  }

  .diff-list__size {
    grid-column: 2;
  }
}

@media (max-width: 560px) {
  .commit-availability-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .commit-availability-grid > div:nth-child(3n) {
    border-right: 1px solid var(--line);
  }

  .commit-availability-grid > div:nth-child(2n) {
    border-right: 0;
  }

  .diff-summary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .diff-summary > div:nth-child(3n) {
    border-right: 1px solid var(--line);
  }

  .diff-summary > div:nth-child(2n) {
    border-right: 0;
  }
}
</style>
