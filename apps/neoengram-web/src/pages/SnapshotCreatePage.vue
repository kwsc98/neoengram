<script setup lang="ts">
import {
  ArrowRight,
  Back,
  CircleCheck,
  DocumentCopy,
  Files,
  Location,
  RefreshRight,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useMutation, useQuery } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  createSnapshot,
  queryArtifactCommitDiff,
  queryArtifactCommitGraph,
  querySnapshot,
  queryStorageVolumeList,
} from '@/api/operations';
import type { CommitNode, CreateSnapshotResponse, StorageVolumeView } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  snapshotIntegrityLabel,
  snapshotIntegrityTagType,
  snapshotPhaseLabel,
  snapshotPollInterval,
  snapshotStateLabel,
  snapshotStateTagType,
} from '@/features/snapshots/status';
import { commitTagNames } from '@/utils/commit';
import { formatBytes, formatCount, formatTime } from '@/utils/format';

type Stage = 'commit' | 'placement' | 'delivery';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const requestedCommitId = computed(() => String(route.query.commit_id ?? ''));
const requestedStorageVolumeId = computed(() => String(route.query.storage_volume_id ?? ''));

const activeStage = ref<Stage>('commit');
const selectedCommitId = ref(requestedCommitId.value);
const commitNodes = ref<CommitNode[]>([]);
const nextCommitCursor = ref<string>();
const loadingMoreCommits = ref(false);
const volumeCursor = ref<string>();
const volumeCursorHistory = ref<string[]>([]);
const selectedVolume = ref<StorageVolumeView>();
const snapshotRequestId = ref<string>();
const createOutcome = ref<CreateSnapshotResponse>();

const stages: Array<{ key: Stage; index: number; label: string; detail: string }> = [
  { key: 'commit', index: 1, label: '固定版本', detail: 'Commit 与 Diff' },
  { key: 'placement', index: 2, label: '存储位置', detail: 'Ready Volume' },
  { key: 'delivery', index: 3, label: '交付状态', detail: '创建与校验' },
];

const activeStageIndex = computed(
  () => stages.find((stage) => stage.key === activeStage.value)?.index ?? 1,
);

const commitGraphQuery = useQuery({
  queryKey: computed(() => [
    'artifact-commits',
    tenantId.value,
    projectId.value,
    artifactId.value,
    'snapshot-create',
  ]),
  queryFn: () => queryArtifactCommitGraph(tenantId.value, projectId.value, artifactId.value),
});

watch(
  () => commitGraphQuery.data.value,
  (result) => {
    if (!result) return;
    commitNodes.value = [...result.data.graph.nodes];
    nextCommitCursor.value = result.data.graph.next_cursor;
    if (!selectedCommitId.value) {
      selectedCommitId.value =
        result.data.graph.head_commit_id ?? result.data.graph.nodes[0]?.commit_id ?? '';
    }
  },
  { immediate: true },
);

const commitDiffQuery = useQuery({
  queryKey: computed(() => [
    'artifact-commit-diff',
    tenantId.value,
    projectId.value,
    artifactId.value,
    selectedCommitId.value,
    'snapshot-create',
  ]),
  queryFn: () =>
    queryArtifactCommitDiff(
      tenantId.value,
      projectId.value,
      artifactId.value,
      selectedCommitId.value,
    ),
  enabled: computed(() => Boolean(selectedCommitId.value)),
});

const selectedCommit = computed(
  () =>
    commitDiffQuery.data.value?.data.diff.target_commit ??
    commitNodes.value.find((node) => node.commit_id === selectedCommitId.value),
);
const selectedCommitTags = computed(() => commitTagNames(selectedCommit.value?.tag_names ?? []));
const commitDiff = computed(() => commitDiffQuery.data.value?.data.diff);

const storageVolumeQuery = useQuery({
  queryKey: computed(() => [
    'storage-volumes',
    tenantId.value,
    'snapshot-create',
    volumeCursor.value ?? '',
  ]),
  queryFn: () =>
    queryStorageVolumeList({
      tenant_id: tenantId.value,
      page_size: 20,
      ...(volumeCursor.value ? { cursor: volumeCursor.value } : {}),
    }),
  enabled: computed(() => activeStage.value === 'placement'),
});

const volumePage = computed(() => storageVolumeQuery.data.value?.data.items ?? []);

watch(
  volumePage,
  (volumes) => {
    if (selectedVolume.value || !requestedStorageVolumeId.value) return;
    selectedVolume.value = volumes.find(
      (volume) =>
        volume.state === 'ready' && volume.storage_volume_id === requestedStorageVolumeId.value,
    );
  },
  { immediate: true },
);

const createMutation = useMutation({ mutationFn: createSnapshot });

watch([tenantId, projectId, artifactId, requestedCommitId, requestedStorageVolumeId], () => {
  activeStage.value = 'commit';
  selectedCommitId.value = requestedCommitId.value;
  commitNodes.value = [];
  nextCommitCursor.value = undefined;
  volumeCursor.value = undefined;
  volumeCursorHistory.value = [];
  selectedVolume.value = undefined;
  snapshotRequestId.value = undefined;
  createOutcome.value = undefined;
  createMutation.reset();
});

const createdSnapshotId = computed(() => createOutcome.value?.snapshot.snapshot_id ?? '');
const deliveryQuery = useQuery({
  queryKey: computed(() => [
    'snapshot',
    tenantId.value,
    projectId.value,
    artifactId.value,
    createdSnapshotId.value,
    'snapshot-create',
  ]),
  queryFn: async () => {
    const result = await querySnapshot(tenantId.value, createdSnapshotId.value);
    const item = result.data.snapshot;
    if (
      item.tenant_id !== tenantId.value ||
      item.project_id !== projectId.value ||
      item.artifact_id !== artifactId.value
    ) {
      throw new Error('Snapshot 不属于当前 Artifact');
    }
    return result;
  },
  enabled: computed(() => Boolean(createdSnapshotId.value)),
  refetchInterval: (query) =>
    snapshotPollInterval(
      query.state.data?.data.snapshot.state ?? createOutcome.value?.snapshot.state,
    ),
});
const deliverySnapshot = computed(
  () => deliveryQuery.data.value?.data.snapshot ?? createOutcome.value?.snapshot,
);

function selectCommit(commitId: string): void {
  if (activeStage.value === 'delivery') return;
  selectedCommitId.value = commitId;
  snapshotRequestId.value = undefined;
  createMutation.reset();
}

async function loadMoreCommits(): Promise<void> {
  if (!nextCommitCursor.value || loadingMoreCommits.value) return;
  loadingMoreCommits.value = true;
  try {
    const result = await queryArtifactCommitGraph(
      tenantId.value,
      projectId.value,
      artifactId.value,
      nextCommitCursor.value,
    );
    const byId = new Map(commitNodes.value.map((node) => [node.commit_id, node]));
    for (const node of result.data.graph.nodes) byId.set(node.commit_id, node);
    commitNodes.value = [...byId.values()];
    nextCommitCursor.value = result.data.graph.next_cursor;
  } finally {
    loadingMoreCommits.value = false;
  }
}

function continueToPlacement(): void {
  if (!selectedCommitId.value) {
    ElMessage.warning('请选择 Commit');
    return;
  }
  if (commitDiffQuery.isPending.value || commitDiffQuery.error.value) {
    ElMessage.warning('Commit Diff 尚未就绪');
    return;
  }
  activeStage.value = 'placement';
}

function selectVolume(volume: StorageVolumeView): void {
  if (volume.state !== 'ready') return;
  selectedVolume.value = volume;
  snapshotRequestId.value = undefined;
  createMutation.reset();
}

function nextVolumePage(): void {
  const next = storageVolumeQuery.data.value?.data.next_cursor;
  if (!next) return;
  volumeCursorHistory.value.push(volumeCursor.value ?? '');
  volumeCursor.value = next;
}

function previousVolumePage(): void {
  volumeCursor.value = volumeCursorHistory.value.pop() || undefined;
}

function backToCommitSelection(): void {
  snapshotRequestId.value = undefined;
  createMutation.reset();
  activeStage.value = 'commit';
}

async function createSnapshotNow(): Promise<void> {
  if (createMutation.isPending.value) return;
  if (!selectedVolume.value || selectedVolume.value.state !== 'ready') {
    ElMessage.warning('请选择可用的 StorageVolume');
    return;
  }
  snapshotRequestId.value ??= `snapshot-request-${globalThis.crypto.randomUUID()}`;
  try {
    const result = await createMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      commit_id: selectedCommitId.value,
      storage_volume_id: selectedVolume.value.storage_volume_id,
      snapshot_request_id: snapshotRequestId.value,
    });
    createOutcome.value = result.data;
    snapshotRequestId.value = undefined;
    activeStage.value = 'delivery';
    ElMessage.success(result.data.replayed ? '已返回同一请求的幂等结果' : 'Snapshot 已创建');
  } catch {
    // ApiProblemAlert renders the typed API error and keeps the request ID reusable.
  }
}

function goToStage(stage: Stage, index: number): void {
  if (activeStage.value === 'delivery' || index >= activeStageIndex.value) return;
  activeStage.value = stage;
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

function backendLabel(volume: StorageVolumeView): string {
  return volume.backend_type.toUpperCase();
}

function accessModeLabel(accessMode: StorageVolumeView['access_mode']): string {
  return {
    read_write_once: '单节点读写',
    read_write_many: '多节点读写',
    read_only_many: '多节点只读',
  }[accessMode];
}

function volumeStateLabel(state: StorageVolumeView['state']): string {
  return { ready: 'Ready', degraded: 'Degraded', unavailable: 'Unavailable' }[state];
}

function volumeStateTagType(state: StorageVolumeView['state']): 'success' | 'warning' | 'danger' {
  if (state === 'ready') return 'success';
  if (state === 'degraded') return 'warning';
  return 'danger';
}

async function backToVersionHistory(): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
    },
    query: {
      tab: 'commits',
      ...(selectedCommitId.value ? { commit_id: selectedCommitId.value } : {}),
    },
  });
}

async function openSnapshot(): Promise<void> {
  if (!deliverySnapshot.value) return;
  await router.push({
    name: 'snapshot-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      snapshotId: deliverySnapshot.value.snapshot_id,
    },
  });
}

async function openSnapshotList(): Promise<void> {
  await router.push({
    name: 'snapshot-list',
    params: { tenantId: tenantId.value },
    query: { project_id: projectId.value, artifact_id: artifactId.value },
  });
}
</script>

<template>
  <div class="page snapshot-create">
    <PageHeading title="创建 Snapshot" :description="`${projectId} / ${artifactId}`">
      <template #actions>
        <el-button :icon="Back" @click="backToVersionHistory">返回版本历史</el-button>
      </template>
    </PageHeading>

    <ol class="create-steps" aria-label="Snapshot 创建流程">
      <li
        v-for="stage in stages"
        :key="stage.key"
        :class="{
          'is-active': activeStage === stage.key,
          'is-complete': activeStageIndex > stage.index,
        }"
      >
        <button type="button" @click="goToStage(stage.key, stage.index)">
          <span>{{ activeStageIndex > stage.index ? '✓' : stage.index }}</span>
          <strong>{{ stage.label }}</strong>
          <small>{{ stage.detail }}</small>
        </button>
      </li>
    </ol>

    <template v-if="activeStage === 'commit'">
      <div class="commit-layout">
        <section class="create-section commit-graph-section">
          <header class="section-heading">
            <div>
              <span>COMMIT GRAPH</span>
              <h2>选择固定版本</h2>
            </div>
            <el-tag v-if="commitGraphQuery.data.value" effect="plain">
              Graph {{ commitGraphQuery.data.value.data.graph.graph_version }}
            </el-tag>
          </header>

          <ApiProblemAlert
            v-if="commitGraphQuery.error.value"
            :error="commitGraphQuery.error.value"
            :retrying="commitGraphQuery.isFetching.value"
            @retry="commitGraphQuery.refetch"
          />
          <el-skeleton v-if="commitGraphQuery.isPending.value" :rows="6" animated />
          <el-empty v-else-if="commitNodes.length === 0" description="此 Artifact 暂无 Commit" />
          <div v-else class="commit-list">
            <button
              v-for="commit in commitNodes"
              :key="commit.commit_id"
              type="button"
              :class="{ 'is-selected': selectedCommitId === commit.commit_id }"
              :aria-pressed="selectedCommitId === commit.commit_id"
              @click="selectCommit(commit.commit_id)"
            >
              <span class="commit-node"><i /></span>
              <span class="commit-copy">
                <strong>{{ commit.message }}</strong>
                <code>{{ commit.commit_id }}</code>
                <small>{{ formatTime(commit.created_at_unix_ms) }}</small>
              </span>
              <span class="commit-tags">
                <el-tag
                  v-for="tagName in commitTagNames(commit.tag_names)"
                  :key="tagName"
                  size="small"
                  effect="plain"
                >
                  {{ tagName }}
                </el-tag>
                <el-tag
                  v-if="commitGraphQuery.data.value?.data.graph.head_commit_id === commit.commit_id"
                  size="small"
                  type="success"
                  effect="plain"
                >
                  Head
                </el-tag>
              </span>
            </button>
            <el-button
              v-if="nextCommitCursor"
              text
              type="primary"
              :loading="loadingMoreCommits"
              @click="loadMoreCommits"
            >
              加载更多 Commit
            </el-button>
          </div>
        </section>

        <section class="create-section commit-diff-section">
          <header class="section-heading">
            <div>
              <span>COMMIT DIFF</span>
              <h2>{{ selectedCommit?.message ?? '选择一个 Commit' }}</h2>
            </div>
            <DocumentCopy />
          </header>

          <ApiProblemAlert
            v-if="commitDiffQuery.error.value"
            :error="commitDiffQuery.error.value"
            :retrying="commitDiffQuery.isFetching.value"
            @retry="commitDiffQuery.refetch"
          />
          <el-skeleton
            v-if="selectedCommitId && commitDiffQuery.isPending.value"
            :rows="7"
            animated
          />
          <el-empty v-else-if="!selectedCommitId" description="尚未选择 Commit" />
          <template v-else-if="commitDiff">
            <div class="fixed-commit">
              <div class="fixed-commit__identity">
                <div>
                  <small>固定 Commit</small>
                  <code>{{ commitDiff.target_commit.commit_id }}</code>
                </div>
                <div>
                  <small>Parent</small>
                  <code>{{ commitDiff.target_commit.parent_commit_id ?? '根 Commit' }}</code>
                </div>
              </div>
              <div class="commit-tags">
                <el-tag v-for="tagName in selectedCommitTags" :key="tagName" effect="plain">
                  {{ tagName }}
                </el-tag>
                <span v-if="selectedCommitTags.length === 0">暂无 Tag</span>
              </div>
            </div>

            <dl class="diff-summary">
              <div>
                <dt>新增</dt>
                <dd>{{ formatCount(commitDiff.summary.files_added) }}</dd>
              </div>
              <div>
                <dt>修改</dt>
                <dd>{{ formatCount(commitDiff.summary.files_modified) }}</dd>
              </div>
              <div>
                <dt>删除</dt>
                <dd>{{ formatCount(commitDiff.summary.files_deleted) }}</dd>
              </div>
              <div>
                <dt>重命名</dt>
                <dd>{{ formatCount(commitDiff.summary.files_renamed) }}</dd>
              </div>
              <div>
                <dt>增加</dt>
                <dd>{{ formatBytes(commitDiff.summary.bytes_added) }}</dd>
              </div>
              <div>
                <dt>移除</dt>
                <dd>{{ formatBytes(commitDiff.summary.bytes_removed) }}</dd>
              </div>
            </dl>

            <div class="diff-list">
              <div
                v-for="change in commitDiff.changes"
                :key="`${change.change_type}:${change.path}`"
              >
                <el-tag :type="diffTagType(change.change_type)" size="small" effect="plain">
                  {{ diffTypeLabel(change.change_type) }}
                </el-tag>
                <span>
                  <code>{{ change.path }}</code>
                  <small v-if="change.previous_path">来自 {{ change.previous_path }}</small>
                </span>
                <small>
                  {{
                    change.new_size_bytes
                      ? formatBytes(change.new_size_bytes)
                      : change.old_size_bytes
                        ? formatBytes(change.old_size_bytes)
                        : '—'
                  }}
                </small>
              </div>
              <el-empty
                v-if="commitDiff.changes.length === 0"
                description="此 Commit 没有文件变化"
              />
            </div>
          </template>
        </section>
      </div>

      <footer class="create-actions">
        <span v-if="selectedCommit"
          >已选择 <code>{{ selectedCommit.commit_id }}</code></span
        >
        <span v-else>请选择一个 Commit</span>
        <el-button type="primary" :icon="ArrowRight" @click="continueToPlacement">
          选择存储位置
        </el-button>
      </footer>
    </template>

    <template v-else-if="activeStage === 'placement'">
      <section class="create-section volume-section">
        <header class="section-heading">
          <div>
            <span>STORAGE VOLUME</span>
            <h2>选择存储位置</h2>
          </div>
          <el-tag v-if="selectedVolume" type="success" effect="plain">
            {{ selectedVolume.region }}
          </el-tag>
        </header>

        <ApiProblemAlert
          v-if="storageVolumeQuery.error.value"
          :error="storageVolumeQuery.error.value"
          :retrying="storageVolumeQuery.isFetching.value"
          @retry="storageVolumeQuery.refetch"
        />
        <ApiProblemAlert
          v-if="createMutation.error.value"
          :error="createMutation.error.value"
          :retrying="createMutation.isPending.value"
          @retry="createSnapshotNow"
        />
        <el-skeleton v-if="storageVolumeQuery.isPending.value" :rows="7" animated />
        <template v-else>
          <div class="volume-list">
            <button
              v-for="volume in volumePage"
              :key="volume.storage_volume_id"
              type="button"
              :class="{
                'is-selected': selectedVolume?.storage_volume_id === volume.storage_volume_id,
              }"
              :aria-pressed="selectedVolume?.storage_volume_id === volume.storage_volume_id"
              :disabled="volume.state !== 'ready'"
              @click="selectVolume(volume)"
            >
              <span class="volume-icon"><Location /></span>
              <span class="volume-copy">
                <strong>{{ volume.display_name }}</strong>
                <code>{{ volume.storage_volume_id }}</code>
                <small>{{ volume.region }}</small>
              </span>
              <span class="volume-meta">
                <strong>{{ backendLabel(volume) }}</strong>
                <small>{{ accessModeLabel(volume.access_mode) }}</small>
                <small>{{ volume.edge_cluster_id }}</small>
                <el-tag size="small" :type="volumeStateTagType(volume.state)" effect="plain">
                  {{ volumeStateLabel(volume.state) }}
                </el-tag>
              </span>
            </button>
            <el-empty v-if="volumePage.length === 0" description="本页没有 StorageVolume" />
          </div>
          <PageCursor
            :has-previous="volumeCursorHistory.length > 0"
            :has-next="Boolean(storageVolumeQuery.data.value?.data.next_cursor)"
            :loading="storageVolumeQuery.isFetching.value"
            @previous="previousVolumePage"
            @next="nextVolumePage"
          />
        </template>
      </section>

      <section v-if="selectedVolume" class="selection-summary" aria-label="Snapshot 创建摘要">
        <div>
          <span>Commit</span><code>{{ selectedCommitId }}</code>
        </div>
        <div>
          <span>StorageVolume</span><code>{{ selectedVolume.storage_volume_id }}</code>
        </div>
        <div>
          <span>Region</span><strong>{{ selectedVolume.region }}</strong>
        </div>
        <div><span>访问模式</span><strong>只读 Snapshot</strong></div>
      </section>

      <footer class="create-actions">
        <el-button @click="backToCommitSelection">返回 Commit</el-button>
        <el-button
          type="primary"
          :icon="ArrowRight"
          :loading="createMutation.isPending.value"
          :disabled="!selectedVolume || selectedVolume.state !== 'ready'"
          @click="createSnapshotNow"
        >
          创建 Snapshot
        </el-button>
      </footer>
    </template>

    <template v-else>
      <section v-if="deliverySnapshot" class="delivery-status">
        <header>
          <span
            class="delivery-status__icon"
            :class="{
              'is-ready': deliverySnapshot.state === 'ready',
              'is-abnormal': deliverySnapshot.state === 'abnormal',
            }"
          >
            <CircleCheck v-if="deliverySnapshot.state === 'ready'" />
            <WarningFilled v-else-if="deliverySnapshot.state === 'abnormal'" />
            <RefreshRight v-else />
          </span>
          <div>
            <small>{{ snapshotStateLabel(deliverySnapshot.state) }}</small>
            <h2>
              {{
                deliverySnapshot.state === 'ready'
                  ? 'Snapshot 已可用'
                  : deliverySnapshot.state === 'abnormal'
                    ? 'Snapshot 交付异常'
                    : '正在创建 Snapshot'
              }}
            </h2>
            <p>{{ snapshotPhaseLabel(deliverySnapshot.phase) }}</p>
          </div>
          <el-button
            :icon="RefreshRight"
            :loading="deliveryQuery.isFetching.value"
            @click="deliveryQuery.refetch"
          >
            刷新
          </el-button>
        </header>

        <ApiProblemAlert
          v-if="deliveryQuery.error.value"
          :error="deliveryQuery.error.value"
          :retrying="deliveryQuery.isFetching.value"
          @retry="deliveryQuery.refetch"
        />
        <el-alert
          v-if="deliverySnapshot.issue"
          :title="deliverySnapshot.issue.message"
          :description="deliverySnapshot.issue.code"
          type="error"
          :closable="false"
          show-icon
        />

        <div class="delivery-resource">
          <span
            ><Location /><strong>{{ deliverySnapshot.region }}</strong></span
          >
          <code>{{ deliverySnapshot.storage_volume_id }}</code>
          <el-tag :type="snapshotStateTagType(deliverySnapshot.state)" effect="plain">
            {{ snapshotStateLabel(deliverySnapshot.state) }} ·
            {{ snapshotPhaseLabel(deliverySnapshot.phase) }}
          </el-tag>
        </div>

        <dl class="delivery-facts">
          <div>
            <dt>Snapshot</dt>
            <dd>
              <code>{{ deliverySnapshot.snapshot_id }}</code>
            </dd>
          </div>
          <div>
            <dt>Commit</dt>
            <dd>
              <code>{{ deliverySnapshot.commit_id }}</code>
            </dd>
          </div>
          <div>
            <dt>完整性</dt>
            <dd>
              <el-tag
                :type="snapshotIntegrityTagType(deliverySnapshot.integrity.state)"
                effect="plain"
              >
                {{ snapshotIntegrityLabel(deliverySnapshot.integrity.state) }}
              </el-tag>
            </dd>
          </div>
          <div>
            <dt>已校验文件</dt>
            <dd>{{ formatCount(deliverySnapshot.integrity.files_verified) }}</dd>
          </div>
          <div>
            <dt>已校验数据</dt>
            <dd>{{ formatBytes(deliverySnapshot.integrity.bytes_verified) }}</dd>
          </div>
        </dl>

        <div v-if="createOutcome" class="create-outcome">
          <el-tag :type="createOutcome.replayed ? 'warning' : 'success'" effect="plain">
            {{ createOutcome.replayed ? '幂等重放' : '新请求' }}
          </el-tag>
          <el-tag :type="createOutcome.placement_reused ? 'success' : 'info'" effect="plain">
            {{ createOutcome.placement_reused ? '复用现有交付位置' : '新建交付位置' }}
          </el-tag>
        </div>

        <footer>
          <el-button @click="openSnapshotList">返回 Snapshot 列表</el-button>
          <el-button type="primary" :icon="Files" @click="openSnapshot">查看 Snapshot</el-button>
        </footer>
      </section>
    </template>
  </div>
</template>

<style scoped>
.snapshot-create {
  width: min(1240px, 100%);
}

.create-steps {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  margin: 0 0 18px;
  padding: 0;
  border: 1px solid var(--line);
  background: var(--surface);
  list-style: none;
}

.create-steps li {
  border-right: 1px solid var(--line);
}

.create-steps li:last-child {
  border-right: 0;
}

.create-steps button {
  width: 100%;
  min-height: 74px;
  display: grid;
  grid-template-columns: 30px minmax(0, 1fr);
  grid-template-rows: auto auto;
  align-content: center;
  gap: 2px 10px;
  padding: 12px 18px;
  border: 0;
  background: transparent;
  cursor: default;
  text-align: left;
}

.create-steps li.is-complete button {
  cursor: pointer;
}

.create-steps button > span {
  grid-row: 1 / 3;
  align-self: center;
  width: 28px;
  height: 28px;
  display: grid;
  place-items: center;
  border: 1px solid #bdc7c2;
  border-radius: 50%;
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}

.create-steps strong {
  font-size: 13px;
}

.create-steps small {
  color: var(--muted);
  font-size: 11px;
}

.create-steps li.is-active {
  box-shadow: 0 -3px 0 var(--green) inset;
}

.create-steps li.is-active button > span,
.create-steps li.is-complete button > span {
  border-color: var(--green);
  color: #fff;
  background: var(--green);
}

.commit-layout {
  display: grid;
  grid-template-columns: minmax(320px, 0.8fr) minmax(480px, 1.35fr);
  gap: 18px;
}

.create-section,
.delivery-status,
.selection-summary {
  border: 1px solid var(--line);
  background: var(--surface);
}

.section-heading {
  min-height: 70px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  padding: 14px 16px;
  border-bottom: 1px solid var(--line);
}

.section-heading span,
.delivery-status header small {
  color: var(--green);
  font-size: 10px;
  font-weight: 700;
}

.section-heading h2,
.delivery-status h2,
.delivery-status p {
  margin: 0;
}

.section-heading h2,
.delivery-status h2 {
  margin-top: 3px;
  font-size: 16px;
}

.section-heading > svg {
  width: 22px;
  color: var(--green);
}

.commit-list {
  padding: 8px 12px 12px;
}

.commit-list > button:not(.el-button) {
  width: 100%;
  min-height: 76px;
  display: grid;
  grid-template-columns: 26px minmax(0, 1fr) minmax(100px, auto);
  align-items: center;
  gap: 10px;
  padding: 10px 8px;
  border: 1px solid transparent;
  border-bottom-color: var(--line);
  background: transparent;
  cursor: pointer;
  text-align: left;
}

.commit-list > button.is-selected {
  border-color: #9fc7b4;
  background: #f1f8f4;
}

.commit-node {
  align-self: stretch;
  position: relative;
  display: grid;
  place-items: center;
}

.commit-node::before {
  position: absolute;
  inset-block: -11px;
  left: 50%;
  width: 1px;
  background: #b7c7bf;
  content: '';
}

.commit-node i {
  z-index: 1;
  width: 11px;
  height: 11px;
  border: 3px solid var(--surface);
  border-radius: 50%;
  background: var(--green);
  box-shadow: 0 0 0 1px var(--green);
}

.commit-copy,
.commit-copy strong,
.commit-copy code,
.commit-copy small {
  min-width: 0;
  display: block;
}

.commit-copy strong,
.commit-copy code {
  overflow-wrap: anywhere;
}

.commit-copy code,
.commit-copy small {
  margin-top: 4px;
  color: var(--muted);
  font-size: 10px;
}

.commit-tags,
.create-outcome {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 6px;
}

.fixed-commit {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 14px 16px;
  background: var(--surface-soft);
}

.fixed-commit__identity {
  min-width: 0;
  display: grid;
  gap: 10px;
}

.fixed-commit small,
.fixed-commit code {
  display: block;
}

.fixed-commit small {
  margin-bottom: 4px;
  color: var(--muted);
  font-size: 10px;
}

.diff-summary {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  margin: 0;
  border-block: 1px solid var(--line);
}

.diff-summary > div {
  min-width: 0;
  padding: 11px;
  border-right: 1px solid var(--line);
}

.diff-summary > div:last-child {
  border-right: 0;
}

.diff-summary dt {
  color: var(--muted);
  font-size: 10px;
}

.diff-summary dd {
  margin: 5px 0 0;
  font-size: 13px;
  font-weight: 700;
  overflow-wrap: anywhere;
}

.diff-list {
  max-height: 390px;
  overflow: auto;
}

.diff-list > div {
  min-height: 52px;
  display: grid;
  grid-template-columns: 70px minmax(0, 1fr) 100px;
  align-items: center;
  gap: 10px;
  padding: 9px 16px;
  border-bottom: 1px solid var(--line);
}

.diff-list span,
.diff-list code,
.diff-list small {
  min-width: 0;
  overflow-wrap: anywhere;
}

.diff-list span > small {
  display: block;
  margin-top: 3px;
  color: var(--muted);
}

.diff-list > div > small {
  text-align: right;
}

.volume-section {
  min-height: 420px;
}

.volume-list {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  padding: 16px;
}

.volume-list > button {
  min-height: 112px;
  display: grid;
  grid-template-columns: 42px minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  padding: 14px;
  border: 1px solid var(--line);
  background: var(--surface);
  cursor: pointer;
  text-align: left;
}

.volume-list > button:hover,
.volume-list > button.is-selected {
  border-color: #85b49e;
  background: #f1f8f4;
}

.volume-list > button:disabled {
  border-color: var(--line);
  background: var(--surface-soft);
  cursor: not-allowed;
  opacity: 0.72;
}

.volume-icon {
  width: 38px;
  height: 38px;
  display: grid;
  place-items: center;
  color: var(--green);
  background: #dcefe6;
}

.volume-icon svg {
  width: 19px;
}

.volume-copy strong,
.volume-copy code,
.volume-copy small,
.volume-meta strong,
.volume-meta small {
  display: block;
}

.volume-copy code,
.volume-copy small,
.volume-meta small {
  margin-top: 4px;
  color: var(--muted);
  font-size: 10px;
  overflow-wrap: anywhere;
}

.volume-meta {
  text-align: right;
}

.volume-meta .el-tag {
  margin-top: 7px;
}

.volume-section :deep(.page-cursor) {
  padding: 0 16px 16px;
}

.selection-summary {
  display: grid;
  grid-template-columns: 1.2fr 1.2fr 0.7fr 0.7fr;
  margin-top: 14px;
}

.selection-summary > div {
  min-width: 0;
  padding: 13px 16px;
  border-right: 1px solid var(--line);
}

.selection-summary > div:last-child {
  border-right: 0;
}

.selection-summary span,
.selection-summary code,
.selection-summary strong {
  display: block;
}

.selection-summary span {
  margin-bottom: 4px;
  color: var(--muted);
  font-size: 10px;
}

.selection-summary code {
  overflow-wrap: anywhere;
}

.create-actions {
  min-height: 66px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-top: 18px;
  padding: 12px 16px;
  border: 1px solid var(--line);
  background: var(--surface);
}

.create-actions > span {
  color: var(--muted);
  font-size: 11px;
}

.delivery-status > header {
  min-height: 112px;
  display: grid;
  grid-template-columns: 54px minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  padding: 18px;
  border-bottom: 1px solid var(--line);
}

.delivery-status header p {
  margin-top: 6px;
  color: var(--muted);
  font-size: 12px;
}

.delivery-status__icon {
  width: 50px;
  height: 50px;
  display: grid;
  place-items: center;
  color: var(--amber);
  background: #fff6df;
}

.delivery-status__icon.is-ready {
  color: var(--green);
  background: #e4f2eb;
}

.delivery-status__icon.is-abnormal {
  color: var(--red);
  background: #fff0ee;
}

.delivery-status__icon svg {
  width: 25px;
}

.delivery-resource {
  min-height: 68px;
  display: grid;
  grid-template-columns: minmax(170px, 0.7fr) minmax(220px, 1fr) auto;
  align-items: center;
  gap: 14px;
  padding: 14px 18px;
  border-bottom: 1px solid var(--line);
}

.delivery-resource > span {
  display: flex;
  align-items: center;
  gap: 7px;
}

.delivery-resource svg {
  width: 16px;
  color: var(--green);
}

.delivery-facts {
  display: grid;
  grid-template-columns: 1.4fr 1.4fr repeat(3, minmax(0, 0.8fr));
  margin: 0;
  border-bottom: 1px solid var(--line);
}

.delivery-facts > div {
  min-width: 0;
  min-height: 82px;
  padding: 15px;
  border-right: 1px solid var(--line);
}

.delivery-facts > div:last-child {
  border-right: 0;
}

.delivery-facts dt {
  margin-bottom: 7px;
  color: var(--muted);
  font-size: 10px;
}

.delivery-facts dd {
  margin: 0;
  font-weight: 700;
  overflow-wrap: anywhere;
}

.create-outcome {
  justify-content: flex-start;
  padding: 14px 18px;
}

.delivery-status > footer {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  padding: 14px 18px;
  border-top: 1px solid var(--line);
}

.delivery-status :deep(.api-problem),
.delivery-status > .el-alert,
.volume-section :deep(.api-problem),
.commit-graph-section :deep(.api-problem),
.commit-diff-section :deep(.api-problem) {
  margin: 14px 16px;
}

@media (max-width: 900px) {
  .commit-layout,
  .volume-list {
    grid-template-columns: 1fr;
  }

  .selection-summary,
  .delivery-facts {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .selection-summary > div:nth-child(2),
  .delivery-facts > div:nth-child(2),
  .delivery-facts > div:nth-child(4) {
    border-right: 0;
  }

  .selection-summary > div:nth-child(-n + 2),
  .delivery-facts > div:nth-child(-n + 4) {
    border-bottom: 1px solid var(--line);
  }
}

@media (max-width: 640px) {
  .create-steps button {
    min-height: 62px;
    grid-template-columns: 24px minmax(0, 1fr);
    padding: 9px;
  }

  .create-steps button > span {
    width: 22px;
    height: 22px;
  }

  .create-steps small {
    display: none;
  }

  .commit-list > button:not(.el-button) {
    grid-template-columns: 22px minmax(0, 1fr);
  }

  .commit-tags {
    grid-column: 2;
    justify-content: flex-start;
  }

  .diff-summary {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .diff-summary > div:nth-child(3),
  .diff-summary > div:nth-child(6) {
    border-right: 0;
  }

  .diff-summary > div:nth-child(-n + 3) {
    border-bottom: 1px solid var(--line);
  }

  .diff-list > div {
    grid-template-columns: 64px minmax(0, 1fr);
  }

  .diff-list > div > small {
    grid-column: 2;
    text-align: left;
  }

  .volume-list > button {
    grid-template-columns: 36px minmax(0, 1fr);
  }

  .volume-meta {
    grid-column: 2;
    text-align: left;
  }

  .selection-summary,
  .delivery-facts,
  .delivery-resource {
    grid-template-columns: 1fr;
  }

  .selection-summary > div,
  .delivery-facts > div {
    min-height: auto;
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .selection-summary > div:last-child,
  .delivery-facts > div:last-child {
    border-bottom: 0;
  }

  .delivery-status > header {
    grid-template-columns: 44px minmax(0, 1fr);
  }

  .delivery-status__icon {
    width: 42px;
    height: 42px;
  }

  .delivery-status > header .el-button {
    grid-column: 2;
    justify-self: start;
  }

  .create-actions {
    align-items: stretch;
    flex-direction: column;
  }

  .create-actions .el-button {
    width: 100%;
  }
}
</style>
