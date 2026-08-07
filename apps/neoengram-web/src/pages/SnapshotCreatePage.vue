<script setup lang="ts">
import {
  Back,
  CircleCheck,
  DocumentCopy,
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
  queryApiVersion,
  queryArtifact,
  querySnapshot,
  queryStorageVolumeList,
} from '@/api/operations';
import type { CreateSnapshotResponse, StorageVolumeView } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import { supportsArtifactCommitGraph } from '@/features/capabilities';
import {
  snapshotIntegrityLabel,
  snapshotIntegrityTagType,
  snapshotPhaseLabel,
  snapshotPollInterval,
  snapshotStateLabel,
  snapshotStateTagType,
} from '@/features/snapshots/status';
import { formatBytes, formatCount } from '@/utils/format';

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
const selectedVolume = ref<StorageVolumeView>();
const volumeCursor = ref<string>();
const volumeCursorHistory = ref<string[]>([]);
const snapshotRequestId = ref<string>();
const createOutcome = ref<CreateSnapshotResponse>();

const versionQuery = useQuery({
  queryKey: ['system', 'version'],
  queryFn: queryApiVersion,
  staleTime: Number.POSITIVE_INFINITY,
});
const artifactQuery = useQuery({
  queryKey: computed(() => ['artifact', tenantId.value, projectId.value, artifactId.value]),
  queryFn: () => queryArtifact(tenantId.value, projectId.value, artifactId.value),
});
const artifact = computed(() => artifactQuery.data.value?.data.artifact);
const commitGraphEnabled = computed(() =>
  supportsArtifactCommitGraph(versionQuery.data.value?.data.capabilities),
);

watch(
  artifact,
  (value) => {
    if (!selectedCommitId.value) selectedCommitId.value = value?.head_commit_id ?? '';
  },
  { immediate: true },
);

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
const createdSnapshotId = computed(() => createOutcome.value?.snapshot.snapshot_id ?? '');
const deliveryQuery = useQuery({
  queryKey: computed(() => ['snapshot', tenantId.value, createdSnapshotId.value]),
  queryFn: async () => {
    const result = await querySnapshot(tenantId.value, createdSnapshotId.value);
    const item = result.data.snapshot;
    if (
      item.project_id !== projectId.value ||
      item.artifact_id !== artifactId.value ||
      item.commit_id !== selectedCommitId.value
    ) {
      throw new Error('Snapshot 与当前 Artifact Commit 不匹配');
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

watch([tenantId, projectId, artifactId], () => {
  activeStage.value = 'commit';
  selectedCommitId.value = requestedCommitId.value;
  selectedVolume.value = undefined;
  volumeCursor.value = undefined;
  volumeCursorHistory.value = [];
  snapshotRequestId.value = undefined;
  createOutcome.value = undefined;
  createMutation.reset();
});

function continueToPlacement(): void {
  if (!selectedCommitId.value) {
    ElMessage.warning('Artifact 尚无可用于 Snapshot 的 Commit');
    return;
  }
  activeStage.value = 'placement';
}

function selectVolume(volume: StorageVolumeView): void {
  if (volume.state !== 'ready') return;
  if (selectedVolume.value?.storage_volume_id !== volume.storage_volume_id) {
    snapshotRequestId.value = undefined;
    createMutation.reset();
  }
  selectedVolume.value = volume;
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

function backToCommit(): void {
  snapshotRequestId.value = undefined;
  createMutation.reset();
  activeStage.value = 'commit';
}

async function createSnapshotNow(): Promise<void> {
  if (createMutation.isPending.value) return;
  if (!selectedCommitId.value || !selectedVolume.value || selectedVolume.value.state !== 'ready') {
    ElMessage.warning('请选择 Commit 和 Ready StorageVolume');
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
    ElMessage.success(result.data.replayed ? '已返回同一创建请求' : 'Snapshot 已开始创建');
  } catch {
    // The same request identity is retained so an uncertain transport result can be retried safely.
  }
}

async function backToArtifact(): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: projectId.value, artifactId: artifactId.value },
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

function volumeStateLabel(state: StorageVolumeView['state']): string {
  return { ready: 'Ready', degraded: 'Degraded', unavailable: 'Unavailable' }[state];
}
</script>

<template>
  <div class="page snapshot-create-page">
    <PageHeading title="创建只读 Snapshot" :description="`${projectId} / ${artifactId}`">
      <template #actions>
        <el-button :icon="Back" @click="backToArtifact">返回 Artifact</el-button>
      </template>
    </PageHeading>

    <ol class="snapshot-steps" aria-label="Snapshot 创建流程">
      <li :class="{ active: activeStage === 'commit', complete: activeStage !== 'commit' }">
        <span>1</span><strong>固定 Commit</strong>
      </li>
      <li :class="{ active: activeStage === 'placement', complete: activeStage === 'delivery' }">
        <span>2</span><strong>选择 Volume</strong>
      </li>
      <li :class="{ active: activeStage === 'delivery' }">
        <span>3</span><strong>FUSE 交付</strong>
      </li>
    </ol>

    <template v-if="activeStage === 'commit'">
      <section class="content-section snapshot-form-section">
        <header class="section-heading">
          <div>
            <span>ARTIFACT VERSION</span>
            <h2>选择不可变版本</h2>
          </div>
          <DocumentCopy />
        </header>
        <ApiProblemAlert
          v-if="artifactQuery.error.value || versionQuery.error.value"
          :error="artifactQuery.error.value ?? versionQuery.error.value"
          :retrying="artifactQuery.isFetching.value || versionQuery.isFetching.value"
          @retry="artifactQuery.refetch"
        />
        <el-skeleton v-if="artifactQuery.isPending.value" :rows="5" animated />
        <template v-else-if="artifact">
          <dl class="snapshot-source">
            <div>
              <dt>Artifact</dt>
              <dd>{{ artifact.display_name }}</dd>
            </div>
            <div>
              <dt>Scope</dt>
              <dd>
                <code>{{ projectId }}/{{ artifactId }}</code>
              </dd>
            </div>
            <div class="snapshot-source__wide">
              <dt>Commit</dt>
              <dd>
                <ArtifactCommitSelect
                  v-model="selectedCommitId"
                  :tenant-id="tenantId"
                  :project-id="projectId"
                  :artifact-id="artifactId"
                  :head-commit-id="artifact.head_commit_id"
                  :allow-history="commitGraphEnabled"
                />
              </dd>
            </div>
          </dl>
          <el-alert
            v-if="!artifact.head_commit_id"
            title="空 Artifact 不能创建 Snapshot"
            type="warning"
            :closable="false"
          />
        </template>
      </section>
      <footer class="snapshot-actions">
        <span>Snapshot 始终固定到选中的不可变 Commit</span>
        <el-button type="primary" :disabled="!selectedCommitId" @click="continueToPlacement">
          选择 StorageVolume
        </el-button>
      </footer>
    </template>

    <template v-else-if="activeStage === 'placement'">
      <section class="content-section snapshot-form-section">
        <header class="section-heading">
          <div>
            <span>PLACEMENT</span>
            <h2>选择 FUSE 挂载所在 Volume</h2>
          </div>
          <Location />
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
        <el-skeleton v-if="storageVolumeQuery.isPending.value" :rows="6" animated />
        <template v-else>
          <div class="snapshot-volume-list">
            <button
              v-for="volume in volumePage"
              :key="volume.storage_volume_id"
              type="button"
              :class="{ selected: selectedVolume?.storage_volume_id === volume.storage_volume_id }"
              :disabled="volume.state !== 'ready'"
              @click="selectVolume(volume)"
            >
              <Location />
              <span
                ><strong>{{ volume.display_name }}</strong
                ><code>{{ volume.storage_volume_id }}</code></span
              >
              <span
                ><small>{{ volume.region }}</small
                ><el-tag size="small" effect="plain">{{
                  volumeStateLabel(volume.state)
                }}</el-tag></span
              >
            </button>
          </div>
          <el-empty v-if="volumePage.length === 0" description="没有可见 StorageVolume" />
          <PageCursor
            :has-previous="volumeCursorHistory.length > 0"
            :has-next="Boolean(storageVolumeQuery.data.value?.data.next_cursor)"
            :loading="storageVolumeQuery.isFetching.value"
            @previous="previousVolumePage"
            @next="nextVolumePage"
          />
        </template>
      </section>

      <section v-if="selectedVolume" class="snapshot-selection" aria-label="Snapshot 创建摘要">
        <div>
          <small>Commit</small
          ><code :title="selectedCommitId">{{ selectedCommitId.slice(0, 16) }}</code>
        </div>
        <div>
          <small>StorageVolume</small><strong>{{ selectedVolume.display_name }}</strong>
          <code>{{ selectedVolume.storage_volume_id }}</code>
        </div>
        <div><small>交付方式</small><strong>只读 FUSE</strong></div>
      </section>
      <footer class="snapshot-actions">
        <el-button @click="backToCommit">返回 Commit</el-button>
        <el-button
          type="primary"
          :icon="DocumentCopy"
          :loading="createMutation.isPending.value"
          :disabled="!selectedVolume"
          @click="createSnapshotNow"
          >创建 Snapshot</el-button
        >
      </footer>
    </template>

    <template v-else>
      <ApiProblemAlert
        v-if="deliveryQuery.error.value"
        :error="deliveryQuery.error.value"
        :retrying="deliveryQuery.isFetching.value"
        @retry="deliveryQuery.refetch"
      />
      <section v-if="deliverySnapshot" class="content-section delivery-panel">
        <div class="delivery-heading">
          <span :class="['delivery-icon', `delivery-icon--${deliverySnapshot.state}`]">
            <CircleCheck v-if="deliverySnapshot.state === 'ready'" />
            <WarningFilled v-else-if="deliverySnapshot.state === 'abnormal'" />
            <RefreshRight v-else />
          </span>
          <div>
            <small>{{ snapshotStateLabel(deliverySnapshot.state) }}</small>
            <h2>{{ snapshotPhaseLabel(deliverySnapshot.phase) }}</h2>
            <p>目标 Volume 正在提供该 Commit 的只读 FUSE 视图。</p>
          </div>
        </div>
        <el-alert
          v-if="deliverySnapshot.issue"
          :title="deliverySnapshot.issue.message"
          :description="deliverySnapshot.issue.code"
          type="error"
          :closable="false"
        />
        <dl class="delivery-facts">
          <div>
            <dt>Snapshot</dt>
            <dd>
              <code>{{ deliverySnapshot.snapshot_id }}</code>
            </dd>
          </div>
          <div>
            <dt>StorageVolume</dt>
            <dd>{{ deliverySnapshot.storage_volume_id }}</dd>
          </div>
          <div>
            <dt>Region</dt>
            <dd>{{ deliverySnapshot.region }}</dd>
          </div>
          <div>
            <dt>状态</dt>
            <dd>
              <el-tag :type="snapshotStateTagType(deliverySnapshot.state)" effect="plain">{{
                snapshotStateLabel(deliverySnapshot.state)
              }}</el-tag>
            </dd>
          </div>
          <div>
            <dt>完整性</dt>
            <dd>
              <el-tag
                :type="snapshotIntegrityTagType(deliverySnapshot.integrity.state)"
                effect="plain"
                >{{ snapshotIntegrityLabel(deliverySnapshot.integrity.state) }}</el-tag
              >
            </dd>
          </div>
          <div>
            <dt>文件</dt>
            <dd>{{ formatCount(deliverySnapshot.logical_file_count) }}</dd>
          </div>
          <div>
            <dt>逻辑大小</dt>
            <dd>{{ formatBytes(deliverySnapshot.logical_size_bytes) }}</dd>
          </div>
        </dl>
        <div class="delivery-flags">
          <el-tag v-if="createOutcome" effect="plain">
            {{ createOutcome.replayed ? '幂等重放' : '新请求' }}
          </el-tag>
          <el-tag v-if="createOutcome" effect="plain">
            {{ createOutcome.placement_reused ? '复用现有放置' : '新建交付位置' }}
          </el-tag>
          <el-tag type="success" effect="plain">只读</el-tag>
        </div>
      </section>
      <footer class="snapshot-actions">
        <span v-if="deliverySnapshot?.state === 'creating'">页面会持续刷新交付状态</span>
        <span v-else />
        <el-button type="primary" @click="openSnapshot">查看 Snapshot</el-button>
      </footer>
    </template>
  </div>
</template>

<style scoped>
.snapshot-create-page {
  max-width: 1120px;
}

.snapshot-steps {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  margin: 0;
  padding: 0;
  border: 1px solid var(--border);
  background: #fff;
  list-style: none;
}

.snapshot-steps li {
  display: flex;
  min-width: 0;
  align-items: center;
  gap: 10px;
  padding: 14px 18px;
  color: var(--muted);
  border-right: 1px solid var(--border);
}

.snapshot-steps li:last-child {
  border-right: 0;
}
.snapshot-steps li.active {
  color: var(--text);
  background: #f3f8f6;
}
.snapshot-steps li.complete {
  color: #167450;
}
.snapshot-steps li > span {
  display: grid;
  width: 26px;
  height: 26px;
  flex: 0 0 26px;
  place-items: center;
  border: 1px solid currentColor;
  border-radius: 50%;
  font-size: 12px;
}

.snapshot-form-section {
  margin-top: 16px;
}
.section-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.section-heading > div > span {
  color: var(--muted);
  font-size: 11px;
}
.section-heading h2 {
  margin: 3px 0 0;
  font-size: 18px;
}
.section-heading > svg {
  width: 24px;
  color: #167450;
}

.snapshot-source,
.delivery-facts {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1px;
  padding: 1px;
  background: var(--border);
}

.snapshot-source > div,
.delivery-facts > div {
  min-width: 0;
  padding: 14px;
  background: #fff;
}

.snapshot-source dt,
.delivery-facts dt {
  margin-bottom: 6px;
  color: var(--muted);
  font-size: 11px;
}
.snapshot-source dd,
.delivery-facts dd {
  min-width: 0;
  margin: 0;
  overflow-wrap: anywhere;
}
.snapshot-source__wide {
  grid-column: 1 / -1;
}

.snapshot-actions {
  display: flex;
  min-height: 64px;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 12px 16px;
  border: 1px solid var(--border);
  border-top: 0;
  background: #fff;
  color: var(--muted);
  font-size: 12px;
}

.snapshot-volume-list {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}
.snapshot-volume-list > button {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  min-height: 82px;
  padding: 12px;
  border: 1px solid var(--border);
  background: #fff;
  color: var(--text);
  text-align: left;
  cursor: pointer;
}
.snapshot-volume-list > button:hover,
.snapshot-volume-list > button.selected {
  border-color: #167450;
  box-shadow: inset 3px 0 #167450;
}
.snapshot-volume-list > button:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}
.snapshot-volume-list svg {
  width: 20px;
  color: #167450;
}
.snapshot-volume-list span {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 4px;
}
.snapshot-volume-list code {
  overflow: hidden;
  color: var(--muted);
  text-overflow: ellipsis;
  white-space: nowrap;
}
.snapshot-volume-list span:last-child {
  align-items: flex-end;
}

.snapshot-selection {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 1px;
  margin-top: 16px;
  padding: 1px;
  background: var(--border);
}
.snapshot-selection > div {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 5px;
  padding: 12px 14px;
  background: #fff;
}
.snapshot-selection small {
  color: var(--muted);
}
.snapshot-selection code {
  overflow: hidden;
  text-overflow: ellipsis;
}

.delivery-panel {
  margin-top: 16px;
}
.delivery-heading {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 20px;
}
.delivery-heading h2 {
  margin: 2px 0 5px;
  font-size: 22px;
}
.delivery-heading p {
  margin: 0;
  color: var(--muted);
}
.delivery-icon {
  display: grid;
  width: 54px;
  height: 54px;
  flex: 0 0 54px;
  place-items: center;
  background: #eef3f1;
  color: #7a8581;
}
.delivery-icon svg {
  width: 28px;
}
.delivery-icon--ready {
  background: #eaf6f0;
  color: #167450;
}
.delivery-icon--abnormal {
  background: #fff0ef;
  color: #c33f35;
}
.delivery-icon--creating svg {
  animation: spin 1.2s linear infinite;
}
.delivery-flags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 14px;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 720px) {
  .snapshot-steps li {
    padding: 12px 8px;
  }
  .snapshot-steps li strong {
    font-size: 12px;
  }
  .snapshot-volume-list,
  .snapshot-source,
  .delivery-facts,
  .snapshot-selection {
    grid-template-columns: 1fr;
  }
  .snapshot-source__wide {
    grid-column: auto;
  }
  .snapshot-actions {
    align-items: stretch;
    flex-direction: column;
  }
  .snapshot-actions :deep(.el-button) {
    width: 100%;
    margin: 0;
  }
}
</style>
