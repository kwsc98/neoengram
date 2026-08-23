<script setup lang="ts">
import {
  Back,
  CircleCheck,
  DocumentCopy,
  RefreshRight,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useMutation, useQuery } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { createSnapshot, queryApiVersion, queryArtifact, querySnapshot } from '@/api/operations';
import type { CreateSnapshotResponse } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import PageHeading from '@/components/PageHeading.vue';
import { supportsArtifactCommitGraph } from '@/features/capabilities';
import {
  snapshotIntegrityLabel,
  snapshotIntegrityTagType,
  snapshotPollInterval,
  snapshotStateLabel,
  snapshotStateTagType,
} from '@/features/snapshots/status';
import { formatBytes, formatCount } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const requestedCommitId = computed(() => String(route.query.commit_id ?? ''));

const selectedCommitId = ref(requestedCommitId.value);
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

const createMutation = useMutation({ mutationFn: createSnapshot });
const createdSnapshotId = computed(() => createOutcome.value?.snapshot.snapshot_id ?? '');
const snapshotQuery = useQuery({
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

watch([tenantId, projectId, artifactId], () => {
  selectedCommitId.value = requestedCommitId.value;
  snapshotRequestId.value = undefined;
  createOutcome.value = undefined;
  createMutation.reset();
});

async function createSnapshotNow(): Promise<void> {
  if (createMutation.isPending.value) return;
  if (!selectedCommitId.value) {
    ElMessage.warning('请选择 Commit');
    return;
  }

  snapshotRequestId.value ??= `snapshot-request-${globalThis.crypto.randomUUID()}`;
  try {
    const result = await createMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      commit_id: selectedCommitId.value,
      request_id: snapshotRequestId.value,
    });
    createOutcome.value = result.data;
    snapshotRequestId.value = undefined;
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
  if (!createOutcome.value?.snapshot) return;
  await router.push({
    name: 'snapshot-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      snapshotId: createOutcome.value.snapshot.snapshot_id,
    },
  });
}
</script>

<template>
  <div class="page snapshot-create-page">
    <PageHeading title="创建只读 Snapshot" :description="`${projectId} / ${artifactId}`">
      <template #actions>
        <el-button :icon="Back" @click="backToArtifact">返回 Artifact</el-button>
      </template>
    </PageHeading>

    <template v-if="!createOutcome">
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
        <el-button
          type="primary"
          :loading="createMutation.isPending.value"
          :disabled="!selectedCommitId"
          @click="createSnapshotNow"
        >
          创建 Snapshot
        </el-button>
      </footer>
    </template>

    <template v-else>
      <section class="content-section delivery-panel">
        <div class="delivery-heading">
          <span
            :class="[
              'delivery-icon',
              `delivery-icon--${snapshotQuery.data.value?.data.snapshot.state}`,
            ]"
          >
            <CircleCheck v-if="snapshotQuery.data.value?.data.snapshot.state === 'ready'" />
            <WarningFilled
              v-else-if="snapshotQuery.data.value?.data.snapshot.state === 'abnormal'"
            />
            <RefreshRight v-else />
          </span>
          <div>
            <small>{{
              snapshotStateLabel(
                snapshotQuery.data.value?.data.snapshot.state ?? createOutcome.snapshot.state,
              )
            }}</small>
            <h2>Snapshot 已创建</h2>
            <p>Snapshot 只保存逻辑 Commit 引用。请在详情页先复制到目标 Volume，再创建只读交付。</p>
          </div>
        </div>
        <el-alert
          v-if="snapshotQuery.data.value?.data.snapshot.issue"
          :title="snapshotQuery.data.value.data.snapshot.issue.message"
          :description="snapshotQuery.data.value.data.snapshot.issue.code"
          type="error"
          :closable="false"
        />
        <dl class="delivery-facts">
          <div>
            <dt>Snapshot</dt>
            <dd>
              <code>{{ createOutcome.snapshot.snapshot_id }}</code>
            </dd>
          </div>
          <div>
            <dt>状态</dt>
            <dd>
              <el-tag
                :type="
                  snapshotStateTagType(
                    snapshotQuery.data.value?.data.snapshot.state ?? createOutcome.snapshot.state,
                  )
                "
                effect="plain"
                >{{
                  snapshotStateLabel(
                    snapshotQuery.data.value?.data.snapshot.state ?? createOutcome.snapshot.state,
                  )
                }}</el-tag
              >
            </dd>
          </div>
          <div>
            <dt>完整性</dt>
            <dd>
              <el-tag
                :type="snapshotIntegrityTagType(createOutcome.snapshot.integrity.state)"
                effect="plain"
                >{{ snapshotIntegrityLabel(createOutcome.snapshot.integrity.state) }}</el-tag
              >
            </dd>
          </div>
          <div>
            <dt>Commit</dt>
            <dd>
              <code>{{ createOutcome.snapshot.commit_id }}</code>
            </dd>
          </div>
          <div>
            <dt>文件</dt>
            <dd>{{ formatCount(createOutcome.snapshot.logical_file_count) }}</dd>
          </div>
          <div>
            <dt>逻辑大小</dt>
            <dd>{{ formatBytes(createOutcome.snapshot.logical_size_bytes) }}</dd>
          </div>
        </dl>
        <div class="delivery-flags">
          <el-tag effect="plain">{{ createOutcome.replayed ? '幂等重放' : '新请求' }}</el-tag>
          <el-tag type="success" effect="plain">只读</el-tag>
        </div>
      </section>
      <footer class="snapshot-actions">
        <span v-if="snapshotQuery.data.value?.data.snapshot.state === 'creating'"
          >页面会持续刷新 Snapshot 状态</span
        >
        <span v-else>下一步在详情页处理 Replicate 和 Delivery</span>
        <el-button type="primary" @click="openSnapshot">查看 Snapshot</el-button>
      </footer>
    </template>
  </div>
</template>

<style scoped>
.snapshot-create-page {
  max-width: 1120px;
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

.delivery-mode-status {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
}
.delivery-mode-status > div {
  display: flex;
  min-width: 0;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  padding: 9px 10px;
  border: 1px solid var(--border);
  background: #fafcfb;
  font-size: 12px;
}
.delivery-mode-status > div > span:last-child {
  width: 100%;
  color: var(--muted);
  font-size: 11px;
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
  .snapshot-source,
  .delivery-facts,
  .delivery-mode-status {
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
