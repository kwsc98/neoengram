<script setup lang="ts">
import { ArrowRight, DocumentCopy, Files, Plus, RefreshRight } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  createPlayground,
  createSnapshot,
  queryArtifact,
  queryArtifactCommitDiff,
  queryArtifactCommitGraph,
  queryPlaygroundList,
  querySnapshotList,
} from '@/api/operations';
import type { CommitNode } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import StorageVolumeFilter from '@/components/StorageVolumeFilter.vue';
import {
  activePreCommitLabel,
  getActivePreCommit,
  playgroundAvailabilityLabel,
  playgroundAvailabilityTagType,
  preCommitScopeKey,
} from '@/features/precommit/prototype';
import {
  snapshotPhaseLabel,
  snapshotStateLabel,
  snapshotStateTagType,
} from '@/features/snapshots/prototype';
import { useTenantsStore } from '@/stores/tenants';
import { commitTagNames } from '@/utils/commit';
import { formatBytes, formatCount, formatTime, shortId } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const allowedTabs = ['overview', 'commits', 'playgrounds', 'snapshots'];
const initialTab = String(route.query.tab ?? 'overview');
const activeTab = ref(allowedTabs.includes(initialTab) ? initialTab : 'overview');
const commitNodes = ref<CommitNode[]>([]);
const nextCommitCursor = ref<string>();
const loadingMoreCommits = ref(false);
const selectedCommitId = ref(String(route.query.commit_id ?? ''));
const commitDetailOpen = ref(Boolean(selectedCommitId.value));
const createPlaygroundOpen = ref(false);
const createSnapshotOpen = ref(false);
const mutationError = ref('');
const playgroundForm = reactive({
  playgroundId: '',
  displayName: '',
  baseCommitId: '',
  storageVolumeId: '',
});
const snapshotForm = reactive({ commitId: '', storageVolumeId: '' });
const canCreatePlayground = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('playground.create') ?? false,
);
const canCreateSnapshot = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('snapshot.create') ?? false,
);
const createPlaygroundMutation = useMutation({ mutationFn: createPlayground });
const createSnapshotMutation = useMutation({ mutationFn: createSnapshot });

const artifactQuery = useQuery({
  queryKey: computed(() => ['artifact', tenantId.value, projectId.value, artifactId.value]),
  queryFn: () => queryArtifact(tenantId.value, projectId.value, artifactId.value),
});
const artifact = computed(() => artifactQuery.data.value?.data.artifact);

const commitQuery = useQuery({
  queryKey: computed(() => ['artifact-commits', tenantId.value, projectId.value, artifactId.value]),
  queryFn: () => queryArtifactCommitGraph(tenantId.value, projectId.value, artifactId.value),
  enabled: computed(() => activeTab.value === 'overview' || activeTab.value === 'commits'),
});
const currentCommit = computed(() => {
  const graph = commitQuery.data.value?.data.graph;
  if (!graph) return undefined;
  return graph.nodes.find((node) => node.commit_id === graph.head_commit_id) ?? graph.nodes[0];
});
const currentCommitTags = computed(() => commitTagNames(currentCommit.value?.tag_names ?? []));
const commitDiffQuery = useQuery({
  queryKey: computed(() => [
    'artifact-commit-diff',
    tenantId.value,
    projectId.value,
    artifactId.value,
    selectedCommitId.value,
  ]),
  queryFn: () =>
    queryArtifactCommitDiff(
      tenantId.value,
      projectId.value,
      artifactId.value,
      selectedCommitId.value,
    ),
  enabled: computed(() => commitDetailOpen.value && Boolean(selectedCommitId.value)),
});
const commitDiff = computed(() => commitDiffQuery.data.value?.data.diff);
const playgroundQuery = useQuery({
  queryKey: computed(() => [
    'playgrounds',
    tenantId.value,
    projectId.value,
    artifactId.value,
    'artifact-detail',
  ]),
  queryFn: () =>
    queryPlaygroundList({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      page_size: 100,
    }),
  enabled: computed(() => activeTab.value === 'overview' || activeTab.value === 'playgrounds'),
});
const snapshotQuery = useQuery({
  queryKey: computed(() => [
    'snapshots',
    tenantId.value,
    projectId.value,
    artifactId.value,
    'artifact-detail',
  ]),
  queryFn: () =>
    querySnapshotList({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      page_size: 100,
    }),
  enabled: computed(() => activeTab.value === 'overview' || activeTab.value === 'snapshots'),
});
const artifactPlaygrounds = computed(() => playgroundQuery.data.value?.data.items ?? []);
const artifactSnapshots = computed(() => snapshotQuery.data.value?.data.items ?? []);
const availableRegions = computed(() => [
  ...new Set(artifactSnapshots.value.map((snapshot) => snapshot.region)),
]);

watch(
  () => commitQuery.data.value,
  (result) => {
    if (!result) return;
    commitNodes.value = [...result.data.graph.nodes];
    nextCommitCursor.value = result.data.graph.next_cursor;
  },
  { immediate: true },
);

watch(
  () => route.query.tab,
  (tab) => {
    const value = String(tab ?? 'overview');
    activeTab.value = allowedTabs.includes(value) ? value : 'overview';
  },
);

watch(
  () => route.query.commit_id,
  (commitId) => {
    selectedCommitId.value = String(commitId ?? '');
    commitDetailOpen.value = Boolean(selectedCommitId.value);
  },
);

async function changeTab(tab: string | number): Promise<void> {
  const value = String(tab);
  await router.replace({ query: value === 'overview' ? {} : { tab: value } });
}

async function showCommitDetail(commitId: string): Promise<void> {
  selectedCommitId.value = commitId;
  commitDetailOpen.value = true;
  await router.replace({ query: { tab: 'commits', commit_id: commitId } });
}

async function closeCommitDetail(): Promise<void> {
  if (!route.query.commit_id) return;
  await router.replace({ query: { tab: 'commits' } });
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

async function loadMoreCommits(): Promise<void> {
  if (!nextCommitCursor.value) return;
  loadingMoreCommits.value = true;
  try {
    const result = await queryArtifactCommitGraph(
      tenantId.value,
      projectId.value,
      artifactId.value,
      nextCommitCursor.value,
    );
    commitNodes.value.push(...result.data.graph.nodes);
    nextCommitCursor.value = result.data.graph.next_cursor;
  } finally {
    loadingMoreCommits.value = false;
  }
}

async function openPlayground(playgroundId: string): Promise<void> {
  await router.push({
    name: 'playground-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      playgroundId,
    },
  });
}

async function openSnapshot(snapshotId: string): Promise<void> {
  await router.push({
    name: 'snapshot-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      snapshotId,
    },
  });
}

function relationPreCommitKey(playgroundId: string): string {
  return preCommitScopeKey(tenantId.value, projectId.value, artifactId.value, playgroundId);
}

async function loadCommitChoices(): Promise<void> {
  const result = await queryArtifactCommitGraph(tenantId.value, projectId.value, artifactId.value);
  commitNodes.value = [...result.data.graph.nodes];
  nextCommitCursor.value = result.data.graph.next_cursor;
}

async function showCreatePlayground(): Promise<void> {
  mutationError.value = '';
  playgroundForm.playgroundId = '';
  playgroundForm.displayName = '';
  playgroundForm.storageVolumeId = '';
  await loadCommitChoices();
  playgroundForm.baseCommitId = artifact.value?.head_commit_id ?? '';
  createPlaygroundOpen.value = true;
}

async function submitPlayground(): Promise<void> {
  mutationError.value = '';
  const resourceId = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
  if (
    !resourceId.test(playgroundForm.playgroundId) ||
    !playgroundForm.displayName.trim() ||
    !playgroundForm.storageVolumeId
  ) {
    mutationError.value = '请输入合法 Playground ID、名称并选择 StorageVolume';
    return;
  }
  try {
    const result = await createPlaygroundMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      playground_id: playgroundForm.playgroundId,
      storage_volume_id: playgroundForm.storageVolumeId,
      display_name: playgroundForm.displayName.trim(),
      ...(playgroundForm.baseCommitId ? { base_commit_id: playgroundForm.baseCommitId } : {}),
    });
    createPlaygroundOpen.value = false;
    await queryClient.invalidateQueries({ queryKey: ['playgrounds', tenantId.value] });
    ElMessage.success(result.data.replayed ? '已返回现有 Playground' : 'Playground 已创建');
    await openPlayground(result.data.playground.playground_id);
  } catch (error) {
    mutationError.value = error instanceof Error ? error.message : '创建 Playground 失败';
  }
}

async function showCreateSnapshot(): Promise<void> {
  mutationError.value = '';
  snapshotForm.storageVolumeId = '';
  await loadCommitChoices();
  snapshotForm.commitId = artifact.value?.head_commit_id ?? commitNodes.value[0]?.commit_id ?? '';
  createSnapshotOpen.value = true;
}

async function submitSnapshot(): Promise<void> {
  mutationError.value = '';
  if (!snapshotForm.commitId || !snapshotForm.storageVolumeId) {
    mutationError.value = '请选择 Commit 和 StorageVolume';
    return;
  }
  try {
    const result = await createSnapshotMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      commit_id: snapshotForm.commitId,
      storage_volume_id: snapshotForm.storageVolumeId,
      snapshot_request_id: `snapshot-request-${globalThis.crypto.randomUUID()}`,
    });
    createSnapshotOpen.value = false;
    await queryClient.invalidateQueries({ queryKey: ['snapshots', tenantId.value] });
    ElMessage.success(
      result.data.replayed
        ? '已返回原 Snapshot'
        : result.data.placement_reused
          ? '该 Commit 在此存储已有 Snapshot'
          : 'Snapshot 已开始创建',
    );
    await openSnapshot(result.data.snapshot.snapshot_id);
  } catch (error) {
    mutationError.value = error instanceof Error ? error.message : '创建 Snapshot 失败';
  }
}
</script>

<template>
  <div class="page">
    <PageHeading
      :title="artifact?.display_name ?? artifactId"
      :description="`${projectId} / ${artifactId}`"
    >
      <template #actions>
        <el-button v-if="canCreatePlayground" :icon="Plus" @click="showCreatePlayground">
          创建 Playground
        </el-button>
        <el-button
          v-if="canCreateSnapshot"
          type="primary"
          :icon="DocumentCopy"
          @click="showCreateSnapshot"
        >
          创建 Snapshot
        </el-button>
        <el-button
          :icon="RefreshRight"
          :loading="artifactQuery.isFetching.value"
          @click="artifactQuery.refetch"
        >
          刷新
        </el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="artifactQuery.error.value"
      :error="artifactQuery.error.value"
      :retrying="artifactQuery.isFetching.value"
      @retry="artifactQuery.refetch"
    />

    <section v-if="artifact" class="content-section resource-detail-shell">
      <el-tabs :model-value="activeTab" @tab-change="changeTab">
        <el-tab-pane label="概览" name="overview">
          <dl class="definition-grid definition-grid--scope">
            <div>
              <dt>Tenant</dt>
              <dd>{{ artifact.tenant_id }}</dd>
            </div>
            <div>
              <dt>Project</dt>
              <dd>{{ artifact.project_id }}</dd>
            </div>
            <div>
              <dt>Artifact ID</dt>
              <dd>
                <code>{{ artifact.artifact_id }}</code>
              </dd>
            </div>
            <div>
              <dt>Resource version</dt>
              <dd>{{ artifact.resource_version }}</dd>
            </div>
            <div>
              <dt>初始化方式</dt>
              <dd>
                {{ artifact.initialization.mode === 'derived' ? '从 Commit 派生' : '空 Artifact' }}
              </dd>
            </div>
            <div v-if="artifact.initialization.mode === 'derived'" class="definition-grid__wide">
              <dt>来源血缘</dt>
              <dd class="commit-identity">
                <span v-if="artifact.initialization.mode === 'derived'">
                  <small>{{ artifact.initialization.source_project_id }}</small>
                  <strong>{{ artifact.initialization.source_artifact_id }}</strong>
                </span>
                <code>
                  {{
                    artifact.initialization.mode === 'derived'
                      ? artifact.initialization.source_commit_id
                      : '—'
                  }}
                </code>
              </dd>
            </div>
            <div class="definition-grid__wide">
              <dt>当前 Commit</dt>
              <dd v-if="currentCommit" class="commit-identity">
                <code>{{ currentCommit.commit_id }}</code>
                <span>{{ currentCommit.message }}</span>
              </dd>
              <dd v-else>尚无 Commit</dd>
            </div>
            <div class="definition-grid__wide">
              <dt>Tags</dt>
              <dd class="tag-list">
                <el-tag v-for="tagName in currentCommitTags" :key="tagName" effect="plain">
                  {{ tagName }}
                </el-tag>
                <span v-if="currentCommitTags.length === 0">暂无 Tag</span>
              </dd>
            </div>
            <div>
              <dt>Playgrounds</dt>
              <dd>{{ artifactPlaygrounds.length }}</dd>
            </div>
            <div>
              <dt>Snapshots</dt>
              <dd>{{ artifactSnapshots.length }}</dd>
            </div>
            <div class="definition-grid__wide">
              <dt>可用区域</dt>
              <dd class="tag-list">
                <el-tag v-for="region in availableRegions" :key="region" effect="plain">
                  {{ region }}
                </el-tag>
                <span v-if="availableRegions.length === 0">尚无区域交付</span>
              </dd>
            </div>
            <div>
              <dt>创建时间</dt>
              <dd>{{ formatTime(artifact.created_at_unix_ms) }}</dd>
            </div>
            <div>
              <dt>更新时间</dt>
              <dd>{{ formatTime(artifact.updated_at_unix_ms) }}</dd>
            </div>
            <div class="definition-grid__wide">
              <dt>描述</dt>
              <dd>{{ artifact.description ?? '—' }}</dd>
            </div>
          </dl>
        </el-tab-pane>

        <el-tab-pane label="版本" name="commits">
          <ApiProblemAlert
            v-if="commitQuery.error.value"
            :error="commitQuery.error.value"
            :retrying="commitQuery.isFetching.value"
            @retry="commitQuery.refetch"
          />
          <el-skeleton v-if="commitQuery.isPending.value" :rows="6" animated />
          <template v-else-if="commitQuery.data.value">
            <div class="commit-summary">
              <span>{{ commitNodes.length }} 个 Commit</span>
              <small>Tags 用于标记可识别的固定版本</small>
            </div>
            <ol class="commit-tree" aria-label="Commit 图">
              <li v-for="node in commitNodes" :key="node.commit_id" class="commit-node">
                <span class="commit-node__dot" />
                <div class="commit-node__body">
                  <div class="commit-node__heading">
                    <strong>{{ node.message }}</strong>
                    <span class="commit-node__actions">
                      <time>{{ formatTime(node.created_at_unix_ms) }}</time>
                      <el-button
                        text
                        type="primary"
                        :icon="Files"
                        @click="showCommitDetail(node.commit_id)"
                      >
                        详情与 Diff
                      </el-button>
                    </span>
                  </div>
                  <p v-if="node.description" class="commit-node__description">
                    {{ node.description }}
                  </p>
                  <div class="commit-node__meta">
                    <code>{{ node.commit_id }}</code>
                    <span v-if="node.parent_commit_id">
                      parent <code>{{ node.parent_commit_id }}</code>
                    </span>
                    <el-tag
                      v-for="tagName in commitTagNames(node.tag_names)"
                      :key="tagName"
                      size="small"
                      effect="plain"
                    >
                      {{ tagName }}
                    </el-tag>
                  </div>
                </div>
              </li>
            </ol>
            <el-button
              v-if="nextCommitCursor"
              :loading="loadingMoreCommits"
              @click="loadMoreCommits"
            >
              加载更多历史
            </el-button>
          </template>
        </el-tab-pane>

        <el-tab-pane label="工作区" name="playgrounds">
          <ApiProblemAlert
            v-if="playgroundQuery.error.value"
            :error="playgroundQuery.error.value"
            :retrying="playgroundQuery.isFetching.value"
            @retry="playgroundQuery.refetch"
          />
          <el-skeleton v-if="playgroundQuery.isPending.value" :rows="5" animated />
          <el-empty
            v-else-if="!playgroundQuery.data.value?.data.items.length"
            description="此 Artifact 暂无 Playground"
          />
          <div v-else class="relation-list">
            <button
              v-for="playground in playgroundQuery.data.value?.data.items"
              :key="playground.playground_id"
              type="button"
              @click="openPlayground(playground.playground_id)"
            >
              <span>
                <strong>{{ playground.display_name }}</strong>
                <code>{{ playground.playground_id }}</code>
              </span>
              <span class="relation-list__aside">
                <small>{{ playground.region }}</small>
                <el-tag :type="playgroundAvailabilityTagType(playground.state)" effect="plain">{{
                  playgroundAvailabilityLabel(playground.state)
                }}</el-tag
                ><el-tag
                  v-if="getActivePreCommit(relationPreCommitKey(playground.playground_id))"
                  type="warning"
                  effect="plain"
                  >Pre-commit ·
                  {{
                    activePreCommitLabel(relationPreCommitKey(playground.playground_id))
                  }} </el-tag
                ><ArrowRight />
              </span>
            </button>
          </div>
        </el-tab-pane>

        <el-tab-pane label="快照" name="snapshots">
          <ApiProblemAlert
            v-if="snapshotQuery.error.value"
            :error="snapshotQuery.error.value"
            :retrying="snapshotQuery.isFetching.value"
            @retry="snapshotQuery.refetch"
          />
          <el-skeleton v-if="snapshotQuery.isPending.value" :rows="5" animated />
          <el-empty
            v-else-if="!snapshotQuery.data.value?.data.items.length"
            description="此 Artifact 暂无 Snapshot"
          />
          <div v-else class="relation-list">
            <button
              v-for="snapshot in snapshotQuery.data.value?.data.items"
              :key="snapshot.snapshot_id"
              type="button"
              @click="openSnapshot(snapshot.snapshot_id)"
            >
              <span>
                <strong>{{ snapshot.message }}</strong>
                <code>{{ snapshot.snapshot_id }}</code>
                <small>Commit {{ snapshot.commit_id }}</small>
              </span>
              <span class="relation-list__aside">
                <small>
                  {{ snapshot.region }} · {{ formatCount(snapshot.logical_file_count) }} files ·
                  {{ formatBytes(snapshot.logical_size_bytes) }}
                </small>
                <el-tag :type="snapshotStateTagType(snapshot.state)" effect="plain">
                  {{ snapshotStateLabel(snapshot.state) }} ·
                  {{ snapshotPhaseLabel(snapshot.phase) }}
                </el-tag>
                <ArrowRight />
              </span>
            </button>
          </div>
        </el-tab-pane>
      </el-tabs>
    </section>

    <div v-else-if="artifactQuery.isPending.value" class="page-loading">
      <el-skeleton :rows="8" animated />
    </div>

    <el-dialog
      v-model="createPlaygroundOpen"
      title="创建 Playground"
      width="min(560px, calc(100vw - 32px))"
    >
      <ApiProblemAlert
        v-if="createPlaygroundMutation.error.value"
        :error="createPlaygroundMutation.error.value"
      />
      <el-alert v-if="mutationError" :title="mutationError" type="error" :closable="false" />
      <el-form label-position="top" class="dialog-form">
        <el-form-item label="Playground ID">
          <el-input v-model="playgroundForm.playgroundId" placeholder="review-july" />
        </el-form-item>
        <el-form-item label="名称">
          <el-input v-model="playgroundForm.displayName" placeholder="七月复核" />
        </el-form-item>
        <el-form-item label="StorageVolume" required>
          <StorageVolumeFilter v-model="playgroundForm.storageVolumeId" :tenant-id="tenantId" />
        </el-form-item>
        <el-form-item label="Base Commit">
          <el-select v-model="playgroundForm.baseCommitId" clearable placeholder="空 Playground">
            <el-option
              v-for="node in commitNodes"
              :key="node.commit_id"
              :label="`${node.message} · ${shortId(node.commit_id, 14)}`"
              :value="node.commit_id"
            />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createPlaygroundOpen = false">取消</el-button>
        <el-button
          type="primary"
          :loading="createPlaygroundMutation.isPending.value"
          @click="submitPlayground"
        >
          创建 Playground
        </el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="createSnapshotOpen"
      title="创建 Snapshot"
      width="min(560px, calc(100vw - 32px))"
    >
      <ApiProblemAlert
        v-if="createSnapshotMutation.error.value"
        :error="createSnapshotMutation.error.value"
      />
      <el-alert v-if="mutationError" :title="mutationError" type="error" :closable="false" />
      <el-form label-position="top" class="dialog-form">
        <el-form-item label="StorageVolume" required>
          <StorageVolumeFilter v-model="snapshotForm.storageVolumeId" :tenant-id="tenantId" />
        </el-form-item>
        <el-form-item label="固定到 Commit">
          <el-select v-model="snapshotForm.commitId" placeholder="选择 Commit">
            <el-option
              v-for="node in commitNodes"
              :key="node.commit_id"
              :label="`${node.message} · ${shortId(node.commit_id, 14)}`"
              :value="node.commit_id"
            />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createSnapshotOpen = false">取消</el-button>
        <el-button
          type="primary"
          :loading="createSnapshotMutation.isPending.value"
          @click="submitSnapshot"
        >
          创建 Snapshot
        </el-button>
      </template>
    </el-dialog>

    <el-drawer
      v-model="commitDetailOpen"
      title="Commit 详情"
      size="min(720px, 100vw)"
      @closed="closeCommitDetail"
    >
      <ApiProblemAlert
        v-if="commitDiffQuery.error.value"
        :error="commitDiffQuery.error.value"
        :retrying="commitDiffQuery.isFetching.value"
        @retry="commitDiffQuery.refetch"
      />
      <el-skeleton v-if="commitDiffQuery.isPending.value" :rows="10" animated />
      <template v-else-if="commitDiff">
        <section class="commit-detail-section">
          <div class="section-heading section-heading--inline">
            <div>
              <h2>{{ commitDiff.target_commit.message }}</h2>
              <code>{{ commitDiff.target_commit.commit_id }}</code>
            </div>
          </div>
          <dl class="definition-grid definition-grid--scope">
            <div>
              <dt>创建时间</dt>
              <dd>{{ formatTime(commitDiff.target_commit.created_at_unix_ms) }}</dd>
            </div>
            <div>
              <dt>Parent</dt>
              <dd>
                <code>{{ commitDiff.target_commit.parent_commit_id ?? '—' }}</code>
              </dd>
            </div>
            <div class="definition-grid__wide">
              <dt>Tags</dt>
              <dd class="tag-list">
                <el-tag
                  v-for="tagName in commitTagNames(commitDiff.target_commit.tag_names)"
                  :key="tagName"
                  effect="plain"
                >
                  {{ tagName }}
                </el-tag>
                <span v-if="commitTagNames(commitDiff.target_commit.tag_names).length === 0">
                  暂无 Tag
                </span>
              </dd>
            </div>
            <div class="definition-grid__wide">
              <dt>详细描述</dt>
              <dd>{{ commitDiff.target_commit.description ?? '—' }}</dd>
            </div>
          </dl>
        </section>

        <section class="commit-detail-section commit-parent-section">
          <div class="section-heading section-heading--inline">
            <div>
              <h2>父 Commit</h2>
              <p v-if="commitDiff.base_commit">{{ commitDiff.base_commit.message }}</p>
              <p v-else>根 Commit，无父版本</p>
            </div>
            <el-button
              v-if="commitDiff.base_commit"
              text
              type="primary"
              :icon="ArrowRight"
              @click="showCommitDetail(commitDiff.base_commit.commit_id)"
            >
              查看父 Commit
            </el-button>
          </div>
          <dl v-if="commitDiff.base_commit" class="definition-grid definition-grid--scope">
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
              <dt>详细描述</dt>
              <dd>{{ commitDiff.base_commit.description ?? '—' }}</dd>
            </div>
          </dl>
        </section>

        <section class="commit-detail-section">
          <div class="section-heading">
            <h2>文件 Diff</h2>
          </div>
          <div class="diff-summary">
            <div>
              <span>新增</span><strong>{{ formatCount(commitDiff.summary.files_added) }}</strong>
            </div>
            <div>
              <span>修改</span><strong>{{ formatCount(commitDiff.summary.files_modified) }}</strong>
            </div>
            <div>
              <span>删除</span><strong>{{ formatCount(commitDiff.summary.files_deleted) }}</strong>
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
            <div v-for="change in commitDiff.changes" :key="`${change.change_type}:${change.path}`">
              <el-tag :type="diffTagType(change.change_type)" effect="plain">
                {{ diffTypeLabel(change.change_type) }}
              </el-tag>
              <span class="diff-list__path">
                <code>{{ change.path }}</code>
                <small v-if="change.previous_path">原路径 {{ change.previous_path }}</small>
              </span>
              <span class="diff-list__size">
                {{ formatBytes(change.old_size_bytes) }} → {{ formatBytes(change.new_size_bytes) }}
              </span>
            </div>
          </div>
        </section>
      </template>
    </el-drawer>
  </div>
</template>
