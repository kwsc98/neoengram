<script setup lang="ts">
import { ArrowRight, DocumentCopy, Plus, RefreshRight } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  createWorkspace,
  queryApiVersion,
  queryArtifact,
  queryArtifactCommitGraph,
  queryWorkspaceList,
  querySnapshotList,
} from '@/api/operations';
import type { CommitNode } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import ArtifactCommitTree from '@/components/ArtifactCommitTree.vue';
import PageHeading from '@/components/PageHeading.vue';
import StorageVolumeFilter from '@/components/StorageVolumeFilter.vue';
import {
  supportsArtifactCommitGraph,
  supportsWorkspaceMaterialize,
  supportsSnapshotMaterialize,
} from '@/features/capabilities';
import {
  workspaceLifecycleLabel,
  workspaceLifecycleTagType,
  workspaceStorageAvailability,
  workspaceStorageAvailabilityLabel,
  workspaceStorageAvailabilityTagType,
} from '@/features/precommit/status';
import { snapshotStateLabel, snapshotStateTagType } from '@/features/snapshots/status';
import { useTenantsStore } from '@/stores/tenants';
import { commitDataLayoutLabel, commitTagNames } from '@/utils/commit';
import { buildCommitTree } from '@/utils/commit-tree';
import { formatBytes, formatCount, formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const versionQuery = useQuery({
  queryKey: ['system', 'version'],
  queryFn: queryApiVersion,
  staleTime: Number.POSITIVE_INFINITY,
});
const artifactCommitGraphEnabled = computed(() =>
  supportsArtifactCommitGraph(versionQuery.data.value?.data.capabilities),
);
const workspaceMaterializeEnabled = computed(() =>
  supportsWorkspaceMaterialize(versionQuery.data.value?.data.capabilities),
);
const snapshotMaterializeEnabled = computed(() =>
  supportsSnapshotMaterialize(versionQuery.data.value?.data.capabilities),
);
const allowedTabs = computed(() => [
  'overview',
  ...(artifactCommitGraphEnabled.value ? ['commits'] : []),
  'workspaces',
  ...(snapshotMaterializeEnabled.value ? ['snapshots'] : []),
]);
const activeTab = ref('overview');
const commitNodes = ref<CommitNode[]>([]);
const nextCommitCursor = ref<string>();
const loadingMoreCommits = ref(false);
const loadMoreCommitsError = ref<unknown>();
const createWorkspaceOpen = ref(false);
const mutationError = ref('');
const workspaceForm = reactive({
  workspaceId: '',
  displayName: '',
  baseCommitId: '',
  storageVolumeId: '',
});
const canCreateSnapshot = computed(
  () =>
    snapshotMaterializeEnabled.value &&
    Boolean(artifact.value?.head_commit_id) &&
    (tenants.byId(tenantId.value)?.permissions.includes('snapshot.create') ?? false),
);
const createWorkspaceMutation = useMutation({ mutationFn: createWorkspace });

const artifactQuery = useQuery({
  queryKey: computed(() => ['artifact', tenantId.value, projectId.value, artifactId.value]),
  queryFn: () => queryArtifact(tenantId.value, projectId.value, artifactId.value),
});
const artifact = computed(() => artifactQuery.data.value?.data.artifact);
const artifactScopeKey = computed(() =>
  [tenantId.value, projectId.value, artifactId.value].join('\u0000'),
);
let commitDataEpoch = 0;
const canCreateWorkspace = computed(
  () =>
    Boolean(artifact.value) &&
    (tenants.byId(tenantId.value)?.permissions.includes('workspace.create') ?? false) &&
    workspaceMaterializeEnabled.value,
);

const commitQuery = useQuery({
  queryKey: computed(() => ['artifact-commits', tenantId.value, projectId.value, artifactId.value]),
  queryFn: () => queryArtifactCommitGraph(tenantId.value, projectId.value, artifactId.value),
  enabled: computed(
    () =>
      artifactCommitGraphEnabled.value &&
      (activeTab.value === 'overview' || activeTab.value === 'commits'),
  ),
});
const currentCommit = computed(() => {
  const graph = commitQuery.data.value?.data.graph;
  if (!graph) return undefined;
  return commitNodes.value.find((node) => node.commit_id === graph.head_commit_id);
});
const currentCommitTags = computed(() => commitTagNames(currentCommit.value?.tag_names ?? []));
const commitTree = computed(() => buildCommitTree(commitNodes.value));
const workspaceQuery = useQuery({
  queryKey: computed(() => [
    'workspaces',
    tenantId.value,
    projectId.value,
    artifactId.value,
    'artifact-detail',
  ]),
  queryFn: () =>
    queryWorkspaceList({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      page_size: 100,
    }),
  enabled: computed(() => activeTab.value === 'overview' || activeTab.value === 'workspaces'),
  refetchInterval: (query) =>
    query.state.data?.data.items.some((workspace) => workspace.state === 'creating')
      ? 1_000
      : 5_000,
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
  enabled: computed(
    () =>
      snapshotMaterializeEnabled.value &&
      (activeTab.value === 'overview' || activeTab.value === 'snapshots'),
  ),
  refetchInterval: (query) =>
    query.state.data?.data.items.some((snapshot) => snapshot.state === 'creating') ? 1000 : false,
});
const artifactWorkspaces = computed(() => workspaceQuery.data.value?.data.items ?? []);
const artifactSnapshots = computed(() => snapshotQuery.data.value?.data.items ?? []);
const detailRefreshing = computed(
  () =>
    artifactQuery.isFetching.value ||
    commitQuery.isFetching.value ||
    workspaceQuery.isFetching.value ||
    snapshotQuery.isFetching.value,
);

watch(
  () => commitQuery.data.value,
  (result) => {
    if (!result) return;
    commitDataEpoch += 1;
    commitNodes.value = [...result.data.graph.nodes];
    nextCommitCursor.value = result.data.graph.next_cursor;
    loadingMoreCommits.value = false;
    loadMoreCommitsError.value = undefined;
  },
  { immediate: true },
);

watch(
  [() => route.query.tab, () => allowedTabs.value.join(',')],
  ([tab]) => {
    const value = String(tab ?? 'overview');
    activeTab.value = allowedTabs.value.includes(value) ? value : 'overview';
  },
  { immediate: true },
);

watch(artifactScopeKey, () => {
  commitDataEpoch += 1;
  commitNodes.value = [];
  nextCommitCursor.value = undefined;
  loadingMoreCommits.value = false;
  loadMoreCommitsError.value = undefined;
  createWorkspaceOpen.value = false;
  mutationError.value = '';
  createWorkspaceMutation.reset();
});

async function changeTab(tab: string | number): Promise<void> {
  const value = String(tab);
  await router.replace({ query: value === 'overview' ? {} : { tab: value } });
}

async function showCommitDetail(commitId: string): Promise<void> {
  await router.push({
    name: 'commit-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      commitId,
    },
  });
}

async function loadMoreCommits(): Promise<void> {
  const requestCursor = nextCommitCursor.value;
  if (!requestCursor || loadingMoreCommits.value) return;
  const requestScopeKey = artifactScopeKey.value;
  const requestEpoch = commitDataEpoch;
  const isCurrentRequest = () =>
    artifactScopeKey.value === requestScopeKey && commitDataEpoch === requestEpoch;
  loadingMoreCommits.value = true;
  loadMoreCommitsError.value = undefined;
  try {
    const result = await queryArtifactCommitGraph(
      tenantId.value,
      projectId.value,
      artifactId.value,
      requestCursor,
    );
    if (!isCurrentRequest() || nextCommitCursor.value !== requestCursor) return;
    const commitsById = new Map(commitNodes.value.map((commit) => [commit.commit_id, commit]));
    for (const commit of result.data.graph.nodes) commitsById.set(commit.commit_id, commit);
    commitNodes.value = [...commitsById.values()];
    nextCommitCursor.value = result.data.graph.next_cursor;
  } catch (error) {
    if (isCurrentRequest()) loadMoreCommitsError.value = error;
  } finally {
    if (isCurrentRequest()) loadingMoreCommits.value = false;
  }
}

async function refreshArtifactDetail(): Promise<void> {
  const requests: Promise<unknown>[] = [artifactQuery.refetch(), workspaceQuery.refetch()];
  if (artifactCommitGraphEnabled.value) requests.push(commitQuery.refetch());
  if (snapshotMaterializeEnabled.value) requests.push(snapshotQuery.refetch());
  await Promise.allSettled(requests);
}

async function openWorkspace(workspaceId: string): Promise<void> {
  await router.push({
    name: 'workspace-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      workspaceId,
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

function showCreateWorkspace(): void {
  mutationError.value = '';
  workspaceForm.workspaceId = '';
  workspaceForm.displayName = '';
  workspaceForm.storageVolumeId = '';
  workspaceForm.baseCommitId = artifact.value?.head_commit_id ?? '';
  createWorkspaceOpen.value = true;
}

async function submitWorkspace(): Promise<void> {
  mutationError.value = '';
  const resourceId = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
  if (
    !resourceId.test(workspaceForm.workspaceId) ||
    !workspaceForm.displayName.trim() ||
    !workspaceForm.storageVolumeId
  ) {
    mutationError.value = '请输入合法 Workspace ID、名称并选择 StorageVolume';
    return;
  }
  try {
    const result = await createWorkspaceMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      workspace_id: workspaceForm.workspaceId,
      storage_volume_id: workspaceForm.storageVolumeId,
      display_name: workspaceForm.displayName.trim(),
      ...(workspaceForm.baseCommitId ? { base_commit_id: workspaceForm.baseCommitId } : {}),
    });
    createWorkspaceOpen.value = false;
    await queryClient.invalidateQueries({ queryKey: ['workspaces', tenantId.value] });
    ElMessage.success(result.data.request_replayed ? '已返回现有 Workspace' : 'Workspace 已创建');
    await openWorkspace(result.data.workspace.workspace_id);
  } catch (error) {
    mutationError.value = error instanceof Error ? error.message : '创建 Workspace 失败';
  }
}

async function showCreateSnapshot(): Promise<void> {
  await router.push({
    name: 'snapshot-create',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
    },
    ...(artifact.value?.head_commit_id
      ? { query: { commit_id: artifact.value.head_commit_id } }
      : {}),
  });
}
</script>

<template>
  <div class="page">
    <PageHeading
      :title="artifact?.display_name ?? artifactId"
      :description="`${projectId} / ${artifactId}`"
    >
      <template #actions>
        <el-button v-if="canCreateWorkspace" :icon="Plus" @click="showCreateWorkspace">
          创建 Workspace
        </el-button>
        <el-button
          v-if="canCreateSnapshot"
          type="primary"
          :icon="DocumentCopy"
          @click="showCreateSnapshot"
        >
          创建 Snapshot
        </el-button>
        <el-button :icon="RefreshRight" :loading="detailRefreshing" @click="refreshArtifactDetail">
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
              <dd v-if="artifact.head_commit_id" class="commit-identity">
                <code>{{ artifact.head_commit_id }}</code>
                <span v-if="currentCommit">{{ currentCommit.message }}</span>
                <el-tag
                  v-if="artifactCommitGraphEnabled"
                  size="small"
                  type="success"
                  effect="plain"
                >
                  默认基线
                </el-tag>
                <el-tag v-if="currentCommit" size="small" effect="plain">
                  归档：{{ commitDataLayoutLabel(currentCommit.data_layout) }}
                </el-tag>
              </dd>
              <dd v-else>尚无 Commit</dd>
            </div>
            <div v-if="artifactCommitGraphEnabled" class="definition-grid__wide">
              <dt>提交图谱</dt>
              <dd v-if="commitQuery.isPending.value" class="commit-overview-summary">
                正在加载提交图谱
              </dd>
              <dd v-else-if="commitQuery.error.value" class="commit-overview-summary">
                <span>提交图谱加载失败</span>
                <el-button text type="primary" @click="commitQuery.refetch">重试</el-button>
              </dd>
              <dd v-else-if="commitQuery.data.value" class="commit-overview-summary">
                <span>
                  <strong>{{ commitTree.loadedCount }}</strong>
                  已加载 Commit
                </span>
                <span>
                  <strong>{{ commitTree.tipCount }}</strong>
                  {{ nextCommitCursor ? '已加载分支末端' : '分支末端' }}
                </span>
                <el-tag v-if="nextCommitCursor" size="small" type="warning" effect="plain">
                  部分图谱
                </el-tag>
                <el-tag v-else size="small" type="success" effect="plain">完整图谱</el-tag>
              </dd>
            </div>
            <div v-if="artifactCommitGraphEnabled" class="definition-grid__wide">
              <dt>Tags</dt>
              <dd v-if="commitQuery.isPending.value">正在加载</dd>
              <dd v-else-if="commitQuery.error.value">提交图谱加载失败</dd>
              <dd v-else class="tag-list">
                <el-tag v-for="tagName in currentCommitTags" :key="tagName" effect="plain">
                  {{ tagName }}
                </el-tag>
                <span v-if="currentCommitTags.length === 0">暂无 Tag</span>
              </dd>
            </div>
            <div>
              <dt>Workspaces</dt>
              <dd>{{ artifactWorkspaces.length }}</dd>
            </div>
            <div v-if="snapshotMaterializeEnabled">
              <dt>Snapshots</dt>
              <dd>{{ artifactSnapshots.length }}</dd>
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

        <el-tab-pane v-if="artifactCommitGraphEnabled" name="commits">
          <template #label>
            <span class="commit-tab-label">
              版本
              <small v-if="commitQuery.data.value">{{ commitTree.loadedCount }}</small>
            </span>
          </template>
          <ApiProblemAlert
            v-if="commitQuery.error.value"
            :error="commitQuery.error.value"
            :retrying="commitQuery.isFetching.value"
            @retry="commitQuery.refetch"
          />
          <el-skeleton v-if="commitQuery.isPending.value" :rows="6" animated />
          <template v-else-if="commitQuery.data.value">
            <ApiProblemAlert v-if="loadMoreCommitsError" :error="loadMoreCommitsError" />
            <div class="commit-summary">
              <div class="commit-summary__metrics">
                <span>
                  <strong>{{ commitTree.loadedCount }}</strong>
                  <small>已加载 Commit</small>
                </span>
                <span>
                  <strong>{{ commitTree.tipCount }}</strong>
                  <small>{{ nextCommitCursor ? '已加载分支末端' : '分支末端' }}</small>
                </span>
              </div>
              <div class="commit-summary__state">
                <span v-if="artifact.head_commit_id">
                  <small>默认 HEAD</small>
                  <code>{{ artifact.head_commit_id }}</code>
                </span>
                <el-tag v-if="nextCommitCursor" type="warning" effect="plain">部分图谱</el-tag>
                <el-tag v-else type="success" effect="plain">完整图谱</el-tag>
              </div>
            </div>
            <el-empty v-if="commitTree.loadedCount === 0" description="此 Artifact 暂无 Commit" />
            <ArtifactCommitTree
              v-else
              :roots="commitTree.roots"
              :head-commit-id="artifact.head_commit_id"
              @select="showCommitDetail"
            />
            <el-button
              v-if="nextCommitCursor"
              :loading="loadingMoreCommits"
              @click="loadMoreCommits"
            >
              加载更多历史
            </el-button>
          </template>
        </el-tab-pane>

        <el-tab-pane label="工作区" name="workspaces">
          <ApiProblemAlert
            v-if="workspaceQuery.error.value"
            :error="workspaceQuery.error.value"
            :retrying="workspaceQuery.isFetching.value"
            @retry="workspaceQuery.refetch"
          />
          <el-skeleton v-if="workspaceQuery.isPending.value" :rows="5" animated />
          <el-empty
            v-else-if="!workspaceQuery.data.value?.data.items.length"
            description="此 Artifact 暂无 Workspace"
          />
          <div v-else class="relation-list">
            <button
              v-for="workspace in workspaceQuery.data.value?.data.items"
              :key="workspace.workspace_id"
              type="button"
              @click="openWorkspace(workspace.workspace_id)"
            >
              <span>
                <strong>{{ workspace.display_name }}</strong>
                <code>{{ workspace.workspace_id }}</code>
              </span>
              <span class="relation-list__aside">
                <small>{{ workspace.region }}</small>
                <el-tag :type="workspaceLifecycleTagType(workspace.state)" effect="plain">{{
                  workspaceLifecycleLabel(workspace.state)
                }}</el-tag
                ><el-tag
                  :type="
                    workspaceStorageAvailabilityTagType(workspaceStorageAvailability(workspace))
                  "
                  effect="plain"
                  >{{
                    workspaceStorageAvailabilityLabel(workspaceStorageAvailability(workspace))
                  }}</el-tag
                ><el-tag v-if="workspace.active_precommit_id" type="warning" effect="plain">
                  活动 Pre-commit
                </el-tag>
                <ArrowRight />
              </span>
            </button>
          </div>
        </el-tab-pane>

        <el-tab-pane v-if="snapshotMaterializeEnabled" label="快照" name="snapshots">
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
                  {{ formatCount(snapshot.logical_file_count) }} files ·
                  {{ formatBytes(snapshot.logical_size_bytes) }}
                </small>
                <el-tag :type="snapshotStateTagType(snapshot.state)" effect="plain">
                  {{ snapshotStateLabel(snapshot.state) }}
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
      v-model="createWorkspaceOpen"
      title="创建 Workspace"
      width="min(560px, calc(100vw - 32px))"
    >
      <ApiProblemAlert
        v-if="createWorkspaceMutation.error.value"
        :error="createWorkspaceMutation.error.value"
      />
      <el-alert v-if="mutationError" :title="mutationError" type="error" :closable="false" />
      <el-form label-position="top" class="dialog-form">
        <el-form-item label="Workspace ID">
          <el-input v-model="workspaceForm.workspaceId" placeholder="review-july" />
        </el-form-item>
        <el-form-item label="名称">
          <el-input v-model="workspaceForm.displayName" placeholder="七月复核" />
        </el-form-item>
        <el-form-item label="StorageVolume" required>
          <StorageVolumeFilter v-model="workspaceForm.storageVolumeId" :tenant-id="tenantId" />
        </el-form-item>
        <el-form-item label="Base Commit">
          <ArtifactCommitSelect
            v-model="workspaceForm.baseCommitId"
            :tenant-id="tenantId"
            :project-id="projectId"
            :artifact-id="artifactId"
            :head-commit-id="artifact?.head_commit_id"
            :enabled="createWorkspaceOpen"
            :allow-history="artifactCommitGraphEnabled"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createWorkspaceOpen = false">取消</el-button>
        <el-button
          type="primary"
          :loading="createWorkspaceMutation.isPending.value"
          @click="submitWorkspace"
        >
          创建 Workspace
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>
