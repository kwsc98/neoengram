<script setup lang="ts">
import { ArrowRight, Plus, Search } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { createWorkspace, queryApiVersion, queryWorkspaceList } from '@/api/operations';
import type { ArtifactView } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import ResourceDeletionDialog from '@/components/ResourceDeletionDialog.vue';
import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import ArtifactSelect from '@/components/ArtifactSelect.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import StorageVolumeFilter from '@/components/StorageVolumeFilter.vue';
import {
  supportsArtifactCommitGraph,
  supportsWorkspaceMaterialize,
  supportsResourceLifecycle,
} from '@/features/capabilities';
import { lifecycleResourceVersion } from '@/features/lifecycle';
import {
  workspaceLifecycleLabel,
  workspaceLifecycleTagType,
  workspaceListPollInterval,
  workspaceStorageAvailability,
  workspaceStorageAvailabilityLabel,
  workspaceStorageAvailabilityTagType,
} from '@/features/precommit/status';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = ref(String(route.query.project_id ?? ''));
const artifactId = ref(String(route.query.artifact_id ?? ''));
const searchInput = ref(String(route.query.q ?? ''));
const search = ref(searchInput.value);
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);
const createOpen = ref(false);
const createError = ref('');
const createForm = reactive({
  artifact: undefined as ArtifactView | undefined,
  workspaceId: '',
  displayName: '',
  baseCommitId: '',
  storageVolumeId: '',
});
const createMutation = useMutation({ mutationFn: createWorkspace });
const versionQuery = useQuery({
  queryKey: ['system', 'version'],
  queryFn: queryApiVersion,
  staleTime: Number.POSITIVE_INFINITY,
});
const materializeEnabled = computed(() =>
  supportsWorkspaceMaterialize(versionQuery.data.value?.data.capabilities),
);
const artifactCommitGraphEnabled = computed(() =>
  supportsArtifactCommitGraph(versionQuery.data.value?.data.capabilities),
);
const canCreateWorkspace = computed(
  () =>
    (tenants.byId(tenantId.value)?.permissions.includes('workspace.create') ?? false) &&
    materializeEnabled.value,
);
const lifecycleEnabled = computed(
  () =>
    supportsResourceLifecycle(versionQuery.data.value?.data.capabilities) &&
    (tenants.byId(tenantId.value)?.permissions.includes('resource.lifecycle.manage' as never) ??
      false),
);

const workspaceQuery = useQuery({
  queryKey: computed(() => [
    'workspaces',
    tenantId.value,
    projectId.value,
    artifactId.value,
    search.value,
    cursor.value ?? '',
  ]),
  queryFn: () =>
    queryWorkspaceList({
      tenant_id: tenantId.value,
      page_size: 50,
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(artifactId.value ? { artifact_id: artifactId.value } : {}),
      ...(search.value ? { query: search.value } : {}),
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
  refetchInterval: (query) => workspaceListPollInterval(query.state.data?.data.items ?? []),
});

watch(projectId, (value, previous) => {
  if (value !== previous && artifactId.value && String(route.query.project_id ?? '') !== value) {
    artifactId.value = '';
  }
  cursor.value = undefined;
  cursorHistory.value = [];
});

watch(
  [tenantId, () => route.query],
  ([, query]) => {
    projectId.value = String(query.project_id ?? '');
    artifactId.value = String(query.artifact_id ?? '');
    searchInput.value = String(query.q ?? '');
    search.value = searchInput.value;
    cursor.value = undefined;
    cursorHistory.value = [];
    createOpen.value = false;
    createError.value = '';
    createMutation.reset();
  },
  { deep: true },
);

watch(artifactId, () => {
  cursor.value = undefined;
  cursorHistory.value = [];
});

watch(
  () => createForm.artifact,
  (artifact) => {
    createForm.baseCommitId = artifact?.head_commit_id ?? '';
  },
);

async function applyFilters(): Promise<void> {
  cursor.value = undefined;
  cursorHistory.value = [];
  await router.replace({
    query: {
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(artifactId.value ? { artifact_id: artifactId.value } : {}),
      ...(searchInput.value.trim() ? { q: searchInput.value.trim() } : {}),
    },
  });
  search.value = searchInput.value.trim();
}

function nextPage(): void {
  const next = workspaceQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  cursor.value = cursorHistory.value.pop() || undefined;
}

function openCreate(): void {
  Object.assign(createForm, {
    artifact: undefined,
    workspaceId: '',
    displayName: '',
    baseCommitId: '',
    storageVolumeId: '',
  });
  createError.value = '';
  createMutation.reset();
  createOpen.value = true;
}

async function submitCreate(): Promise<void> {
  createError.value = '';
  const resourceId = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
  const artifact = createForm.artifact;
  if (
    !artifact ||
    artifact.tenant_id !== tenantId.value ||
    (Boolean(artifact.head_commit_id) && !createForm.baseCommitId) ||
    !resourceId.test(createForm.workspaceId) ||
    !createForm.displayName.trim() ||
    !resourceId.test(createForm.storageVolumeId)
  ) {
    createError.value = '请选择 Artifact，并填写合法的 Workspace、名称和 StorageVolume';
    return;
  }

  let result;
  try {
    result = await createMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: artifact.project_id,
      artifact_id: artifact.artifact_id,
      workspace_id: createForm.workspaceId,
      display_name: createForm.displayName.trim(),
      storage_volume_id: createForm.storageVolumeId,
      ...(createForm.baseCommitId ? { base_commit_id: createForm.baseCommitId } : {}),
    });
  } catch {
    return;
  }

  await queryClient.invalidateQueries({ queryKey: ['workspaces', tenantId.value] });
  createOpen.value = false;
  ElMessage.success(result.data.request_replayed ? '已返回现有 Workspace' : 'Workspace 已创建');
  await openWorkspace(
    result.data.workspace.project_id,
    result.data.workspace.artifact_id,
    result.data.workspace.workspace_id,
  );
}

async function openWorkspace(project: string, artifact: string, workspace: string): Promise<void> {
  await router.push({
    name: 'workspace-detail',
    params: {
      tenantId: tenantId.value,
      projectId: project,
      artifactId: artifact,
      workspaceId: workspace,
    },
  });
}
</script>

<template>
  <div class="page">
    <PageHeading title="工作区" :description="`${tenantId} 内可以产生数据变化的 Workspace`">
      <template v-if="canCreateWorkspace" #actions>
        <el-button type="primary" :icon="Plus" @click="openCreate">创建 Workspace</el-button>
      </template>
    </PageHeading>
    <form class="resource-toolbar resource-toolbar--wide" @submit.prevent="applyFilters">
      <el-input
        v-model="projectId"
        aria-label="Project 筛选"
        clearable
        placeholder="全部 Project"
      />
      <el-input
        v-model="artifactId"
        aria-label="Artifact 筛选"
        clearable
        placeholder="全部 Artifact"
      />
      <el-input v-model="searchInput" clearable placeholder="搜索 Workspace" />
      <el-button type="primary" native-type="submit" :icon="Search">查询</el-button>
    </form>

    <ApiProblemAlert
      v-if="workspaceQuery.error.value"
      :error="workspaceQuery.error.value"
      :retrying="workspaceQuery.isFetching.value"
      @retry="workspaceQuery.refetch"
    />
    <section class="content-section resource-section">
      <el-skeleton v-if="workspaceQuery.isPending.value" :rows="7" animated />
      <el-empty
        v-else-if="!workspaceQuery.data.value?.data.items.length"
        description="当前筛选下没有 Workspace"
        :image-size="78"
      />
      <template v-else>
        <el-table
          :data="workspaceQuery.data.value?.data.items"
          class="resource-table desktop-table"
        >
          <el-table-column label="Workspace" min-width="230">
            <template #default="scope">
              <button
                class="resource-link"
                type="button"
                @click="
                  openWorkspace(scope.row.project_id, scope.row.artifact_id, scope.row.workspace_id)
                "
              >
                <strong>{{ scope.row.display_name }}</strong
                ><code>{{ scope.row.workspace_id }}</code>
              </button>
            </template>
          </el-table-column>
          <el-table-column prop="project_id" label="Project" min-width="150" />
          <el-table-column prop="artifact_id" label="Artifact" min-width="160" />
          <el-table-column label="放置" min-width="190">
            <template #default="scope">
              <div class="table-placement">
                <strong>{{ scope.row.region }}</strong>
                <code>{{ scope.row.storage_volume_id }}</code>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="生命周期 / 存储" min-width="210">
            <template #default="scope">
              <div class="state-stack">
                <el-tag :type="workspaceLifecycleTagType(scope.row.state)" effect="plain">
                  {{ workspaceLifecycleLabel(scope.row.state) }}
                </el-tag>
                <el-tag
                  :type="
                    workspaceStorageAvailabilityTagType(workspaceStorageAvailability(scope.row))
                  "
                  effect="plain"
                >
                  {{ workspaceStorageAvailabilityLabel(workspaceStorageAvailability(scope.row)) }}
                </el-tag>
                <el-tag v-if="scope.row.active_precommit_id" type="warning" effect="plain">
                  存在活动 Pre-commit
                </el-tag>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="更新时间" min-width="160">
            <template #default="scope">{{ formatTime(scope.row.updated_at_unix_ms) }}</template>
          </el-table-column>
          <el-table-column :width="lifecycleEnabled ? 96 : 54" align="right">
            <template #default="scope">
              <div class="row-actions">
                <ResourceDeletionDialog
                  v-if="lifecycleEnabled"
                  :tenant-id="tenantId"
                  :resource="{
                    type: 'workspace',
                    project_id: scope.row.project_id,
                    artifact_id: scope.row.artifact_id,
                    workspace_id: scope.row.workspace_id,
                  }"
                  :resource-version="lifecycleResourceVersion(scope.row)"
                  :display-name="scope.row.display_name"
                />
                <el-button
                  text
                  :icon="ArrowRight"
                  title="查看 Workspace"
                  @click="
                    openWorkspace(
                      scope.row.project_id,
                      scope.row.artifact_id,
                      scope.row.workspace_id,
                    )
                  "
                />
              </div>
            </template>
          </el-table-column>
        </el-table>
        <div class="mobile-resource-list">
          <div
            v-for="workspace in workspaceQuery.data.value?.data.items"
            :key="`${workspace.project_id}/${workspace.artifact_id}/${workspace.workspace_id}`"
            class="mobile-resource-item"
            role="button"
            tabindex="0"
            @click="
              openWorkspace(workspace.project_id, workspace.artifact_id, workspace.workspace_id)
            "
            @keydown.enter="
              openWorkspace(workspace.project_id, workspace.artifact_id, workspace.workspace_id)
            "
            @keydown.space.prevent="
              openWorkspace(workspace.project_id, workspace.artifact_id, workspace.workspace_id)
            "
          >
            <span
              ><strong>{{ workspace.display_name }}</strong
              ><code>{{ workspace.workspace_id }}</code></span
            >
            <span
              ><small>{{ workspace.region }}</small
              ><el-tag
                :type="workspaceLifecycleTagType(workspace.state)"
                size="small"
                effect="plain"
                >{{ workspaceLifecycleLabel(workspace.state) }}</el-tag
              ><el-tag
                :type="workspaceStorageAvailabilityTagType(workspaceStorageAvailability(workspace))"
                size="small"
                effect="plain"
                >{{
                  workspaceStorageAvailabilityLabel(workspaceStorageAvailability(workspace))
                }}</el-tag
              ><el-tag
                v-if="workspace.active_precommit_id"
                type="warning"
                size="small"
                effect="plain"
                >活动 Pre-commit</el-tag
              ><ResourceDeletionDialog
                v-if="lifecycleEnabled"
                :tenant-id="tenantId"
                :resource="{
                  type: 'workspace',
                  project_id: workspace.project_id,
                  artifact_id: workspace.artifact_id,
                  workspace_id: workspace.workspace_id,
                }"
                :resource-version="lifecycleResourceVersion(workspace)"
                :display-name="workspace.display_name" />
              ><ArrowRight
            /></span>
          </div>
        </div>
        <PageCursor
          :has-previous="cursorHistory.length > 0"
          :has-next="Boolean(workspaceQuery.data.value?.data.next_cursor)"
          :loading="workspaceQuery.isFetching.value"
          @previous="previousPage"
          @next="nextPage"
        />
      </template>
    </section>

    <el-dialog v-model="createOpen" title="创建 Workspace" width="min(580px, calc(100vw - 32px))">
      <ApiProblemAlert v-if="createMutation.error.value" :error="createMutation.error.value" />
      <el-alert v-if="createError" :title="createError" type="error" :closable="false" />
      <el-form label-position="top" class="dialog-form">
        <el-form-item label="Artifact" required>
          <ArtifactSelect
            v-model="createForm.artifact"
            :tenant-id="tenantId"
            :allow-non-empty="materializeEnabled"
          />
        </el-form-item>
        <el-form-item v-if="createForm.artifact" label="Base Commit">
          <ArtifactCommitSelect
            v-model="createForm.baseCommitId"
            :tenant-id="tenantId"
            :project-id="createForm.artifact.project_id"
            :artifact-id="createForm.artifact.artifact_id"
            :head-commit-id="createForm.artifact.head_commit_id"
            :enabled="createOpen"
            :allow-history="artifactCommitGraphEnabled"
          />
        </el-form-item>
        <div class="dialog-form-grid">
          <el-form-item label="Workspace ID" required>
            <el-input v-model="createForm.workspaceId" placeholder="review-august" />
          </el-form-item>
          <el-form-item label="名称" required>
            <el-input v-model="createForm.displayName" placeholder="八月复核" />
          </el-form-item>
        </div>
        <el-form-item label="StorageVolume" required>
          <StorageVolumeFilter v-model="createForm.storageVolumeId" :tenant-id="tenantId" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createOpen = false">取消</el-button>
        <el-button type="primary" :loading="createMutation.isPending.value" @click="submitCreate">
          创建 Workspace
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.state-stack {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
}
</style>
