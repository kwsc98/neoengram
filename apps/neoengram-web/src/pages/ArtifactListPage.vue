<script setup lang="ts">
import { ArrowRight, Plus, Search } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { createArtifact, queryArtifactCommitGraph, queryArtifactList } from '@/api/operations';
import type { ArtifactInitializationMode } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import ProjectFilter from '@/components/ProjectFilter.vue';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = ref(String(route.query.project_id ?? ''));
const searchInput = ref(String(route.query.q ?? ''));
const search = ref(searchInput.value);
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);
const createOpen = ref(false);
const createError = ref('');
const createForm = reactive({
  projectId: '',
  artifactId: '',
  displayName: '',
  description: '',
  initializationMode: 'empty' as ArtifactInitializationMode,
  sourceArtifactKey: '',
  sourceCommitId: '',
});
const canCreate = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('artifact.create') ?? false,
);
const createMutation = useMutation({ mutationFn: createArtifact });
const sourceArtifactsQuery = useQuery({
  queryKey: computed(() => ['artifacts', tenantId.value, 'artifact-source-options']),
  queryFn: () => queryArtifactList({ tenant_id: tenantId.value, page_size: 100 }),
  enabled: computed(() => createOpen.value && createForm.initializationMode === 'derived'),
});
function artifactOptionKey(project: string, artifact: string): string {
  return `${project}\u0000${artifact}`;
}

const selectedSourceArtifact = computed(() =>
  sourceArtifactsQuery.data.value?.data.items.find(
    (artifact) =>
      artifactOptionKey(artifact.project_id, artifact.artifact_id) === createForm.sourceArtifactKey,
  ),
);
const sourceCommitQuery = useQuery({
  queryKey: computed(() => [
    'artifact-commits',
    tenantId.value,
    selectedSourceArtifact.value?.project_id ?? '',
    selectedSourceArtifact.value?.artifact_id ?? '',
    'artifact-source-options',
  ]),
  queryFn: () =>
    queryArtifactCommitGraph(
      tenantId.value,
      selectedSourceArtifact.value?.project_id ?? '',
      selectedSourceArtifact.value?.artifact_id ?? '',
    ),
  enabled: computed(
    () =>
      createOpen.value &&
      createForm.initializationMode === 'derived' &&
      Boolean(selectedSourceArtifact.value),
  ),
});

watch(
  () => createForm.sourceArtifactKey,
  () => {
    createForm.sourceCommitId = '';
  },
);

watch(
  () => route.query,
  (query) => {
    projectId.value = String(query.project_id ?? '');
    searchInput.value = String(query.q ?? '');
    search.value = searchInput.value;
    cursor.value = undefined;
    cursorHistory.value = [];
  },
);

watch([tenantId, projectId], () => {
  cursor.value = undefined;
  cursorHistory.value = [];
});

const artifactsQuery = useQuery({
  queryKey: computed(() => [
    'artifacts',
    tenantId.value,
    projectId.value,
    search.value,
    cursor.value ?? '',
  ]),
  queryFn: () =>
    queryArtifactList({
      tenant_id: tenantId.value,
      page_size: 50,
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(search.value ? { query: search.value } : {}),
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
});

async function applyFilters(): Promise<void> {
  cursor.value = undefined;
  cursorHistory.value = [];
  await router.replace({
    query: {
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(searchInput.value.trim() ? { q: searchInput.value.trim() } : {}),
    },
  });
}

function nextPage(): void {
  const next = artifactsQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  const previous = cursorHistory.value.pop();
  cursor.value = previous || undefined;
}

async function openArtifact(project: string, artifact: string): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: project, artifactId: artifact },
  });
}

function openCreate(): void {
  createForm.projectId = projectId.value;
  createForm.artifactId = '';
  createForm.displayName = '';
  createForm.description = '';
  createForm.initializationMode = 'empty';
  createForm.sourceArtifactKey = '';
  createForm.sourceCommitId = '';
  createError.value = '';
  createOpen.value = true;
}

async function submitCreate(): Promise<void> {
  createError.value = '';
  const resourceId = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
  if (!resourceId.test(createForm.projectId) || !resourceId.test(createForm.artifactId)) {
    createError.value = 'Project ID 和 Artifact ID 必须是合法资源标识';
    return;
  }
  if (!createForm.displayName.trim()) {
    createError.value = '请输入 Artifact 名称';
    return;
  }
  if (
    createForm.initializationMode === 'derived' &&
    (!selectedSourceArtifact.value || !createForm.sourceCommitId)
  ) {
    createError.value = '请选择来源 Artifact 和明确的 Commit';
    return;
  }
  try {
    const result = await createMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: createForm.projectId,
      artifact_id: createForm.artifactId,
      display_name: createForm.displayName.trim(),
      ...(createForm.description.trim() ? { description: createForm.description.trim() } : {}),
      initialization:
        createForm.initializationMode === 'derived'
          ? {
              mode: 'derived',
              source_project_id: selectedSourceArtifact.value!.project_id,
              source_artifact_id: selectedSourceArtifact.value!.artifact_id,
              source_commit_id: createForm.sourceCommitId,
            }
          : { mode: 'empty' },
    });
    createOpen.value = false;
    await queryClient.invalidateQueries({ queryKey: ['artifacts', tenantId.value] });
    ElMessage.success(result.data.replayed ? '已返回现有 Artifact' : 'Artifact 已创建');
    await openArtifact(result.data.artifact.project_id, result.data.artifact.artifact_id);
  } catch (error) {
    createError.value = error instanceof Error ? error.message : '创建 Artifact 失败';
  }
}
</script>

<template>
  <div class="page">
    <PageHeading title="数据资产" :description="`${tenantId} 内版本化管理的 Artifact`">
      <template #actions>
        <el-button v-if="canCreate" type="primary" :icon="Plus" @click="openCreate">
          创建 Artifact
        </el-button>
      </template>
    </PageHeading>

    <form class="resource-toolbar" @submit.prevent="applyFilters">
      <ProjectFilter v-model="projectId" :tenant-id="tenantId" />
      <el-input v-model="searchInput" clearable placeholder="搜索名称或 Artifact ID" />
      <el-button type="primary" native-type="submit" :icon="Search">查询</el-button>
    </form>

    <ApiProblemAlert
      v-if="artifactsQuery.error.value"
      :error="artifactsQuery.error.value"
      :retrying="artifactsQuery.isFetching.value"
      @retry="artifactsQuery.refetch"
    />

    <section class="content-section resource-section">
      <el-skeleton v-if="artifactsQuery.isPending.value" :rows="7" animated />
      <el-empty
        v-else-if="!artifactsQuery.data.value?.data.items.length"
        description="当前筛选下没有 Artifact"
        :image-size="78"
      />
      <template v-else>
        <el-table
          :data="artifactsQuery.data.value?.data.items"
          class="resource-table desktop-table"
        >
          <el-table-column label="Artifact" min-width="250">
            <template #default="scope">
              <button
                class="resource-link"
                type="button"
                @click="openArtifact(scope.row.project_id, scope.row.artifact_id)"
              >
                <strong>{{ scope.row.display_name }}</strong>
                <code>{{ scope.row.artifact_id }}</code>
              </button>
            </template>
          </el-table-column>
          <el-table-column prop="project_id" label="Project" min-width="170" />
          <el-table-column prop="description" label="描述" min-width="280" />
          <el-table-column label="更新时间" min-width="160">
            <template #default="scope">{{ formatTime(scope.row.updated_at_unix_ms) }}</template>
          </el-table-column>
          <el-table-column width="54" align="right">
            <template #default="scope">
              <el-button
                text
                :icon="ArrowRight"
                title="查看 Artifact"
                @click="openArtifact(scope.row.project_id, scope.row.artifact_id)"
              />
            </template>
          </el-table-column>
        </el-table>
        <div class="mobile-resource-list">
          <button
            v-for="artifact in artifactsQuery.data.value?.data.items"
            :key="`${artifact.project_id}/${artifact.artifact_id}`"
            type="button"
            class="mobile-resource-item"
            @click="openArtifact(artifact.project_id, artifact.artifact_id)"
          >
            <span
              ><strong>{{ artifact.display_name }}</strong
              ><code>{{ artifact.artifact_id }}</code></span
            >
            <span
              ><small>{{ artifact.project_id }}</small
              ><ArrowRight
            /></span>
          </button>
        </div>
        <PageCursor
          :has-previous="cursorHistory.length > 0"
          :has-next="Boolean(artifactsQuery.data.value?.data.next_cursor)"
          :loading="artifactsQuery.isFetching.value"
          @previous="previousPage"
          @next="nextPage"
        />
      </template>
    </section>

    <el-dialog v-model="createOpen" title="创建 Artifact" width="min(560px, calc(100vw - 32px))">
      <ApiProblemAlert v-if="createMutation.error.value" :error="createMutation.error.value" />
      <el-alert v-if="createError" :title="createError" type="error" :closable="false" />
      <el-form label-position="top" class="dialog-form">
        <el-form-item label="Project">
          <ProjectFilter v-model="createForm.projectId" :tenant-id="tenantId" />
        </el-form-item>
        <el-form-item label="Artifact ID">
          <el-input v-model="createForm.artifactId" placeholder="evaluation-set" />
        </el-form-item>
        <el-form-item label="名称">
          <el-input v-model="createForm.displayName" placeholder="评测数据集" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="createForm.description" type="textarea" :rows="3" />
        </el-form-item>
        <el-form-item label="初始化方式">
          <el-segmented
            v-model="createForm.initializationMode"
            :options="[
              { label: '创建空 Artifact', value: 'empty' },
              { label: '从 Commit 派生', value: 'derived' },
            ]"
          />
        </el-form-item>
        <template v-if="createForm.initializationMode === 'derived'">
          <el-form-item label="来源 Artifact">
            <el-select
              v-model="createForm.sourceArtifactKey"
              filterable
              placeholder="选择同租户的数据资产"
              :loading="sourceArtifactsQuery.isFetching.value"
            >
              <el-option
                v-for="artifact in sourceArtifactsQuery.data.value?.data.items ?? []"
                :key="`${artifact.project_id}/${artifact.artifact_id}`"
                :label="`${artifact.display_name} · ${artifact.project_id}/${artifact.artifact_id}`"
                :value="artifactOptionKey(artifact.project_id, artifact.artifact_id)"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="来源 Commit">
            <el-select
              v-model="createForm.sourceCommitId"
              filterable
              :disabled="!createForm.sourceArtifactKey"
              :loading="sourceCommitQuery.isFetching.value"
              placeholder="选择一个固定版本"
            >
              <el-option
                v-for="commit in sourceCommitQuery.data.value?.data.graph.nodes ?? []"
                :key="commit.commit_id"
                :label="`${commit.message} · ${commit.commit_id}`"
                :value="commit.commit_id"
              />
            </el-select>
          </el-form-item>
          <el-alert
            title="新 Artifact 将获得独立的初始化 Commit；不会继承来源版本历史，也不会创建区域副本。"
            type="info"
            :closable="false"
            show-icon
          />
        </template>
      </el-form>
      <template #footer>
        <el-button @click="createOpen = false">取消</el-button>
        <el-button type="primary" :loading="createMutation.isPending.value" @click="submitCreate">
          创建 Artifact
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.dialog-form :deep(.el-segmented),
.dialog-form :deep(.el-select) {
  width: 100%;
}
</style>
