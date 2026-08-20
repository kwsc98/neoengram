<script setup lang="ts">
import { ArrowRight, Plus, Search } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { createProject, queryProjectList } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const searchInput = ref(String(route.query.q ?? ''));
const search = ref(searchInput.value);
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);
const createOpen = ref(false);
const createError = ref('');
const createForm = reactive({ projectId: '', displayName: '', description: '' });

const canCreate = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('project.create') ?? false,
);
const projectsQuery = useQuery({
  queryKey: computed(() => ['projects', tenantId.value, search.value, cursor.value ?? '']),
  queryFn: () =>
    queryProjectList({
      tenant_id: tenantId.value,
      page_size: 50,
      ...(search.value ? { query: search.value } : {}),
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
});
const createMutation = useMutation({ mutationFn: createProject });

watch(tenantId, () => {
  searchInput.value = '';
  search.value = '';
  cursor.value = undefined;
  cursorHistory.value = [];
  createOpen.value = false;
  createError.value = '';
  createMutation.reset();
});

watch(
  () => route.query.q,
  (value) => {
    const next = String(value ?? '');
    searchInput.value = next;
    search.value = next;
    cursor.value = undefined;
    cursorHistory.value = [];
  },
);

watch(
  () => route.query.create,
  async (value) => {
    if (value !== '1') return;
    openCreate();
    const query = { ...route.query };
    delete query.create;
    await router.replace({
      name: 'project-list',
      params: { tenantId: tenantId.value },
      query,
    });
  },
  { immediate: true },
);

async function applyFilters(): Promise<void> {
  const next = searchInput.value.trim();
  cursor.value = undefined;
  cursorHistory.value = [];
  await router.replace({
    name: 'project-list',
    params: { tenantId: tenantId.value },
    query: next ? { q: next } : {},
  });
  search.value = next;
}

function nextPage(): void {
  const next = projectsQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  cursor.value = cursorHistory.value.pop() || undefined;
}

function openCreate(): void {
  Object.assign(createForm, { projectId: '', displayName: '', description: '' });
  createError.value = '';
  createMutation.reset();
  createOpen.value = true;
}

async function submitCreate(): Promise<void> {
  createError.value = '';
  const projectId = createForm.projectId.trim();
  const displayName = createForm.displayName.trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(projectId)) {
    createError.value = 'Project ID 必须是 1-128 位合法资源标识';
    return;
  }
  if (!displayName) {
    createError.value = '请输入 Project 名称';
    return;
  }

  try {
    const result = await createMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: projectId,
      display_name: displayName,
      ...(createForm.description.trim() ? { description: createForm.description.trim() } : {}),
    });
    await queryClient.invalidateQueries({ queryKey: ['projects', tenantId.value] });
    createOpen.value = false;
    ElMessage.success(result.data.replayed ? '已返回现有 Project' : 'Project 已创建');
  } catch {
    // ApiProblemAlert renders the structured mutation error below.
  }
}

async function openArtifacts(projectId: string): Promise<void> {
  await router.push({
    name: 'artifact-list',
    params: { tenantId: tenantId.value },
    query: { project_id: projectId },
  });
}
</script>

<template>
  <div class="page">
    <PageHeading title="Projects" :description="`${tenantId} 内的逻辑项目目录`">
      <template v-if="canCreate" #actions>
        <el-button type="primary" :icon="Plus" @click="openCreate">创建 Project</el-button>
      </template>
    </PageHeading>

    <form class="resource-toolbar" @submit.prevent="applyFilters">
      <el-input v-model="searchInput" clearable placeholder="搜索 Project 名称或 ID" />
      <el-button type="primary" native-type="submit" :icon="Search">查询</el-button>
    </form>

    <ApiProblemAlert
      v-if="projectsQuery.error.value"
      :error="projectsQuery.error.value"
      :retrying="projectsQuery.isFetching.value"
      @retry="projectsQuery.refetch"
    />

    <section class="content-section resource-section">
      <el-skeleton v-if="projectsQuery.isPending.value" :rows="6" animated />
      <el-empty
        v-else-if="!projectsQuery.data.value?.data.items.length"
        description="当前筛选下没有 Project"
        :image-size="78"
      />
      <template v-else>
        <el-table :data="projectsQuery.data.value?.data.items" class="resource-table desktop-table">
          <el-table-column label="Project" min-width="250">
            <template #default="scope">
              <button
                class="resource-link"
                type="button"
                @click="openArtifacts(scope.row.project_id)"
              >
                <strong>{{ scope.row.display_name }}</strong>
                <code>{{ scope.row.project_id }}</code>
              </button>
            </template>
          </el-table-column>
          <el-table-column prop="description" label="描述" min-width="300" />
          <el-table-column label="更新时间" min-width="170">
            <template #default="scope">{{ formatTime(scope.row.updated_at_unix_ms) }}</template>
          </el-table-column>
          <el-table-column width="70" align="right">
            <template #default="scope">
              <el-button
                text
                :icon="ArrowRight"
                title="查看项目数据资产"
                @click="openArtifacts(scope.row.project_id)"
              />
            </template>
          </el-table-column>
        </el-table>
        <div class="mobile-resource-list">
          <div
            v-for="project in projectsQuery.data.value?.data.items"
            :key="project.project_id"
            class="mobile-resource-item"
            role="button"
            tabindex="0"
            @click="openArtifacts(project.project_id)"
            @keydown.enter="openArtifacts(project.project_id)"
            @keydown.space.prevent="openArtifacts(project.project_id)"
          >
            <span>
              <strong>{{ project.display_name }}</strong>
              <code>{{ project.project_id }}</code>
              <small v-if="project.description">{{ project.description }}</small>
            </span>
            <ArrowRight />
          </div>
        </div>
        <PageCursor
          :has-previous="cursorHistory.length > 0"
          :has-next="Boolean(projectsQuery.data.value?.data.next_cursor)"
          :loading="projectsQuery.isFetching.value"
          @previous="previousPage"
          @next="nextPage"
        />
      </template>
    </section>

    <el-dialog v-model="createOpen" title="创建 Project" width="min(520px, calc(100vw - 32px))">
      <ApiProblemAlert v-if="createMutation.error.value" :error="createMutation.error.value" />
      <el-alert v-if="createError" :title="createError" type="error" :closable="false" />
      <el-form label-position="top" class="dialog-form" @submit.prevent="submitCreate">
        <el-form-item label="Project ID" required>
          <el-input v-model="createForm.projectId" placeholder="project-lab" />
        </el-form-item>
        <el-form-item label="名称" required>
          <el-input v-model="createForm.displayName" placeholder="算法实验室" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="createForm.description" type="textarea" :rows="3" maxlength="2048" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createOpen = false">取消</el-button>
        <el-button type="primary" :loading="createMutation.isPending.value" @click="submitCreate">
          创建 Project
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>
