<script setup lang="ts">
import { ArrowRight, RefreshRight, Search } from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { queryTaskList, queryTaskSummary } from '@/api/operations';
import type { TaskKind, TaskState, TaskView } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = ref(String(route.query.project_id ?? ''));
const artifactId = ref(String(route.query.artifact_id ?? ''));
const commitId = ref(String(route.query.commit_id ?? ''));
const selectedStates = ref<TaskState[]>(parseList<TaskState>(route.query.state));
const selectedKinds = ref<TaskKind[]>(parseList<TaskKind>(route.query.task_kind));
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);

const stateOptions: Array<{ value: TaskState; label: string }> = [
  { value: 'queued', label: '排队中' },
  { value: 'running', label: '运行中' },
  { value: 'waiting', label: '等待资源' },
  { value: 'verifying', label: '校验中' },
  { value: 'stalled', label: '已停滞' },
  { value: 'succeeded', label: '已完成' },
  { value: 'failed', label: '失败' },
  { value: 'cancelled', label: '已取消' },
];

const kindOptions: Array<{ value: TaskKind; label: string }> = [
  { value: 'workspace.create', label: '创建工作区' },
  { value: 'workspace.materialize', label: '物化工作区' },
  { value: 'precommit.check', label: 'Pre-commit 校验' },
  { value: 'add.scan', label: '扫描 Add' },
  { value: 'commit.create', label: '创建 Commit' },
  { value: 'snapshot.create', label: '创建快照' },
  { value: 'snapshot.delivery.materialize', label: '物化快照交付' },
  { value: 'commit.materialize', label: '物化 Commit' },
  { value: 'integrity.scan', label: '完整性扫描' },
  { value: 'resource.repair', label: '资源修复' },
  { value: 'catalog.lifecycle', label: '资源生命周期' },
  { value: 'storage.lifecycle', label: '存储生命周期' },
  { value: 'gateway.lifecycle', label: 'Gateway 生命周期' },
  { value: 's3.lifecycle', label: 'S3 生命周期' },
];

const activeStates = new Set<TaskState>(['queued', 'running', 'waiting', 'verifying', 'stalled']);

const listRequest = computed(() => ({
  tenant_id: tenantId.value,
  page_size: 50,
  ...(projectId.value ? { project_id: projectId.value } : {}),
  ...(artifactId.value ? { artifact_id: artifactId.value } : {}),
  ...(commitId.value ? { commit_id: commitId.value } : {}),
  ...(selectedStates.value.length ? { state: selectedStates.value } : {}),
  ...(selectedKinds.value.length ? { task_kind: selectedKinds.value } : {}),
  ...(cursor.value ? { cursor: cursor.value } : {}),
}));

const taskQuery = useQuery({
  queryKey: computed(() => ['tasks', listRequest.value]),
  queryFn: () => queryTaskList(listRequest.value),
  refetchInterval: (query) =>
    query.state.data?.data.items.some((task) => activeStates.has(task.state)) ? 2000 : false,
});

const summaryQuery = useQuery({
  queryKey: computed(() => [
    'task-summary',
    tenantId.value,
    projectId.value,
    artifactId.value,
    commitId.value,
    selectedStates.value,
    selectedKinds.value,
  ]),
  queryFn: () =>
    queryTaskSummary({
      tenant_id: tenantId.value,
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(artifactId.value ? { artifact_id: artifactId.value } : {}),
      ...(commitId.value ? { commit_id: commitId.value } : {}),
      ...(selectedStates.value.length ? { state: selectedStates.value } : {}),
      ...(selectedKinds.value.length ? { task_kind: selectedKinds.value } : {}),
    }),
});

const tasks = computed(() => taskQuery.data.value?.data.items ?? []);
const summary = computed(() => summaryQuery.data.value?.data.summary);

watch(
  () => route.query,
  (query) => {
    projectId.value = String(query.project_id ?? '');
    artifactId.value = String(query.artifact_id ?? '');
    commitId.value = String(query.commit_id ?? '');
    selectedStates.value = parseList<TaskState>(query.state);
    selectedKinds.value = parseList<TaskKind>(query.task_kind);
    cursor.value = undefined;
    cursorHistory.value = [];
  },
  { deep: true },
);

async function applyFilters(): Promise<void> {
  cursor.value = undefined;
  cursorHistory.value = [];
  await router.replace({
    query: {
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(artifactId.value ? { artifact_id: artifactId.value } : {}),
      ...(commitId.value ? { commit_id: commitId.value } : {}),
      ...(selectedStates.value.length ? { state: selectedStates.value.join(',') } : {}),
      ...(selectedKinds.value.length ? { task_kind: selectedKinds.value.join(',') } : {}),
    },
  });
}

function nextPage(): void {
  const next = taskQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  cursor.value = cursorHistory.value.pop() || undefined;
}

async function openTask(task: TaskView): Promise<void> {
  await router.push({
    name: 'task-detail',
    params: { tenantId: tenantId.value, taskId: task.task_id },
  });
}

function stateLabel(state: TaskState): string {
  return stateOptions.find((option) => option.value === state)?.label ?? state;
}

function stateType(state: TaskState): 'success' | 'warning' | 'danger' | 'info' {
  if (state === 'succeeded') return 'success';
  if (state === 'failed' || state === 'cancelled') return 'danger';
  if (state === 'stalled') return 'warning';
  return 'info';
}

function kindLabel(kind: TaskKind): string {
  return kindOptions.find((option) => option.value === kind)?.label ?? kind;
}

function progress(task: TaskView): string {
  const completed = BigInt(task.progress.completed);
  const total = BigInt(task.progress.total);
  if (total === 0n) return '—';
  return `${((Number(completed) / Number(total)) * 100).toFixed(1)}%`;
}

function scope(task: TaskView): string {
  return task.commit_id ?? task.artifact_id ?? task.playground_id ?? task.snapshot_id ?? '—';
}

function parseList<T extends string>(value: unknown): T[] {
  const raw: unknown = Array.isArray(value) ? (value as unknown[])[0] : value;
  return typeof raw === 'string' && raw ? (raw.split(',').filter(Boolean) as T[]) : [];
}
</script>

<template>
  <div class="page">
    <PageHeading title="操作任务" :description="`${tenantId} 的统一写操作、重试与审计入口`">
      <template #actions>
        <el-button
          :icon="RefreshRight"
          :loading="taskQuery.isFetching.value"
          @click="taskQuery.refetch"
        >
          刷新
        </el-button>
      </template>
    </PageHeading>

    <form class="resource-toolbar resource-toolbar--wide" @submit.prevent="applyFilters">
      <el-input v-model="projectId" aria-label="Project 筛选" placeholder="Project ID" clearable />
      <el-input
        v-model="artifactId"
        aria-label="Artifact 筛选"
        placeholder="Artifact ID"
        clearable
      />
      <el-input v-model="commitId" aria-label="Commit 筛选" placeholder="Commit ID" clearable />
      <el-select
        v-model="selectedKinds"
        aria-label="任务类型筛选"
        multiple
        collapse-tags
        clearable
        placeholder="任务类型"
      >
        <el-option
          v-for="option in kindOptions"
          :key="option.value"
          :label="option.label"
          :value="option.value"
        />
      </el-select>
      <el-select
        v-model="selectedStates"
        aria-label="任务状态筛选"
        multiple
        collapse-tags
        clearable
        placeholder="状态"
      >
        <el-option
          v-for="option in stateOptions"
          :key="option.value"
          :label="option.label"
          :value="option.value"
        />
      </el-select>
      <el-button type="primary" native-type="submit" :icon="Search">查询</el-button>
    </form>

    <ApiProblemAlert
      v-if="taskQuery.error.value || summaryQuery.error.value"
      :error="taskQuery.error.value ?? summaryQuery.error.value"
      :retrying="taskQuery.isFetching.value || summaryQuery.isFetching.value"
      @retry="taskQuery.refetch"
    />

    <section class="task-summary-grid" aria-label="任务状态汇总">
      <div>
        <span>全部</span><strong>{{ summary?.total ?? '—' }}</strong>
      </div>
      <div>
        <span>运行中</span><strong>{{ summary?.running ?? '—' }}</strong>
      </div>
      <div>
        <span>等待</span><strong>{{ summary?.waiting ?? '—' }}</strong>
      </div>
      <div>
        <span>停滞</span><strong>{{ summary?.stalled ?? '—' }}</strong>
      </div>
      <div>
        <span>失败</span><strong>{{ summary?.failed ?? '—' }}</strong>
      </div>
      <div>
        <span>已完成</span><strong>{{ summary?.succeeded ?? '—' }}</strong>
      </div>
    </section>

    <section class="content-section resource-section">
      <el-skeleton v-if="taskQuery.isPending.value" :rows="7" animated />
      <el-empty v-else-if="!tasks.length" description="当前筛选下没有任务" :image-size="78" />
      <template v-else>
        <el-table :data="tasks" class="resource-table task-table">
          <el-table-column label="任务" min-width="260">
            <template #default="slotProps">
              <button class="resource-link" type="button" @click="openTask(slotProps.row)">
                <strong>{{ kindLabel(slotProps.row.task_kind) }}</strong>
                <code>{{ slotProps.row.task_id }}</code>
              </button>
            </template>
          </el-table-column>
          <el-table-column label="范围" min-width="180">
            <template #default="slotProps"
              ><code>{{ scope(slotProps.row) }}</code></template
            >
          </el-table-column>
          <el-table-column label="状态" min-width="120">
            <template #default="slotProps">
              <el-tag :type="stateType(slotProps.row.state)" effect="plain">{{
                stateLabel(slotProps.row.state)
              }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="进度" min-width="100">
            <template #default="slotProps">{{ progress(slotProps.row) }}</template>
          </el-table-column>
          <el-table-column label="更新时间" min-width="180">
            <template #default="slotProps">{{
              formatTime(slotProps.row.updated_at_unix_ms)
            }}</template>
          </el-table-column>
          <el-table-column width="64" align="right">
            <template #default="slotProps">
              <el-button
                text
                :icon="ArrowRight"
                title="打开任务详情"
                @click="openTask(slotProps.row)"
              />
            </template>
          </el-table-column>
        </el-table>
        <div class="pagination-actions">
          <el-button :disabled="!cursorHistory.length" @click="previousPage">上一页</el-button>
          <el-button
            type="primary"
            :disabled="!taskQuery.data.value?.data.next_cursor"
            @click="nextPage"
            >下一页</el-button
          >
        </div>
      </template>
    </section>
  </div>
</template>

<style scoped>
.task-summary-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 18px;
}

.task-summary-grid > div {
  border: 1px solid var(--el-border-color-light);
  background: var(--el-bg-color);
  padding: 14px 16px;
}

.task-summary-grid span,
.task-summary-grid strong {
  display: block;
}

.task-summary-grid span {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.task-summary-grid strong {
  color: var(--el-text-color-primary);
  font-size: 22px;
  margin-top: 5px;
}

.task-table code {
  overflow-wrap: anywhere;
}

@media (max-width: 900px) {
  .task-summary-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 560px) {
  .task-summary-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
