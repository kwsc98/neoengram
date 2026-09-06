<script setup lang="ts">
import { ArrowLeft, CircleClose, RefreshRight } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { cancelTask, queryTask, retryTask } from '@/api/operations';
import type { TaskEventView, TaskState, TaskView } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const taskId = computed(() => String(route.params.taskId ?? ''));
const queryKey = computed(() => ['task', tenantId.value, taskId.value] as const);
const activeStates = new Set<TaskState>([
  'queued',
  'running',
  'waiting',
  'verifying',
  'stalled',
  'cancelling',
]);

const taskQuery = useQuery({
  queryKey,
  queryFn: () => queryTask({ tenant_id: tenantId.value, task_id: taskId.value }),
  refetchInterval: (query) => {
    const state = query.state.data?.data.task.state;
    return state && activeStates.has(state) ? 2000 : false;
  },
});

const task = computed(() => taskQuery.data.value?.data.task);
const attempts = computed(() => taskQuery.data.value?.data.attempts ?? []);
const events = computed(() => taskQuery.data.value?.data.events ?? []);
const stages = computed(() => task.value?.stages ?? []);

const retryMutation = useMutation({
  mutationFn: () =>
    retryTask({
      tenant_id: tenantId.value,
      task_id: taskId.value,
      ...(task.value ? { expected_resource_version: task.value.resource_version } : {}),
    }),
  onSuccess: async (result) => {
    queryClient.setQueryData(queryKey.value, result);
    await taskQuery.refetch();
    ElMessage.success(result.data.request_replayed ? '任务已保持原状态' : '任务已重新排队');
  },
});

const cancelMutation = useMutation({
  mutationFn: () =>
    cancelTask({
      tenant_id: tenantId.value,
      task_id: taskId.value,
      ...(task.value ? { expected_resource_version: task.value.resource_version } : {}),
    }),
  onSuccess: async (result) => {
    queryClient.setQueryData(queryKey.value, result);
    await taskQuery.refetch();
    ElMessage.success(result.data.request_replayed ? '任务已是取消状态' : '任务已取消');
  },
});

const canRetry = computed(() =>
  Boolean(task.value && ['stalled', 'failed'].includes(task.value.state)),
);
const canCancel = computed(() => Boolean(task.value && activeStates.has(task.value.state)));

async function retry(): Promise<void> {
  try {
    await ElMessageBox.confirm('将复用当前任务 ID，并创建新的 Attempt。', '确认重试', {
      confirmButtonText: '重试',
      cancelButtonText: '取消',
      type: 'warning',
    });
    await retryMutation.mutateAsync();
  } catch {
    // Cancelled confirmation and API errors are rendered below.
  }
}

async function cancel(): Promise<void> {
  try {
    await ElMessageBox.confirm('取消后任务不会再被调度。', '确认取消', {
      confirmButtonText: '取消任务',
      cancelButtonText: '返回',
      type: 'warning',
    });
    await cancelMutation.mutateAsync();
  } catch {
    // Cancelled confirmation and API errors are rendered below.
  }
}

function stateLabel(state: TaskState): string {
  return stateLabels[state] ?? state;
}

function stateType(state: TaskState): 'success' | 'warning' | 'danger' | 'info' {
  if (state === 'succeeded') return 'success';
  if (state === 'failed' || state === 'cancelled') return 'danger';
  if (state === 'stalled') return 'warning';
  return 'info';
}

function eventLabel(event: TaskEventView): string {
  if (event.kind === 'state_changed' && event.from_state && event.to_state) {
    return `${stateLabel(event.from_state)} -> ${stateLabel(event.to_state)}`;
  }
  return eventLabels[event.kind] ?? event.kind;
}

function scope(taskView: TaskView): string {
  return taskView.primary_resource.resource_id || '—';
}

async function back(): Promise<void> {
  await router.push({ name: 'task-list', params: { tenantId: tenantId.value } });
}

const stateLabels: Record<TaskState, string> = {
  queued: '排队中',
  running: '运行中',
  waiting: '等待资源',
  verifying: '校验中',
  stalled: '已停滞',
  succeeded: '已完成',
  failed: '失败',
  cancelling: '取消中',
  cancelled: '已取消',
};
const eventLabels: Record<string, string> = {
  created: '任务创建',
  attempt_started: 'Attempt 开始',
  attempt_finished: 'Attempt 结束',
  retried: '任务重试',
  cancel_requested: '请求取消',
  cancelled: '任务取消',
  assigned: '已分配',
  reported: '收到报告',
  progress_updated: '进度更新',
  resource_linked: '关联资源',
  resource_published: '资源发布',
  failed: '任务失败',
};
</script>

<template>
  <div class="page">
    <PageHeading
      :title="task?.task_id ?? taskId"
      :description="task?.intent_kind ?? '统一操作任务'"
    >
      <template #actions>
        <el-button :icon="ArrowLeft" @click="back">返回任务列表</el-button>
        <el-button
          :icon="RefreshRight"
          :loading="taskQuery.isFetching.value"
          @click="taskQuery.refetch"
        >
          刷新
        </el-button>
        <el-button
          v-if="canRetry"
          type="warning"
          :loading="retryMutation.isPending.value"
          @click="retry"
        >
          重试
        </el-button>
        <el-button
          v-if="canCancel"
          type="danger"
          :icon="CircleClose"
          :loading="cancelMutation.isPending.value"
          @click="cancel"
        >
          取消任务
        </el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="taskQuery.error.value || retryMutation.error.value || cancelMutation.error.value"
      :error="taskQuery.error.value ?? retryMutation.error.value ?? cancelMutation.error.value"
      :retrying="taskQuery.isFetching.value"
      @retry="taskQuery.refetch"
    />

    <div v-if="task" class="task-detail-grid">
      <section class="content-section task-overview">
        <div class="section-heading section-heading--inline">
          <div>
            <h2>任务状态</h2>
            <p>统一生命周期与当前 Attempt</p>
          </div>
          <el-tag :type="stateType(task.state)" effect="plain">{{ stateLabel(task.state) }}</el-tag>
        </div>
        <dl class="definition-grid definition-grid--scope">
          <div>
            <dt>类型</dt>
            <dd>{{ task.intent_kind }}</dd>
          </div>
          <div>
            <dt>当前阶段</dt>
            <dd>{{ task.current_stage.stage_key }}</dd>
          </div>
          <div>
            <dt>Attempt</dt>
            <dd>{{ task.attempt }}</dd>
          </div>
          <div>
            <dt>进度</dt>
            <dd>{{ task.progress.completed }} / {{ task.progress.total }}</dd>
          </div>
          <div>
            <dt>资源版本</dt>
            <dd>{{ task.resource_version }}</dd>
          </div>
          <div>
            <dt>作用范围</dt>
            <dd>
              <code>{{ scope(task) }}</code>
            </dd>
          </div>
          <div>
            <dt>创建时间</dt>
            <dd>{{ formatTime(task.created_at_unix_ms) }}</dd>
          </div>
          <div>
            <dt>更新时间</dt>
            <dd>{{ formatTime(task.updated_at_unix_ms) }}</dd>
          </div>
          <div class="definition-grid__wide">
            <dt>Request ID</dt>
            <dd>
              <code>{{ task.request_id }}</code>
            </dd>
          </div>
        </dl>
        <el-alert v-if="task.issue" type="error" :closable="false" :title="task.issue.code">
          {{ task.issue.message }}<span v-if="task.issue.detail">：{{ task.issue.detail }}</span>
        </el-alert>
      </section>

      <section class="content-section">
        <div class="section-heading">
          <div>
            <h2>执行 Attempts</h2>
            <p>{{ attempts.length }} 次执行记录</p>
          </div>
        </div>
        <el-empty v-if="!attempts.length" description="暂无 Attempt 记录" :image-size="60" />
        <el-table v-else :data="attempts" size="small">
          <el-table-column prop="attempt" label="#" width="70" />
          <el-table-column label="状态" width="120"
            ><template #default="slotProps"
              ><el-tag :type="stateType(slotProps.row.state)" effect="plain">{{
                stateLabel(slotProps.row.state)
              }}</el-tag></template
            ></el-table-column
          >
          <el-table-column prop="current_stage_key" label="当前阶段" min-width="130" />
          <el-table-column label="更新时间" min-width="180"
            ><template #default="slotProps">{{
              formatTime(slotProps.row.updated_at_unix_ms)
            }}</template></el-table-column
          >
        </el-table>
      </section>

      <section class="content-section task-events">
        <div class="section-heading">
          <div>
            <h2>事件时间线</h2>
            <p>追加写入的任务审计事件</p>
          </div>
        </div>
        <el-empty v-if="!events.length" description="暂无事件" :image-size="60" />
        <ol v-else class="event-timeline">
          <li v-for="event in events" :key="event.event_id">
            <div class="event-timeline__marker" />
            <div class="event-timeline__body">
              <div class="event-timeline__heading">
                <strong>{{ eventLabel(event) }}</strong
                ><time>{{ formatTime(event.occurred_at_unix_ms) }}</time>
              </div>
              <p v-if="event.message">{{ event.message }}</p>
              <small>Attempt {{ event.attempt }} · {{ event.actor }} · #{{ event.sequence }}</small>
            </div>
          </li>
        </ol>
      </section>

      <section v-if="stages.length" class="content-section">
        <div class="section-heading">
          <div>
            <h2>执行阶段</h2>
            <p>{{ stages.length }} 个阶段</p>
          </div>
        </div>
        <el-table :data="stages" size="small">
          <el-table-column prop="stage_key" label="阶段" min-width="190" />
          <el-table-column prop="stage_kind" label="类型" min-width="190" />
          <el-table-column label="状态" width="120"
            ><template #default="slotProps"
              ><el-tag :type="stateType(slotProps.row.state)" effect="plain">{{
                stateLabel(slotProps.row.state)
              }}</el-tag></template
            ></el-table-column
          >
          <el-table-column prop="stage_attempt" label="阶段 Attempt" width="120" />
        </el-table>
      </section>
    </div>

    <div v-else-if="taskQuery.isPending.value" class="page-loading">
      <el-skeleton :rows="8" animated />
    </div>
  </div>
</template>

<style scoped>
.task-detail-grid {
  display: grid;
  gap: 18px;
}

.task-overview :deep(.el-alert) {
  margin-top: 18px;
}

.event-timeline {
  list-style: none;
  margin: 0;
  padding: 0 0 0 12px;
}

.event-timeline li {
  display: grid;
  grid-template-columns: 12px minmax(0, 1fr);
  column-gap: 14px;
  position: relative;
  padding: 0 0 20px;
}

.event-timeline li:not(:last-child)::before {
  background: var(--el-border-color);
  content: '';
  left: 5px;
  position: absolute;
  top: 12px;
  bottom: 0;
  width: 1px;
}

.event-timeline__marker {
  background: var(--el-color-primary);
  border-radius: 50%;
  height: 10px;
  margin-top: 3px;
  width: 10px;
  z-index: 1;
}

.event-timeline__heading {
  display: flex;
  gap: 14px;
  justify-content: space-between;
}

.event-timeline__heading time,
.event-timeline__body small {
  color: var(--el-text-color-secondary);
}

.event-timeline__body p {
  margin: 5px 0;
}

@media (max-width: 700px) {
  .event-timeline__heading {
    align-items: flex-start;
    flex-direction: column;
    gap: 4px;
  }
}
</style>
