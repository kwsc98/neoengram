<script setup lang="ts">
import { Clock, Lock, Refresh, RefreshLeft, Unlock } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue';
import { useRoute } from 'vue-router';

import {
  createRetentionHold,
  queryDeletion,
  queryDeletionList,
  releaseRetentionHold,
  restoreDeletion,
  retryDeletion,
} from '@/api/operations';
import type {
  CreateRetentionHoldRequest,
  DeletionOperationState,
  DeletionOperationView,
  ReleaseRetentionHoldRequest,
  RetentionHoldView,
  UpdateDeletionRequest,
} from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  deletionCanRestore,
  deletionCanRetry,
  deletionStateLabel,
  deletionStateTagType,
  resourceRefLabel,
  resourceRefScope,
} from '@/features/lifecycle';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

const route = useRoute();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const permissions = computed(() => tenants.byId(tenantId.value)?.permissions ?? []);
const canManage = computed(() => permissions.value.includes('resource.lifecycle.manage' as never));
const canManageRetention = computed(() => permissions.value.includes('retention.manage' as never));
const state = ref<DeletionOperationState | ''>('');
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);
const selectedDeletionId = ref('');
const detailsOpen = ref(false);
const now = ref(Date.now());
let clock: ReturnType<typeof globalThis.setInterval> | undefined;

const holdForm = reactive({ reason: '', expiresAt: undefined as Date | undefined });
const restoreRequests = new Map<string, UpdateDeletionRequest>();
const retryRequests = new Map<string, UpdateDeletionRequest>();
const releaseHoldRequests = new Map<string, ReleaseRetentionHoldRequest>();
const pendingHoldRequest = ref<CreateRetentionHoldRequest>();

const deletionsQuery = useQuery({
  queryKey: computed(() => ['resource-deletions', tenantId.value, state.value, cursor.value ?? '']),
  queryFn: () =>
    queryDeletionList({
      tenant_id: tenantId.value,
      page_size: 50,
      ...(state.value ? { states: [state.value] } : {}),
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
  refetchInterval: 5_000,
  refetchIntervalInBackground: false,
});
const detailQuery = useQuery({
  queryKey: computed(() => ['resource-deletion', tenantId.value, selectedDeletionId.value]),
  queryFn: () =>
    queryDeletion({ tenant_id: tenantId.value, deletion_id: selectedDeletionId.value }),
  enabled: computed(() => detailsOpen.value && Boolean(selectedDeletionId.value)),
  refetchInterval: 5_000,
});
const restoreMutation = useMutation({ mutationFn: restoreDeletion });
const retryMutation = useMutation({ mutationFn: retryDeletion });
const createHoldMutation = useMutation({ mutationFn: createRetentionHold });
const releaseHoldMutation = useMutation({ mutationFn: releaseRetentionHold });

const selectedDeletion = computed(() => detailQuery.data.value?.data.deletion);
const canCreateHold = computed(
  () =>
    Boolean(selectedDeletion.value) &&
    !['purging', 'finalizing', 'completed'].includes(selectedDeletion.value?.state ?? 'completed'),
);
const activeHolds = computed(
  () =>
    detailQuery.data.value?.data.retention_holds.filter((hold) => hold.state === 'active') ?? [],
);

watch([tenantId, state], () => {
  cursor.value = undefined;
  cursorHistory.value = [];
  detailsOpen.value = false;
});

watch([() => holdForm.reason, () => holdForm.expiresAt], () => {
  pendingHoldRequest.value = undefined;
});

onMounted(() => {
  clock = globalThis.setInterval(() => {
    now.value = Date.now();
  }, 30_000);
});

onUnmounted(() => {
  if (clock) globalThis.clearInterval(clock);
});

function nextPage(): void {
  const next = deletionsQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  cursor.value = cursorHistory.value.pop() || undefined;
}

function recoveryCountdown(deletion: DeletionOperationView): string {
  if (['purging', 'finalizing'].includes(deletion.state)) return '已不可恢复';
  if (deletion.state === 'completed') {
    return deletion.completion === 'restored' ? '已恢复' : '已永久清理';
  }
  const remaining = Number(deletion.purge_after_unix_ms) - now.value;
  if (remaining <= 0) return '等待永久清理';
  const hours = Math.ceil(remaining / 3_600_000);
  const days = Math.floor(hours / 24);
  return days > 0 ? `${days} 天 ${hours % 24} 小时` : `${hours} 小时`;
}

async function refresh(): Promise<void> {
  await deletionsQuery.refetch();
}

function openDetails(deletion: DeletionOperationView): void {
  selectedDeletionId.value = deletion.deletion_id;
  pendingHoldRequest.value = undefined;
  createHoldMutation.reset();
  releaseHoldMutation.reset();
  holdForm.reason = '';
  holdForm.expiresAt = undefined;
  detailsOpen.value = true;
}

async function invalidate(deletionId: string): Promise<void> {
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ['resource-deletions', tenantId.value] }),
    queryClient.invalidateQueries({ queryKey: ['resource-deletion', tenantId.value, deletionId] }),
  ]);
}

async function restore(deletion: DeletionOperationView): Promise<void> {
  try {
    await ElMessageBox.confirm(
      '恢复会整批执行；此前取消的 Job 和 Pre-commit 不会自动重启。',
      '确认恢复资源',
      { type: 'warning', confirmButtonText: '整批恢复', cancelButtonText: '取消' },
    );
  } catch {
    return;
  }
  const request =
    restoreRequests.get(deletion.deletion_id) ??
    ({
      tenant_id: tenantId.value,
      deletion_id: deletion.deletion_id,
      expected_resource_version: deletion.resource_version,
      request_id: `resource-restore-${globalThis.crypto.randomUUID()}`,
    } satisfies UpdateDeletionRequest);
  restoreRequests.set(deletion.deletion_id, request);
  try {
    const result = await restoreMutation.mutateAsync(request);
    restoreRequests.delete(deletion.deletion_id);
    await invalidate(deletion.deletion_id);
    ElMessage.success(result.data.request_replayed ? '已返回原恢复任务' : '资源恢复已开始');
  } catch {
    return;
  }
}

async function retry(deletion: DeletionOperationView): Promise<void> {
  const request =
    retryRequests.get(deletion.deletion_id) ??
    ({
      tenant_id: tenantId.value,
      deletion_id: deletion.deletion_id,
      expected_resource_version: deletion.resource_version,
      request_id: `resource-deletion-retry-${globalThis.crypto.randomUUID()}`,
    } satisfies UpdateDeletionRequest);
  retryRequests.set(deletion.deletion_id, request);
  try {
    const result = await retryMutation.mutateAsync(request);
    retryRequests.delete(deletion.deletion_id);
    await invalidate(deletion.deletion_id);
    ElMessage.success(result.data.request_replayed ? '已返回原重试任务' : '删除任务已重新排队');
  } catch {
    return;
  }
}

async function createHold(): Promise<void> {
  const deletion = selectedDeletion.value;
  const reason = holdForm.reason.trim();
  if (!deletion || !reason) {
    ElMessage.warning('请填写保留原因');
    return;
  }
  pendingHoldRequest.value ??= {
    tenant_id: tenantId.value,
    deletion_id: deletion.deletion_id,
    expected_resource_version: deletion.resource_version,
    request_id: `retention-hold-${globalThis.crypto.randomUUID()}`,
    reason,
    ...(holdForm.expiresAt ? { expires_at_unix_ms: holdForm.expiresAt.getTime().toString() } : {}),
  };
  let result;
  try {
    result = await createHoldMutation.mutateAsync(pendingHoldRequest.value);
  } catch {
    return;
  }
  pendingHoldRequest.value = undefined;
  holdForm.reason = '';
  holdForm.expiresAt = undefined;
  await invalidate(deletion.deletion_id);
  ElMessage.success(result.data.request_replayed ? '已返回原保留锁' : '保留锁已创建');
}

async function releaseHold(hold: RetentionHoldView): Promise<void> {
  const deletion = selectedDeletion.value;
  if (!deletion) return;
  const requestKey = `${deletion.deletion_id}/${hold.retention_hold_id}`;
  const releaseRequest =
    releaseHoldRequests.get(requestKey) ??
    ({
      tenant_id: tenantId.value,
      deletion_id: deletion.deletion_id,
      retention_hold_id: hold.retention_hold_id,
      expected_resource_version: deletion.resource_version,
      request_id: `retention-hold-release-${globalThis.crypto.randomUUID()}`,
    } satisfies ReleaseRetentionHoldRequest);
  releaseHoldRequests.set(requestKey, releaseRequest);
  let result;
  try {
    result = await releaseHoldMutation.mutateAsync(releaseRequest);
  } catch {
    return;
  }
  releaseHoldRequests.delete(requestKey);
  await invalidate(deletion.deletion_id);
  ElMessage.success(result.data.request_replayed ? '已返回原释放结果' : '保留锁已释放');
}
</script>

<template>
  <div class="page">
    <PageHeading title="回收站" :description="`${tenantId} 内已停止访问的存储资源`">
      <template #actions>
        <el-button :icon="Refresh" :loading="deletionsQuery.isFetching.value" @click="refresh">
          刷新
        </el-button>
      </template>
    </PageHeading>

    <div class="resource-toolbar recycle-toolbar">
      <el-select v-model="state" clearable placeholder="全部阶段" aria-label="删除任务阶段">
        <el-option label="可恢复" value="recoverable" />
        <el-option label="处理中" value="quarantining" />
        <el-option label="已阻塞" value="blocked" />
        <el-option label="失败" value="failed" />
        <el-option label="已完成" value="completed" />
      </el-select>
    </div>

    <ApiProblemAlert
      v-if="deletionsQuery.error.value"
      :error="deletionsQuery.error.value"
      :retrying="deletionsQuery.isFetching.value"
      @retry="deletionsQuery.refetch"
    />
    <ApiProblemAlert v-if="restoreMutation.error.value" :error="restoreMutation.error.value" />
    <ApiProblemAlert v-if="retryMutation.error.value" :error="retryMutation.error.value" />

    <section class="content-section resource-section">
      <el-skeleton v-if="deletionsQuery.isPending.value" :rows="7" animated />
      <el-empty
        v-else-if="!deletionsQuery.data.value?.data.items.length"
        description="回收站为空"
        :image-size="78"
      />
      <template v-else>
        <el-table
          :data="deletionsQuery.data.value?.data.items"
          class="resource-table desktop-table"
          @row-click="openDetails"
        >
          <el-table-column label="资源" min-width="260">
            <template #default="scope">
              <div class="resource-link resource-link--static">
                <strong>{{ resourceRefLabel(scope.row.root) }}</strong>
                <code>{{ resourceRefScope(scope.row.root) }}</code>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="阶段" min-width="145">
            <template #default="scope">
              <div class="state-stack">
                <el-tag :type="deletionStateTagType(scope.row.state)" effect="plain">
                  {{ deletionStateLabel(scope.row.state) }}
                </el-tag>
                <small v-if="scope.row.last_error">{{ scope.row.last_error }}</small>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="恢复窗口" min-width="150">
            <template #default="scope">
              <span class="countdown"><Clock />{{ recoveryCountdown(scope.row) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="影响" min-width="140">
            <template #default="scope"> {{ scope.row.targets.length }} 个资源 </template>
          </el-table-column>
          <el-table-column label="更新时间" min-width="170">
            <template #default="scope">{{ formatTime(scope.row.updated_at_unix_ms) }}</template>
          </el-table-column>
          <el-table-column v-if="canManage" label="操作" width="150" fixed="right">
            <template #default="scope">
              <div class="row-actions" @click.stop>
                <el-tooltip content="整批恢复" placement="top">
                  <el-button
                    text
                    type="primary"
                    :icon="RefreshLeft"
                    :disabled="!deletionCanRestore(scope.row.state)"
                    :loading="restoreMutation.isPending.value"
                    :aria-label="`恢复 ${scope.row.deletion_id}`"
                    @click="restore(scope.row)"
                  />
                </el-tooltip>
                <el-tooltip content="重试任务" placement="top">
                  <el-button
                    text
                    :icon="Refresh"
                    :disabled="!deletionCanRetry(scope.row.state)"
                    :loading="retryMutation.isPending.value"
                    :aria-label="`重试 ${scope.row.deletion_id}`"
                    @click="retry(scope.row)"
                  />
                </el-tooltip>
              </div>
            </template>
          </el-table-column>
        </el-table>

        <div class="mobile-resource-list">
          <button
            v-for="deletion in deletionsQuery.data.value?.data.items"
            :key="deletion.deletion_id"
            class="mobile-resource-item"
            type="button"
            @click="openDetails(deletion)"
          >
            <span>
              <strong>{{ resourceRefLabel(deletion.root) }}</strong>
              <code>{{ resourceRefScope(deletion.root) }}</code>
              <small>{{ deletion.targets.length }} 个受影响资源</small>
            </span>
            <span>
              <el-tag :type="deletionStateTagType(deletion.state)" size="small" effect="plain">
                {{ deletionStateLabel(deletion.state) }}
              </el-tag>
              <small>{{ recoveryCountdown(deletion) }}</small>
            </span>
          </button>
        </div>

        <PageCursor
          :has-previous="cursorHistory.length > 0"
          :has-next="Boolean(deletionsQuery.data.value?.data.next_cursor)"
          :loading="deletionsQuery.isFetching.value"
          @previous="previousPage"
          @next="nextPage"
        />
      </template>
    </section>

    <el-drawer v-model="detailsOpen" title="删除任务详情" size="min(520px, 100vw)">
      <ApiProblemAlert
        v-if="detailQuery.error.value"
        :error="detailQuery.error.value"
        :retrying="detailQuery.isFetching.value"
        @retry="detailQuery.refetch"
      />
      <ApiProblemAlert
        v-if="createHoldMutation.error.value"
        :error="createHoldMutation.error.value"
      />
      <ApiProblemAlert
        v-if="releaseHoldMutation.error.value"
        :error="releaseHoldMutation.error.value"
      />
      <el-skeleton v-if="detailQuery.isPending.value" :rows="8" animated />
      <template v-else-if="selectedDeletion">
        <div class="deletion-detail-heading">
          <div>
            <small>{{ resourceRefLabel(selectedDeletion.root) }}</small>
            <h2>{{ resourceRefScope(selectedDeletion.root) }}</h2>
            <code>{{ selectedDeletion.deletion_id }}</code>
          </div>
          <el-tag :type="deletionStateTagType(selectedDeletion.state)" effect="plain">
            {{ deletionStateLabel(selectedDeletion.state) }}
          </el-tag>
        </div>

        <dl class="detail-list">
          <div>
            <dt>恢复截止</dt>
            <dd>{{ formatTime(selectedDeletion.purge_after_unix_ms) }}</dd>
          </div>
          <div>
            <dt>剩余时间</dt>
            <dd>{{ recoveryCountdown(selectedDeletion) }}</dd>
          </div>
          <div>
            <dt>受影响资源</dt>
            <dd>{{ selectedDeletion.targets.length }}</dd>
          </div>
          <div>
            <dt>级联删除</dt>
            <dd>{{ selectedDeletion.cascade ? '是' : '否' }}</dd>
          </div>
          <div>
            <dt>重试次数</dt>
            <dd>{{ selectedDeletion.retry_count }}</dd>
          </div>
        </dl>

        <el-alert
          v-if="selectedDeletion.last_error"
          :title="selectedDeletion.last_error"
          type="error"
          :closable="false"
          show-icon
        />

        <section class="drawer-section">
          <div class="section-title">
            <div>
              <small>Retention Hold</small>
              <h3>保留锁</h3>
            </div>
            <el-tag v-if="activeHolds.length" type="warning" effect="plain">
              {{ activeHolds.length }} 个生效中
            </el-tag>
          </div>
          <el-empty v-if="!activeHolds.length" description="没有活动保留锁" :image-size="56" />
          <div v-for="hold in activeHolds" :key="hold.retention_hold_id" class="hold-item">
            <Lock />
            <div>
              <strong>{{ hold.reason }}</strong>
              <small>
                {{
                  hold.expires_at_unix_ms ? `到期 ${formatTime(hold.expires_at_unix_ms)}` : '永久'
                }}
              </small>
            </div>
            <el-button
              v-if="canManageRetention"
              text
              type="danger"
              :icon="Unlock"
              :loading="releaseHoldMutation.isPending.value"
              :aria-label="`释放保留锁 ${hold.retention_hold_id}`"
              @click="releaseHold(hold)"
            />
          </div>

          <el-form
            v-if="canManageRetention && canCreateHold"
            label-position="top"
            class="hold-form"
          >
            <el-form-item label="保留原因" required>
              <el-input
                v-model="holdForm.reason"
                maxlength="512"
                placeholder="合规审计或业务保留原因"
              />
            </el-form-item>
            <el-form-item label="到期时间（留空表示永久）">
              <el-date-picker
                v-model="holdForm.expiresAt"
                type="datetime"
                placeholder="永久保留"
                :disabled-date="(date: Date) => date.getTime() < Date.now() - 86_400_000"
              />
            </el-form-item>
            <el-button
              :icon="Lock"
              :loading="createHoldMutation.isPending.value"
              @click="createHold"
              >创建保留锁</el-button
            >
          </el-form>
        </section>
      </template>
    </el-drawer>
  </div>
</template>

<style scoped>
.recycle-toolbar {
  grid-template-columns: minmax(180px, 280px);
}

.resource-link--static {
  cursor: pointer;
}

.countdown,
.hold-item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.countdown svg,
.hold-item > svg {
  width: 16px;
  flex: 0 0 auto;
}

.deletion-detail-heading,
.section-title {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
}

.deletion-detail-heading h2,
.section-title h3 {
  margin: 3px 0;
  letter-spacing: 0;
}

.deletion-detail-heading code {
  overflow-wrap: anywhere;
}

.detail-list {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
  margin: 24px 0;
}

.detail-list dt,
.section-title small,
.deletion-detail-heading small,
.hold-item small {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.detail-list dd {
  margin: 4px 0 0;
  font-weight: 600;
}

.drawer-section {
  margin-top: 28px;
  border-top: 1px solid var(--el-border-color-lighter);
  padding-top: 20px;
}

.hold-item {
  margin-top: 12px;
  border: 1px solid var(--el-border-color-lighter);
  padding: 12px;
}

.hold-item > div {
  display: grid;
  flex: 1;
  gap: 3px;
  min-width: 0;
}

.hold-form {
  margin-top: 20px;
}

.hold-form .el-date-editor {
  width: 100%;
}

@media (max-width: 640px) {
  .detail-list {
    grid-template-columns: 1fr;
  }
}
</style>
