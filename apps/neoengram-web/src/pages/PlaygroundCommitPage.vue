<script setup lang="ts">
import {
  ArrowRight,
  Back,
  CircleCheck,
  CircleClose,
  Files,
  Plus,
  RefreshRight,
  Search,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  cancelPlaygroundPreCommit,
  commitPlayground,
  queryPlayground,
  queryPlaygroundChangeList,
  queryPlaygroundPreCommit,
  restartPlaygroundPreCommit,
  startPlaygroundPreCommit,
} from '@/api/operations';
import { isApiProblem } from '@/api/problem';
import type {
  CancelPreCommitRequest,
  CommitPlaygroundRequest,
  CommitPlaygroundResponse,
  PlaygroundChangeEntry,
  RestartPreCommitRequest,
  StartPreCommitRequest,
} from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  canCommitPreCommit,
  preCommitPhaseLabels,
  preCommitPollInterval,
  preCommitStateLabels,
  preCommitStateTagType,
} from '@/features/precommit/status';
import { useTenantsStore } from '@/stores/tenants';
import { formatBytes, formatCount } from '@/utils/format';

type ChangeFilter = 'all' | PlaygroundChangeEntry['change_type'];

interface RedetectOperation {
  cancel: CancelPreCommitRequest;
  start: StartPreCommitRequest;
}

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const playgroundId = computed(() => String(route.params.playgroundId ?? ''));
const playgroundKey = computed(
  () =>
    ['playground', tenantId.value, projectId.value, artifactId.value, playgroundId.value] as const,
);

const playgroundQuery = useQuery({
  queryKey: playgroundKey,
  queryFn: () =>
    queryPlayground(tenantId.value, projectId.value, artifactId.value, playgroundId.value),
});
const playground = computed(() => playgroundQuery.data.value?.data.playground);
const activePreCommitId = computed(() => playground.value?.active_precommit_id ?? '');
const routedPreCommitId = computed(() => String(route.query.precommit_id ?? ''));
const retainedPreCommitId = ref(routedPreCommitId.value);
const currentPreCommitId = computed(
  () => activePreCommitId.value || routedPreCommitId.value || retainedPreCommitId.value,
);
watch(
  currentPreCommitId,
  (precommitId) => {
    if (precommitId) retainedPreCommitId.value = precommitId;
  },
  { immediate: true },
);
const preCommitKey = computed(
  () =>
    [
      'playground-precommit',
      tenantId.value,
      projectId.value,
      artifactId.value,
      playgroundId.value,
      currentPreCommitId.value,
    ] as const,
);
const preCommitQuery = useQuery({
  queryKey: preCommitKey,
  queryFn: async () => {
    const result = await queryPlaygroundPreCommit(tenantId.value, currentPreCommitId.value);
    const item = result.data.precommit;
    if (
      item.tenant_id !== tenantId.value ||
      item.project_id !== projectId.value ||
      item.artifact_id !== artifactId.value ||
      item.playground_id !== playgroundId.value
    ) {
      throw new Error('Pre-commit 不属于当前 Playground');
    }
    return result;
  },
  enabled: computed(() => Boolean(currentPreCommitId.value)),
  refetchInterval: (query) => preCommitPollInterval(query.state.data?.data.precommit.state),
});
const precommit = computed(() => preCommitQuery.data.value?.data.precommit);

const changeType = ref<ChangeFilter>('all');
const changePathInput = ref('');
const changePathPrefix = ref('');
const changeCursor = ref<string>();
const changeCursorHistory = ref<string[]>([]);
const frozenChangesQuery = useQuery({
  queryKey: computed(
    () =>
      [
        'playground-changes',
        tenantId.value,
        projectId.value,
        artifactId.value,
        playgroundId.value,
        'precommit',
        currentPreCommitId.value,
        precommit.value?.attempt ?? 0,
        precommit.value?.source_index_version.revision ?? '',
        precommit.value?.source_index_version.digest ?? '',
        changeType.value,
        changePathPrefix.value,
        changeCursor.value ?? '',
      ] as const,
  ),
  queryFn: () =>
    queryPlaygroundChangeList({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      playground_id: playgroundId.value,
      precommit_id: currentPreCommitId.value,
      page_size: 50,
      ...(changeType.value !== 'all' ? { change_type: changeType.value } : {}),
      ...(changePathPrefix.value ? { path_prefix: changePathPrefix.value } : {}),
      ...(changeCursor.value ? { cursor: changeCursor.value } : {}),
    }),
  enabled: computed(() => Boolean(precommit.value)),
});

const commitDialogOpen = ref(false);
const commitMessage = ref('');
const commitDescription = ref('');
const tagInput = ref('');
const tagNames = ref<string[]>([]);
const commitError = ref('');
const createdCommit = ref<CommitPlaygroundResponse['commit']>();
const createdParentCommitId = ref('');
const createdIndexRevision = ref('');
const commitReplayed = ref(false);
const pendingCommitRequest = ref<CommitPlaygroundRequest>();
const pendingCancelRequest = ref<CancelPreCommitRequest>();
const pendingRestartRequest = ref<RestartPreCommitRequest>();
const pendingRedetectOperation = ref<RedetectOperation>();

const commitMutation = useMutation({ mutationFn: commitPlayground });
const cancelMutation = useMutation({ mutationFn: cancelPlaygroundPreCommit });
const restartMutation = useMutation({ mutationFn: restartPlaygroundPreCommit });
const redetectMutation = useMutation({
  mutationFn: async (operation: RedetectOperation) => {
    await cancelPlaygroundPreCommit(operation.cancel);
    return startPlaygroundPreCommit(operation.start);
  },
});

const hasCommitPermission = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('commit.create') ?? false,
);
const preCommitReady = computed(() => canCommitPreCommit(precommit.value));
const canCreateCommit = computed(() => preCommitReady.value && hasCommitPermission.value);
const commitCreated = computed(() => Boolean(createdCommit.value));
const diffSummary = computed(
  () => precommit.value?.diff_summary ?? frozenChangesQuery.data.value?.data.summary,
);
const canRedetect = computed(
  () => precommit.value?.state === 'running' || precommit.value?.state === 'ready',
);
const canRetry = computed(
  () => precommit.value?.state === 'abnormal' || precommit.value?.state === 'cancelled',
);

function resetChangeCursor(): void {
  changeCursor.value = undefined;
  changeCursorHistory.value = [];
}

watch(changeType, resetChangeCursor);
watch([tenantId, projectId, artifactId, playgroundId], () => {
  retainedPreCommitId.value = routedPreCommitId.value;
  resetChangeCursor();
  pendingCommitRequest.value = undefined;
  pendingCancelRequest.value = undefined;
  pendingRestartRequest.value = undefined;
  pendingRedetectOperation.value = undefined;
  commitDialogOpen.value = false;
  commitMessage.value = '';
  commitDescription.value = '';
  tagInput.value = '';
  tagNames.value = [];
  commitError.value = '';
  createdCommit.value = undefined;
  createdParentCommitId.value = '';
  createdIndexRevision.value = '';
  commitReplayed.value = false;
  commitMutation.reset();
  cancelMutation.reset();
  restartMutation.reset();
  redetectMutation.reset();
});
watch(
  [
    currentPreCommitId,
    () => precommit.value?.attempt,
    () => precommit.value?.source_index_version.revision,
    () => precommit.value?.source_index_version.digest,
  ],
  () => {
    resetChangeCursor();
    pendingCommitRequest.value = undefined;
    pendingCancelRequest.value = undefined;
    pendingRestartRequest.value = undefined;
    pendingRedetectOperation.value = undefined;
    commitError.value = '';
    commitMutation.reset();
    cancelMutation.reset();
    restartMutation.reset();
    redetectMutation.reset();
  },
);
watch(
  [commitMessage, commitDescription, tagNames],
  () => {
    pendingCommitRequest.value = undefined;
    commitError.value = '';
  },
  { deep: true },
);

function applyChangeFilters(): void {
  changePathPrefix.value = changePathInput.value.trim();
  resetChangeCursor();
}

function nextChangePage(): void {
  const next = frozenChangesQuery.data.value?.data.next_cursor;
  if (!next) return;
  changeCursorHistory.value.push(changeCursor.value ?? '');
  changeCursor.value = next;
}

function previousChangePage(): void {
  changeCursor.value = changeCursorHistory.value.pop() || undefined;
}

function changeTypeLabel(type: PlaygroundChangeEntry['change_type']): string {
  return { added: '新增', modified: '修改', deleted: '删除', renamed: '重命名' }[type];
}

function changeTagType(
  type: PlaygroundChangeEntry['change_type'],
): 'success' | 'warning' | 'danger' | 'info' {
  if (type === 'added') return 'success';
  if (type === 'modified') return 'warning';
  if (type === 'deleted') return 'danger';
  return 'info';
}

function changeSize(row: PlaygroundChangeEntry): string {
  return formatBytes(row.new_size_bytes ?? row.old_size_bytes);
}

function addTag(): boolean {
  const value = tagInput.value.trim();
  if (!value) return true;
  if (!/^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$/.test(value) || value.startsWith('refs/')) {
    commitError.value = 'Tag 必须以字母或数字开头，且只能包含字母、数字、点、横线、下划线和斜线';
    return false;
  }
  if (!tagNames.value.includes(value)) tagNames.value.push(value);
  tagInput.value = '';
  pendingCommitRequest.value = undefined;
  commitError.value = '';
  return true;
}

function removeTag(tagName: string): void {
  tagNames.value = tagNames.value.filter((item) => item !== tagName);
}

function openCommitDialog(): void {
  if (!canCreateCommit.value) return;
  commitError.value = '';
  commitDialogOpen.value = true;
}

function onCommitDialogClosed(): void {
  pendingCommitRequest.value = undefined;
  commitMutation.reset();
}

async function refreshAuthoritativeState(): Promise<void> {
  await playgroundQuery.refetch();
  if (currentPreCommitId.value) await preCommitQuery.refetch();
}

async function pinPreCommitInUrl(precommitId: string): Promise<void> {
  if (routedPreCommitId.value === precommitId) return;
  await router.replace({
    query: { ...route.query, precommit_id: precommitId },
  });
}

async function redetectPreCommit(): Promise<void> {
  if (redetectMutation.isPending.value) return;
  const current = playground.value;
  const currentPreCommit = precommit.value;
  if (!current || !currentPreCommit || !canRedetect.value) return;
  try {
    await ElMessageBox.confirm(
      '当前候选结果会被取消，并基于最新 IndexVersion 创建新的 Pre-commit。',
      '重新检测',
      {
        confirmButtonText: '重新检测',
        cancelButtonText: '保留当前结果',
        type: 'warning',
      },
    );
  } catch {
    pendingRedetectOperation.value = undefined;
    redetectMutation.reset();
    return;
  }
  pendingRedetectOperation.value ??= {
    cancel: {
      tenant_id: tenantId.value,
      precommit_id: currentPreCommit.precommit_id,
      cancel_request_id: `cancel-request-${globalThis.crypto.randomUUID()}`,
    },
    start: {
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      playground_id: playgroundId.value,
      precommit_request_id: `precommit-request-${globalThis.crypto.randomUUID()}`,
      expected_index_version: current.index_version,
    },
  };
  try {
    await redetectMutation.mutateAsync(pendingRedetectOperation.value);
  } catch {
    return;
  }
  pendingRedetectOperation.value = undefined;
  pendingCommitRequest.value = undefined;
  commitDialogOpen.value = false;
  await refreshAuthoritativeState();
  if (activePreCommitId.value) await pinPreCommitInUrl(activePreCommitId.value);
  await queryClient.invalidateQueries({ queryKey: ['playgrounds', tenantId.value] });
  ElMessage.success('新的 Pre-commit 已发起');
}

async function retryPreCommit(): Promise<void> {
  if (restartMutation.isPending.value) return;
  const current = playground.value;
  const currentPreCommit = precommit.value;
  if (!current || !currentPreCommit || !canRetry.value) return;
  pendingRestartRequest.value ??= {
    tenant_id: tenantId.value,
    precommit_id: currentPreCommit.precommit_id,
    restart_request_id: `restart-request-${globalThis.crypto.randomUUID()}`,
    expected_index_version: current.index_version,
  };
  try {
    await restartMutation.mutateAsync(pendingRestartRequest.value);
  } catch {
    return;
  }
  pendingRestartRequest.value = undefined;
  await refreshAuthoritativeState();
  ElMessage.success(`Pre-commit ${currentPreCommit.precommit_id} 已重试`);
}

async function cancelPreCommit(): Promise<void> {
  if (cancelMutation.isPending.value) return;
  const currentPreCommit = precommit.value;
  if (!currentPreCommit || currentPreCommit.state === 'committed') return;
  try {
    await ElMessageBox.confirm(
      '取消后会丢弃尚未提交的 Candidate，不会修改 Playground 文件。',
      '取消 Pre-commit',
      {
        confirmButtonText: '确认取消',
        cancelButtonText: '继续处理',
        type: 'warning',
      },
    );
  } catch {
    pendingCancelRequest.value = undefined;
    cancelMutation.reset();
    return;
  }
  await pinPreCommitInUrl(currentPreCommit.precommit_id);
  pendingCancelRequest.value ??= {
    tenant_id: tenantId.value,
    precommit_id: currentPreCommit.precommit_id,
    cancel_request_id: `cancel-request-${globalThis.crypto.randomUUID()}`,
  };
  try {
    await cancelMutation.mutateAsync(pendingCancelRequest.value);
  } catch {
    return;
  }
  pendingCancelRequest.value = undefined;
  await refreshAuthoritativeState();
  await queryClient.invalidateQueries({ queryKey: ['playgrounds', tenantId.value] });
  ElMessage.success('Pre-commit 已取消');
}

async function createCommit(): Promise<void> {
  if (commitMutation.isPending.value) return;
  commitError.value = '';
  if (!commitMessage.value.trim()) {
    commitError.value = '请输入 Commit 标题';
    return;
  }
  if (!addTag()) return;
  const current = playground.value;
  const currentPreCommit = precommit.value;
  if (!current || !canCreateCommit.value || !currentPreCommit?.candidate_index_version) return;
  createdParentCommitId.value = current.head_commit_id ?? '';
  pendingCommitRequest.value ??= {
    tenant_id: tenantId.value,
    project_id: projectId.value,
    artifact_id: artifactId.value,
    playground_id: playgroundId.value,
    commit_request_id: `commit-request-${globalThis.crypto.randomUUID()}`,
    precommit_id: currentPreCommit.precommit_id,
    expected_candidate_index_version: currentPreCommit.candidate_index_version,
    message: commitMessage.value.trim(),
    ...(commitDescription.value.trim() ? { description: commitDescription.value.trim() } : {}),
    ...(tagNames.value.length ? { tag_names: tagNames.value } : {}),
  };
  try {
    const result = await commitMutation.mutateAsync(pendingCommitRequest.value);
    createdCommit.value = result.data.commit;
    createdIndexRevision.value = result.data.playground.index_version.revision;
    commitReplayed.value = result.data.replayed;
    pendingCommitRequest.value = undefined;
    commitDialogOpen.value = false;
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ['artifacts', tenantId.value] }),
      queryClient.invalidateQueries({
        queryKey: ['artifact', tenantId.value, projectId.value, artifactId.value],
      }),
      queryClient.invalidateQueries({
        queryKey: ['artifact-commits', tenantId.value, projectId.value, artifactId.value],
      }),
      queryClient.invalidateQueries({ queryKey: ['playgrounds', tenantId.value] }),
      queryClient.invalidateQueries({ queryKey: playgroundKey.value }),
      queryClient.invalidateQueries({ queryKey: ['playground-precommit', tenantId.value] }),
    ]);
    ElMessage.success(result.data.replayed ? 'Commit 请求已重放' : 'Commit 已创建');
  } catch (error) {
    if (isApiProblem(error) && error.status === 409) {
      commitError.value =
        'Playground Head 或候选版本已经变化。表单内容已保留，请重新检测后再提交。';
      await refreshAuthoritativeState();
      return;
    }
    commitError.value = error instanceof Error ? error.message : 'Commit 创建失败';
  }
}

async function backToPlayground(): Promise<void> {
  await router.push({
    name: 'playground-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      playgroundId: playgroundId.value,
    },
  });
}

async function openVersionHistory(): Promise<void> {
  if (!createdCommit.value) return;
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: projectId.value, artifactId: artifactId.value },
    query: { tab: 'commits', commit_id: createdCommit.value.commit_id },
  });
}

async function createSnapshot(): Promise<void> {
  if (!createdCommit.value) return;
  await router.push({
    name: 'snapshot-create',
    params: { tenantId: tenantId.value, projectId: projectId.value, artifactId: artifactId.value },
    query: { commit_id: createdCommit.value.commit_id },
  });
}
</script>

<template>
  <div class="page commit-page">
    <PageHeading
      title="提交 Playground"
      :description="`${projectId} / ${artifactId} / ${playgroundId}`"
    >
      <template #actions>
        <el-button :icon="Back" @click="backToPlayground">返回 Playground</el-button>
        <el-button
          v-if="precommit && !['cancelled', 'committed'].includes(precommit.state)"
          :icon="CircleClose"
          type="danger"
          plain
          :loading="cancelMutation.isPending.value"
          @click="cancelPreCommit"
        >
          取消 Pre-commit
        </el-button>
        <el-button
          :icon="RefreshRight"
          :loading="playgroundQuery.isFetching.value || preCommitQuery.isFetching.value"
          @click="refreshAuthoritativeState"
        >
          刷新
        </el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="playgroundQuery.error.value"
      :error="playgroundQuery.error.value"
      :retrying="playgroundQuery.isFetching.value"
      @retry="playgroundQuery.refetch"
    />
    <ApiProblemAlert
      v-if="preCommitQuery.error.value"
      :error="preCommitQuery.error.value"
      :retrying="preCommitQuery.isFetching.value"
      @retry="preCommitQuery.refetch"
    />
    <ApiProblemAlert
      v-if="redetectMutation.error.value"
      :error="redetectMutation.error.value"
      :retrying="redetectMutation.isPending.value"
      @retry="redetectPreCommit"
    />
    <ApiProblemAlert
      v-if="restartMutation.error.value"
      :error="restartMutation.error.value"
      :retrying="restartMutation.isPending.value"
      @retry="retryPreCommit"
    />
    <ApiProblemAlert
      v-if="cancelMutation.error.value"
      :error="cancelMutation.error.value"
      :retrying="cancelMutation.isPending.value"
      @retry="cancelPreCommit"
    />

    <el-skeleton v-if="playgroundQuery.isPending.value" :rows="7" animated />
    <template v-else-if="playground">
      <section class="commit-context" aria-label="当前 Playground 上下文">
        <div class="commit-context__identity">
          <span><Files /></span>
          <div>
            <small>Artifact / Playground</small>
            <strong>{{ artifactId }} / {{ playgroundId }}</strong>
            <code>{{ tenantId }} / {{ projectId }}</code>
          </div>
        </div>
        <dl>
          <div>
            <dt>Region</dt>
            <dd>{{ playground.region }}</dd>
          </div>
          <div>
            <dt>StorageVolume</dt>
            <dd>
              <code>{{ playground.storage_volume_id }}</code>
            </dd>
          </div>
          <div>
            <dt>IndexVersion</dt>
            <dd>revision {{ playground.index_version.revision }}</dd>
          </div>
          <div>
            <dt>当前 Head</dt>
            <dd>
              <code>{{ playground.head_commit_id ?? '尚无 Commit' }}</code>
            </dd>
          </div>
        </dl>
      </section>

      <section v-if="!currentPreCommitId && !commitCreated" class="empty-precommit">
        <WarningFilled />
        <div>
          <h2>没有活动 Pre-commit</h2>
          <p>刷新此页面不会创建任务。请返回 Playground 并显式发起 Pre-commit。</p>
        </div>
        <el-button type="primary" @click="backToPlayground">返回 Playground</el-button>
      </section>

      <template v-else-if="precommit && !commitCreated">
        <section class="preflight-status" :class="`is-${precommit.state}`">
          <span class="preflight-status__icon">
            <CircleCheck v-if="precommit.state === 'ready' || precommit.state === 'committed'" />
            <CircleClose
              v-else-if="precommit.state === 'abnormal' || precommit.state === 'cancelled'"
            />
            <RefreshRight v-else class="is-spinning" />
          </span>
          <div class="preflight-status__body">
            <small>{{ precommit.precommit_id }} · attempt {{ precommit.attempt }}</small>
            <strong
              >{{ preCommitStateLabels[precommit.state] }} ·
              {{ preCommitPhaseLabels[precommit.phase] }}</strong
            >
            <p>
              {{ formatCount(precommit.progress.files_completed) }} /
              {{ formatCount(precommit.progress.files_total) }} 文件，
              {{ formatBytes(precommit.progress.bytes_completed) }} /
              {{ formatBytes(precommit.progress.bytes_total) }}
            </p>
            <div class="progress-track" aria-label="Pre-commit 进度">
              <span :style="{ width: `${precommit.progress.percent}%` }" />
            </div>
          </div>
          <strong>{{ precommit.progress.percent }}%</strong>
          <el-tag :type="preCommitStateTagType(precommit.state)" effect="plain">
            {{ preCommitStateLabels[precommit.state] }}
          </el-tag>
        </section>

        <el-alert
          v-if="precommit.issue"
          :title="precommit.issue.message"
          :type="precommit.issue.retryable ? 'warning' : 'error'"
          :closable="false"
          show-icon
        />

        <section class="operation-actions">
          <span>状态和进度来自服务端；只有 running 状态会自动刷新。</span>
          <el-button
            v-if="canRetry"
            :icon="RefreshRight"
            :loading="restartMutation.isPending.value"
            @click="retryPreCommit"
          >
            失败重试
          </el-button>
          <el-button
            v-if="canRedetect"
            :icon="RefreshRight"
            :loading="redetectMutation.isPending.value"
            @click="redetectPreCommit"
          >
            重新检测
          </el-button>
          <el-button
            v-if="canCreateCommit"
            type="primary"
            :icon="ArrowRight"
            @click="openCommitDialog"
          >
            填写 Commit 信息
          </el-button>
        </section>

        <div v-if="diffSummary" class="change-metrics" aria-label="冻结变化摘要">
          <div>
            <span>新增</span><strong>{{ formatCount(diffSummary.files_added) }}</strong>
          </div>
          <div>
            <span>修改</span><strong>{{ formatCount(diffSummary.files_modified) }}</strong>
          </div>
          <div>
            <span>删除</span><strong>{{ formatCount(diffSummary.files_deleted) }}</strong>
          </div>
          <div>
            <span>重命名</span><strong>{{ formatCount(diffSummary.files_renamed) }}</strong>
          </div>
          <div>
            <span>增加</span><strong>{{ formatBytes(diffSummary.bytes_added) }}</strong>
          </div>
          <div>
            <span>移除</span><strong>{{ formatBytes(diffSummary.bytes_removed) }}</strong>
          </div>
        </div>

        <div class="review-layout">
          <main class="review-panel">
            <header class="panel-heading">
              <div>
                <small>PRE-COMMIT DIFF</small>
                <h2>冻结的逻辑文件变化</h2>
                <p>
                  <code>{{ precommit.precommit_id }}</code> · revision
                  {{ precommit.source_index_version.revision }}
                </p>
              </div>
              <el-tag :type="precommit.blockers.length ? 'danger' : 'success'" effect="plain">
                {{ precommit.blockers.length }} 项阻断
              </el-tag>
            </header>
            <form class="diff-toolbar" @submit.prevent="applyChangeFilters">
              <el-select v-model="changeType" aria-label="变化类型">
                <el-option label="全部变化" value="all" />
                <el-option label="新增" value="added" />
                <el-option label="修改" value="modified" />
                <el-option label="删除" value="deleted" />
                <el-option label="重命名" value="renamed" />
              </el-select>
              <el-input v-model="changePathInput" clearable placeholder="路径前缀" />
              <el-button native-type="submit" :icon="Search">查询</el-button>
            </form>
            <ApiProblemAlert
              v-if="frozenChangesQuery.error.value"
              :error="frozenChangesQuery.error.value"
              :retrying="frozenChangesQuery.isFetching.value"
              @retry="frozenChangesQuery.refetch"
            />
            <el-skeleton v-if="frozenChangesQuery.isPending.value" :rows="6" animated />
            <el-empty
              v-else-if="!frozenChangesQuery.data.value?.data.items.length"
              description="当前筛选下没有变化"
              :image-size="68"
            />
            <template v-else>
              <el-table :data="frozenChangesQuery.data.value?.data.items">
                <el-table-column label="变化" width="86">
                  <template #default="scope">
                    <el-tag
                      :type="changeTagType(scope.row.change_type)"
                      size="small"
                      effect="plain"
                    >
                      {{ changeTypeLabel(scope.row.change_type) }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column label="逻辑路径" min-width="300">
                  <template #default="scope">
                    <div class="path-cell">
                      <code>{{ scope.row.path }}</code>
                      <small v-if="scope.row.previous_path"
                        >原路径 {{ scope.row.previous_path }}</small
                      >
                    </div>
                  </template>
                </el-table-column>
                <el-table-column prop="format" label="格式" min-width="100" />
                <el-table-column label="大小" min-width="110">
                  <template #default="scope">{{ changeSize(scope.row) }}</template>
                </el-table-column>
              </el-table>
              <PageCursor
                :has-previous="changeCursorHistory.length > 0"
                :has-next="Boolean(frozenChangesQuery.data.value?.data.next_cursor)"
                :loading="frozenChangesQuery.isFetching.value"
                @previous="previousChangePage"
                @next="nextChangePage"
              />
            </template>
          </main>

          <aside class="review-aside">
            <section>
              <h3>提交检查</h3>
              <el-empty v-if="!precommit.checks.length" description="没有检查项" :image-size="48" />
              <ul v-else class="check-list">
                <li v-for="check in precommit.checks" :key="check.check_id">
                  <CircleCheck v-if="check.status === 'passed'" />
                  <WarningFilled
                    v-else-if="check.status === 'warning' || check.status === 'pending'"
                  />
                  <CircleClose v-else />
                  <span
                    ><strong>{{ check.summary }}</strong
                    ><small>{{ check.status }}</small></span
                  >
                </li>
              </ul>
            </section>
            <section v-if="precommit.warnings.length">
              <h3>警告</h3>
              <ul class="notice-list">
                <li
                  v-for="warning in precommit.warnings"
                  :key="`${warning.code}/${warning.path ?? ''}`"
                >
                  <code>{{ warning.code }}</code>
                  <span>{{ warning.message }}</span>
                  <small v-if="warning.path">{{ warning.path }}</small>
                </li>
              </ul>
            </section>
            <section v-if="precommit.blockers.length" class="blocker-section">
              <h3>阻断项</h3>
              <ul class="notice-list">
                <li
                  v-for="blocker in precommit.blockers"
                  :key="`${blocker.code}/${blocker.path ?? ''}`"
                >
                  <code>{{ blocker.code }}</code>
                  <span>{{ blocker.message }}</span>
                  <small v-if="blocker.path">{{ blocker.path }}</small>
                </li>
              </ul>
            </section>
            <section>
              <h3>版本上下文</h3>
              <dl class="version-context">
                <div>
                  <dt>Head</dt>
                  <dd>
                    <code>{{ playground.head_commit_id ?? '根 Commit' }}</code>
                  </dd>
                </div>
                <div>
                  <dt>Source revision</dt>
                  <dd>{{ precommit.source_index_version.revision }}</dd>
                </div>
                <div>
                  <dt>Candidate revision</dt>
                  <dd>{{ precommit.candidate_index_version?.revision ?? '—' }}</dd>
                </div>
              </dl>
            </section>
          </aside>
        </div>
      </template>

      <section
        v-else-if="currentPreCommitId && preCommitQuery.isPending.value && !commitCreated"
        class="page-loading"
      >
        <el-skeleton :rows="8" animated />
      </section>

      <section v-if="createdCommit" class="commit-result">
        <span class="commit-result__icon"><CircleCheck /></span>
        <div>
          <small>{{ commitReplayed ? 'REPLAYED' : 'COMMIT CREATED' }}</small>
          <h2>{{ createdCommit.message }}</h2>
          <p>不可变版本已发布，Artifact 的版本历史已经更新。</p>
        </div>
        <dl>
          <div>
            <dt>Commit ID</dt>
            <dd>
              <code>{{ createdCommit.commit_id }}</code>
            </dd>
          </div>
          <div>
            <dt>Parent</dt>
            <dd>
              <code>{{ createdParentCommitId || '根 Commit' }}</code>
            </dd>
          </div>
          <div>
            <dt>Tags</dt>
            <dd class="tag-list">
              <el-tag v-for="tagName in createdCommit.tag_names" :key="tagName" effect="plain">{{
                tagName
              }}</el-tag>
              <span v-if="!createdCommit.tag_names.length">—</span>
            </dd>
          </div>
          <div>
            <dt>IndexVersion</dt>
            <dd>revision {{ createdIndexRevision }}</dd>
          </div>
        </dl>
        <div class="result-next">
          <div>
            <strong>创建只读 Snapshot</strong>
            <p>选择目标 StorageVolume，将该 Commit 交付到指定 Region。</p>
          </div>
          <el-button type="primary" :icon="ArrowRight" @click="createSnapshot"
            >创建 Snapshot</el-button
          >
        </div>
        <div class="result-actions">
          <el-button @click="backToPlayground">返回 Playground</el-button>
          <el-button @click="openVersionHistory">查看版本历史</el-button>
        </div>
      </section>
    </template>

    <el-dialog
      v-model="commitDialogOpen"
      title="创建 Commit"
      width="min(620px, calc(100vw - 32px))"
      :close-on-click-modal="false"
      @closed="onCommitDialogClosed"
    >
      <ApiProblemAlert v-if="commitMutation.error.value" :error="commitMutation.error.value" />
      <el-alert v-if="commitError" :title="commitError" type="error" :closable="false" />
      <section class="dialog-context">
        <div>
          <span>Parent</span><code>{{ playground?.head_commit_id ?? '根 Commit' }}</code>
        </div>
        <div>
          <span>Candidate</span
          ><strong>revision {{ precommit?.candidate_index_version?.revision ?? '—' }}</strong>
        </div>
        <div>
          <span>变化文件</span>
          <strong v-if="diffSummary">
            {{
              formatCount(
                (
                  BigInt(diffSummary.files_added) +
                  BigInt(diffSummary.files_modified) +
                  BigInt(diffSummary.files_deleted) +
                  BigInt(diffSummary.files_renamed)
                ).toString(),
              )
            }}
          </strong>
          <strong v-else>—</strong>
        </div>
      </section>
      <el-form label-position="top" class="commit-form">
        <el-form-item label="Commit 标题" required>
          <el-input
            v-model="commitMessage"
            maxlength="256"
            show-word-limit
            :disabled="commitMutation.isPending.value"
            placeholder="简短描述本次变化"
          />
        </el-form-item>
        <el-form-item label="详细描述">
          <el-input
            v-model="commitDescription"
            type="textarea"
            :rows="4"
            maxlength="2048"
            show-word-limit
            :disabled="commitMutation.isPending.value"
            placeholder="记录变化背景、范围和验证结论"
          />
        </el-form-item>
        <el-form-item label="Tags">
          <div class="tag-editor">
            <div class="tag-editor__input">
              <el-input
                v-model="tagInput"
                aria-label="Commit Tags"
                :disabled="commitMutation.isPending.value"
                placeholder="输入 Tag 后按 Enter"
                @keyup.enter.prevent="addTag"
              />
              <el-tooltip content="添加 Tag" placement="top">
                <el-button
                  :icon="Plus"
                  aria-label="添加 Commit Tag"
                  :disabled="commitMutation.isPending.value"
                  @click="addTag"
                />
              </el-tooltip>
            </div>
            <div class="tag-editor__values">
              <el-tag
                v-for="tagName in tagNames"
                :key="tagName"
                closable
                :disable-transitions="true"
                @close="removeTag(tagName)"
              >
                {{ tagName }}
              </el-tag>
            </div>
          </div>
        </el-form-item>
      </el-form>
      <div class="cas-note">
        <WarningFilled />
        <p>发布前会校验冻结候选与当前 Head；发生冲突时不会覆盖其他提交。</p>
      </div>
      <template #footer>
        <el-button :disabled="commitMutation.isPending.value" @click="commitDialogOpen = false"
          >取消</el-button
        >
        <el-button type="primary" :loading="commitMutation.isPending.value" @click="createCommit"
          >确认 Commit</el-button
        >
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.commit-page {
  width: min(1240px, 100%);
}

.commit-context {
  display: grid;
  grid-template-columns: minmax(300px, 1.1fr) minmax(500px, 1.8fr);
  margin-bottom: 18px;
  border: 1px solid var(--line);
  background: #eef2f0;
}

.commit-context__identity {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 13px;
  padding: 17px 20px;
  border-right: 1px solid var(--line);
}

.commit-context__identity > span {
  flex: 0 0 auto;
  width: 36px;
  height: 36px;
  display: grid;
  place-items: center;
  color: var(--green);
  background: #dcebe4;
}

.commit-context__identity small,
.commit-context__identity strong,
.commit-context__identity code {
  display: block;
  overflow-wrap: anywhere;
}

.commit-context__identity small,
.commit-context__identity code,
.commit-context dt {
  color: var(--muted);
  font-size: 10px;
}

.commit-context__identity strong {
  margin: 3px 0;
  font-size: 13px;
}

.commit-context dl {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin: 0;
}

.commit-context dl > div {
  min-width: 0;
  padding: 13px 16px;
  border-right: 1px solid var(--line);
}

.commit-context dl > div:last-child {
  border-right: 0;
}

.commit-context dd {
  margin: 5px 0 0;
  font-size: 11px;
  font-weight: 650;
  overflow-wrap: anywhere;
}

.empty-precommit {
  display: grid;
  grid-template-columns: 36px minmax(0, 1fr) auto;
  align-items: center;
  gap: 14px;
  padding: 22px;
  border: 1px solid var(--line);
  background: #fff;
}

.empty-precommit > svg {
  width: 26px;
  color: #ad761f;
}

.empty-precommit h2,
.empty-precommit p {
  margin: 0;
}

.empty-precommit h2 {
  font-size: 15px;
}

.empty-precommit p {
  margin-top: 5px;
  color: var(--muted);
  font-size: 11px;
}

.preflight-status {
  display: grid;
  grid-template-columns: 38px minmax(0, 1fr) auto auto;
  align-items: center;
  gap: 13px;
  padding: 16px 18px;
  border: 1px solid #dbc28f;
  border-left: 3px solid #b57413;
  background: #fff9ec;
}

.preflight-status.is-ready,
.preflight-status.is-committed {
  border-color: #aac5b9;
  border-left-color: var(--green);
  background: #edf6f1;
}

.preflight-status.is-abnormal,
.preflight-status.is-cancelled {
  border-color: #d7aaa5;
  border-left-color: #b3473d;
  background: #fff2f0;
}

.preflight-status__icon {
  width: 36px;
  height: 36px;
  display: grid;
  place-items: center;
  color: #9d650f;
  background: #f4e3bd;
}

.preflight-status__body {
  min-width: 0;
}

.preflight-status__body small,
.preflight-status__body strong,
.preflight-status__body p {
  display: block;
  margin: 0;
  overflow-wrap: anywhere;
}

.preflight-status__body small,
.preflight-status__body p {
  color: var(--muted);
  font-size: 10px;
}

.preflight-status__body strong {
  margin: 3px 0;
  font-size: 13px;
}

.progress-track {
  height: 6px;
  margin-top: 9px;
  overflow: hidden;
  background: #e6e9e7;
}

.progress-track span {
  display: block;
  height: 100%;
  background: var(--green);
}

.operation-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 12px;
}

.operation-actions > span {
  margin-right: auto;
  color: var(--muted);
  font-size: 10px;
}

.change-metrics {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  margin-top: 16px;
  border: 1px solid var(--line);
  background: #fff;
}

.change-metrics > div {
  min-width: 0;
  padding: 14px;
  border-right: 1px solid var(--line);
}

.change-metrics > div:last-child {
  border-right: 0;
}

.change-metrics span,
.change-metrics strong {
  display: block;
}

.change-metrics span {
  color: var(--muted);
  font-size: 10px;
}

.change-metrics strong {
  margin-top: 5px;
  font-size: 15px;
  overflow-wrap: anywhere;
}

.review-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 300px;
  gap: 18px;
  margin-top: 18px;
}

.review-panel,
.review-aside > section {
  min-width: 0;
  border: 1px solid var(--line);
  background: #fff;
}

.review-panel {
  padding: 20px;
}

.review-aside {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.review-aside > section {
  padding: 16px;
}

.review-aside h3 {
  margin: 0 0 12px;
  font-size: 13px;
}

.panel-heading {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 14px;
  margin-bottom: 14px;
}

.panel-heading small {
  color: var(--green);
  font-size: 9px;
  font-weight: 800;
}

.panel-heading h2,
.panel-heading p {
  margin: 0;
}

.panel-heading h2 {
  margin-top: 5px;
  font-size: 16px;
}

.panel-heading p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 10px;
  overflow-wrap: anywhere;
}

.diff-toolbar {
  display: grid;
  grid-template-columns: 160px minmax(180px, 1fr) auto;
  gap: 9px;
  margin-bottom: 14px;
}

.path-cell {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 3px;
}

.path-cell code,
.path-cell small {
  overflow-wrap: anywhere;
}

.path-cell small {
  color: var(--muted);
}

.check-list,
.notice-list {
  margin: 0;
  padding: 0;
  list-style: none;
}

.check-list li {
  display: grid;
  grid-template-columns: 18px minmax(0, 1fr);
  gap: 8px;
  padding: 9px 0;
  border-bottom: 1px solid var(--line);
}

.check-list svg {
  width: 15px;
  color: var(--green);
}

.check-list strong,
.check-list small {
  display: block;
}

.check-list strong {
  font-size: 10px;
}

.check-list small,
.notice-list small {
  margin-top: 3px;
  color: var(--muted);
  font-size: 9px;
}

.notice-list li {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 9px 0;
  border-bottom: 1px solid var(--line);
  font-size: 10px;
  overflow-wrap: anywhere;
}

.blocker-section {
  border-color: #d7aaa5 !important;
}

.version-context {
  margin: 0;
}

.version-context > div {
  padding: 8px 0;
  border-bottom: 1px solid var(--line);
}

.version-context dt {
  color: var(--muted);
  font-size: 9px;
}

.version-context dd {
  margin: 3px 0 0;
  font-size: 10px;
  overflow-wrap: anywhere;
}

.commit-result {
  padding: 24px;
  border: 1px solid #a7c5b7;
  background: #f3faf6;
}

.commit-result__icon {
  width: 42px;
  height: 42px;
  display: grid;
  place-items: center;
  color: var(--green);
  background: #d9ebe2;
}

.commit-result h2,
.commit-result p {
  margin: 0;
}

.commit-result h2 {
  margin-top: 5px;
  font-size: 18px;
}

.commit-result p {
  margin-top: 5px;
  color: var(--muted);
  font-size: 11px;
}

.commit-result dl,
.dialog-context {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin: 18px 0;
  border: 1px solid var(--line);
  background: #fff;
}

.commit-result dl > div,
.dialog-context > div {
  min-width: 0;
  padding: 12px;
  border-right: 1px solid var(--line);
}

.commit-result dt,
.dialog-context span {
  color: var(--muted);
  font-size: 9px;
}

.commit-result dd,
.dialog-context code,
.dialog-context strong {
  display: block;
  margin: 4px 0 0;
  overflow-wrap: anywhere;
}

.tag-list {
  display: flex !important;
  flex-wrap: wrap;
  gap: 4px;
}

.result-next {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 16px;
  background: #e8f3ed;
}

.result-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 14px;
}

.tag-editor,
.tag-editor__values {
  width: 100%;
}

.tag-editor__input {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 8px;
}

.tag-editor__values {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 8px;
}

.cas-note {
  display: grid;
  grid-template-columns: 20px minmax(0, 1fr);
  gap: 9px;
  padding: 11px;
  color: #75500f;
  background: #fff7e5;
}

.cas-note svg {
  width: 17px;
}

.cas-note p {
  margin: 0;
  font-size: 10px;
}

@media (max-width: 900px) {
  .commit-context,
  .review-layout {
    grid-template-columns: 1fr;
  }

  .commit-context__identity {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .change-metrics {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 640px) {
  .commit-context dl,
  .commit-result dl,
  .dialog-context,
  .change-metrics,
  .diff-toolbar {
    grid-template-columns: 1fr;
  }

  .preflight-status,
  .empty-precommit {
    grid-template-columns: 1fr;
  }

  .preflight-status__icon,
  .empty-precommit > svg {
    display: none;
  }

  .operation-actions,
  .result-next {
    align-items: stretch;
    flex-direction: column;
  }

  .commit-result dl > div,
  .dialog-context > div,
  .change-metrics > div {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }
}
</style>
