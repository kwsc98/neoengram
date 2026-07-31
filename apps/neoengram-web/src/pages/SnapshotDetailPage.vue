<script setup lang="ts">
import {
  Back,
  Box,
  CircleCheck,
  Files,
  Location,
  Lock,
  RefreshRight,
  Search,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useMutation, useQuery } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  queryArtifactCommitDiff,
  querySnapshot,
  querySnapshotActivityList,
  querySnapshotDatasetProfile,
  querySnapshotFileList,
  retrySnapshotDelivery,
} from '@/api/operations';
import type { LogicalFileEntry } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  datasetProfileStateLabel,
  datasetProfileTagType,
  snapshotActivityTagType,
  snapshotActivityTypeLabel,
  snapshotIntegrityLabel,
  snapshotIntegrityTagType,
  snapshotPhaseLabel,
  snapshotPollInterval,
  snapshotStateLabel,
  snapshotStateTagType,
} from '@/features/snapshots/status';
import { commitTagNames } from '@/utils/commit';
import { formatBytes, formatCount, formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const snapshotId = computed(() => String(route.params.snapshotId ?? ''));
const allowedTabs = ['overview', 'files', 'activity', 'profile'];
const requestedTab = computed(() => String(route.query.tab ?? 'overview'));
const activeTab = ref(allowedTabs.includes(requestedTab.value) ? requestedTab.value : 'overview');

const filePathInput = ref('');
const filePathPrefix = ref('');
const fileCursor = ref<string>();
const fileCursorHistory = ref<string[]>([]);
const activityCursor = ref<string>();
const activityCursorHistory = ref<string[]>([]);
const retryRequestId = ref<string>();

const snapshotQuery = useQuery({
  queryKey: computed(() => [
    'snapshot',
    tenantId.value,
    projectId.value,
    artifactId.value,
    snapshotId.value,
  ]),
  queryFn: async () => {
    const result = await querySnapshot(tenantId.value, snapshotId.value);
    const item = result.data.snapshot;
    if (
      item.tenant_id !== tenantId.value ||
      item.project_id !== projectId.value ||
      item.artifact_id !== artifactId.value
    ) {
      throw new Error('Snapshot 不属于当前 Artifact');
    }
    return result;
  },
  refetchInterval: (query) => snapshotPollInterval(query.state.data?.data.snapshot.state),
});
const snapshot = computed(() => snapshotQuery.data.value?.data.snapshot);
const commitId = computed(() => snapshot.value?.commit_id ?? '');

const commitQuery = useQuery({
  queryKey: computed(() => [
    'artifact-commit-diff',
    tenantId.value,
    projectId.value,
    artifactId.value,
    commitId.value,
    'snapshot-detail',
  ]),
  queryFn: () =>
    queryArtifactCommitDiff(tenantId.value, projectId.value, artifactId.value, commitId.value),
  enabled: computed(() => Boolean(commitId.value)),
});
const commitDiff = computed(() => commitQuery.data.value?.data.diff);
const snapshotTags = computed(() =>
  commitTagNames(commitDiff.value?.target_commit.tag_names ?? snapshot.value?.tag_names ?? []),
);

const filesEnabled = computed(
  () => activeTab.value === 'files' && snapshot.value?.state === 'ready',
);
const fileQuery = useQuery({
  queryKey: computed(() => [
    'snapshot-files',
    tenantId.value,
    projectId.value,
    artifactId.value,
    snapshotId.value,
    filePathPrefix.value,
    fileCursor.value ?? '',
  ]),
  queryFn: () =>
    querySnapshotFileList({
      tenant_id: tenantId.value,
      snapshot_id: snapshotId.value,
      page_size: 50,
      ...(filePathPrefix.value ? { path_prefix: filePathPrefix.value } : {}),
      ...(fileCursor.value ? { cursor: fileCursor.value } : {}),
    }),
  enabled: filesEnabled,
});

const activityQuery = useQuery({
  queryKey: computed(() => [
    'snapshot-activities',
    tenantId.value,
    projectId.value,
    artifactId.value,
    snapshotId.value,
    activityCursor.value ?? '',
    snapshot.value?.updated_at_unix_ms ?? '',
  ]),
  queryFn: () =>
    querySnapshotActivityList({
      tenant_id: tenantId.value,
      snapshot_id: snapshotId.value,
      page_size: 25,
      ...(activityCursor.value ? { cursor: activityCursor.value } : {}),
    }),
  enabled: computed(() => activeTab.value === 'activity' && Boolean(snapshot.value)),
});

const profileQuery = useQuery({
  queryKey: computed(() => [
    'snapshot-dataset-profile',
    tenantId.value,
    projectId.value,
    artifactId.value,
    snapshotId.value,
  ]),
  queryFn: () =>
    querySnapshotDatasetProfile({
      tenant_id: tenantId.value,
      snapshot_id: snapshotId.value,
    }),
  enabled: computed(() => activeTab.value === 'profile' && Boolean(snapshot.value)),
});
const profile = computed(() => profileQuery.data.value?.data.profile);

const retryMutation = useMutation({ mutationFn: retrySnapshotDelivery });

watch(requestedTab, (tab) => {
  activeTab.value = allowedTabs.includes(tab) ? tab : 'overview';
});

watch([tenantId, projectId, artifactId, snapshotId], () => {
  activeTab.value = allowedTabs.includes(requestedTab.value) ? requestedTab.value : 'overview';
  filePathInput.value = '';
  filePathPrefix.value = '';
  fileCursor.value = undefined;
  fileCursorHistory.value = [];
  activityCursor.value = undefined;
  activityCursorHistory.value = [];
  retryRequestId.value = undefined;
  retryMutation.reset();
});

const refreshing = computed(
  () =>
    snapshotQuery.isFetching.value ||
    commitQuery.isFetching.value ||
    (activeTab.value === 'files' && fileQuery.isFetching.value) ||
    (activeTab.value === 'activity' && activityQuery.isFetching.value) ||
    (activeTab.value === 'profile' && profileQuery.isFetching.value),
);

function applyFileFilter(): void {
  filePathPrefix.value = filePathInput.value.trim();
  fileCursor.value = undefined;
  fileCursorHistory.value = [];
}

function nextFilePage(): void {
  const next = fileQuery.data.value?.data.next_cursor;
  if (!next) return;
  fileCursorHistory.value.push(fileCursor.value ?? '');
  fileCursor.value = next;
}

function previousFilePage(): void {
  fileCursor.value = fileCursorHistory.value.pop() || undefined;
}

function nextActivityPage(): void {
  const next = activityQuery.data.value?.data.next_cursor;
  if (!next) return;
  activityCursorHistory.value.push(activityCursor.value ?? '');
  activityCursor.value = next;
}

function previousActivityPage(): void {
  activityCursor.value = activityCursorHistory.value.pop() || undefined;
}

async function refresh(): Promise<void> {
  const tasks: Array<Promise<unknown>> = [snapshotQuery.refetch()];
  if (commitId.value) tasks.push(commitQuery.refetch());
  if (filesEnabled.value) tasks.push(fileQuery.refetch());
  if (activeTab.value === 'activity') tasks.push(activityQuery.refetch());
  if (activeTab.value === 'profile') tasks.push(profileQuery.refetch());
  await Promise.all(tasks);
}

async function retryDelivery(): Promise<void> {
  if (retryMutation.isPending.value) return;
  retryRequestId.value ??= `snapshot-retry-${globalThis.crypto.randomUUID()}`;
  try {
    const result = await retryMutation.mutateAsync({
      tenant_id: tenantId.value,
      snapshot_id: snapshotId.value,
      retry_request_id: retryRequestId.value,
    });
    retryRequestId.value = undefined;
    ElMessage.success(result.data.replayed ? '已返回同一重试请求的幂等结果' : '已重新开始交付');
    activityCursor.value = undefined;
    activityCursorHistory.value = [];
    await snapshotQuery.refetch();
    if (activeTab.value === 'activity') await activityQuery.refetch();
  } catch {
    // Keep retry_request_id stable so a transport retry remains idempotent.
  }
}

async function openCommit(): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
    },
    query: { tab: 'commits', commit_id: commitId.value },
  });
}

async function backToList(): Promise<void> {
  await router.push({
    name: 'snapshot-list',
    params: { tenantId: tenantId.value },
    query: { project_id: projectId.value, artifact_id: artifactId.value },
  });
}

function fileSize(file: LogicalFileEntry): string {
  return file.size_bytes === undefined ? '—' : formatBytes(file.size_bytes);
}

function fileRows(file: LogicalFileEntry): string {
  return file.row_count === undefined ? '—' : formatCount(file.row_count);
}

function entryTypeLabel(entryType: LogicalFileEntry['entry_type']): string {
  return entryType === 'directory' ? '目录' : '文件';
}

function qualityStateLabel(state: string): string {
  return (
    {
      not_evaluated: '未评估',
      passed: '通过',
      warning: '警告',
      failed: '失败',
    }[state] ?? state
  );
}

function qualityTagType(state: string): 'info' | 'success' | 'warning' | 'danger' {
  if (state === 'passed') return 'success';
  if (state === 'warning') return 'warning';
  if (state === 'failed') return 'danger';
  return 'info';
}

function integrityPercent(): number {
  if (!snapshot.value) return 0;
  const total = BigInt(snapshot.value.logical_file_count);
  if (total === 0n) return snapshot.value.integrity.state === 'verified' ? 100 : 0;
  const verified = BigInt(snapshot.value.integrity.files_verified);
  return Math.min(100, Number((verified * 10000n) / total) / 100);
}
</script>

<template>
  <div class="page snapshot-detail">
    <PageHeading :title="snapshot?.message ?? snapshotId" :description="`Snapshot · ${snapshotId}`">
      <template #actions>
        <el-tag v-if="snapshot" :type="snapshotStateTagType(snapshot.state)" effect="plain">
          只读 · {{ snapshotStateLabel(snapshot.state) }}
        </el-tag>
        <el-button :icon="Back" @click="backToList">返回列表</el-button>
        <el-button
          v-if="snapshot?.state === 'abnormal'"
          type="primary"
          :icon="RefreshRight"
          :loading="retryMutation.isPending.value"
          @click="retryDelivery"
        >
          重试交付
        </el-button>
        <el-button :icon="RefreshRight" :loading="refreshing" @click="refresh">刷新</el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="snapshotQuery.error.value"
      :error="snapshotQuery.error.value"
      :retrying="snapshotQuery.isFetching.value"
      @retry="snapshotQuery.refetch"
    />
    <ApiProblemAlert
      v-if="commitQuery.error.value"
      :error="commitQuery.error.value"
      :retrying="commitQuery.isFetching.value"
      @retry="commitQuery.refetch"
    />
    <ApiProblemAlert
      v-if="retryMutation.error.value"
      :error="retryMutation.error.value"
      :retrying="retryMutation.isPending.value"
      @retry="retryDelivery"
    />

    <template v-if="snapshot">
      <section class="snapshot-summary" aria-label="Snapshot 摘要">
        <div>
          <span>状态</span>
          <strong
            class="state-value"
            :class="{
              'is-creating': snapshot.state === 'creating',
              'is-abnormal': snapshot.state === 'abnormal',
            }"
          >
            <CircleCheck v-if="snapshot.state === 'ready'" />
            <RefreshRight v-else-if="snapshot.state === 'creating'" />
            <WarningFilled v-else />
            {{ snapshotStateLabel(snapshot.state) }}
          </strong>
        </div>
        <div>
          <span>阶段</span><strong>{{ snapshotPhaseLabel(snapshot.phase) }}</strong>
        </div>
        <div>
          <span>文件数</span><strong>{{ formatCount(snapshot.logical_file_count) }}</strong>
        </div>
        <div>
          <span>逻辑大小</span><strong>{{ formatBytes(snapshot.logical_size_bytes) }}</strong>
        </div>
        <div>
          <span>所在区域</span><strong>{{ snapshot.region }}</strong>
        </div>
        <div>
          <span>更新时间</span><strong>{{ formatTime(snapshot.updated_at_unix_ms) }}</strong>
        </div>
      </section>

      <el-alert
        v-if="snapshot.issue"
        class="snapshot-issue"
        :title="snapshot.issue.message"
        :description="snapshot.issue.code"
        type="error"
        :closable="false"
        show-icon
      />

      <section class="content-section snapshot-detail-shell">
        <el-tabs v-model="activeTab">
          <el-tab-pane label="概览" name="overview">
            <section class="fixed-commit-band">
              <span class="fixed-commit-band__icon"><Lock /></span>
              <div>
                <small>固定 Commit</small>
                <strong>{{ commitDiff?.target_commit.message ?? snapshot.message }}</strong>
                <code>{{ snapshot.commit_id }}</code>
              </div>
              <div class="fixed-commit-band__tags">
                <el-tag v-for="tagName in snapshotTags" :key="tagName" effect="plain">
                  {{ tagName }}
                </el-tag>
                <span v-if="snapshotTags.length === 0">暂无 Tag</span>
              </div>
              <el-button text type="primary" :icon="Box" @click="openCommit">
                查看 Commit 与 Diff
              </el-button>
            </section>

            <div class="overview-grid">
              <section class="snapshot-subsection placement-section">
                <header>
                  <div>
                    <h2>存储位置</h2>
                    <p>{{ snapshot.region }}</p>
                  </div>
                  <el-tag :type="snapshotStateTagType(snapshot.state)" effect="plain">
                    {{ snapshotStateLabel(snapshot.state) }}
                  </el-tag>
                </header>
                <dl class="placement-facts">
                  <div>
                    <dt>Region</dt>
                    <dd><Location />{{ snapshot.region }}</dd>
                  </div>
                  <div>
                    <dt>StorageVolume</dt>
                    <dd>
                      <code>{{ snapshot.storage_volume_id }}</code>
                    </dd>
                  </div>
                  <div>
                    <dt>交付阶段</dt>
                    <dd>{{ snapshotPhaseLabel(snapshot.phase) }}</dd>
                  </div>
                  <div>
                    <dt>访问</dt>
                    <dd>只读</dd>
                  </div>
                </dl>
              </section>

              <section class="snapshot-subsection integrity-section">
                <header>
                  <div>
                    <h2>完整性</h2>
                    <p>{{ integrityPercent() }}%</p>
                  </div>
                  <el-tag :type="snapshotIntegrityTagType(snapshot.integrity.state)" effect="plain">
                    {{ snapshotIntegrityLabel(snapshot.integrity.state) }}
                  </el-tag>
                </header>
                <el-progress
                  :percentage="integrityPercent()"
                  :status="
                    snapshot.integrity.state === 'verified'
                      ? 'success'
                      : snapshot.integrity.state === 'failed'
                        ? 'exception'
                        : undefined
                  "
                />
                <dl>
                  <div>
                    <dt>已校验文件</dt>
                    <dd>
                      {{ formatCount(snapshot.integrity.files_verified) }} /
                      {{ formatCount(snapshot.logical_file_count) }}
                    </dd>
                  </div>
                  <div>
                    <dt>已校验数据</dt>
                    <dd>
                      {{ formatBytes(snapshot.integrity.bytes_verified) }} /
                      {{ formatBytes(snapshot.logical_size_bytes) }}
                    </dd>
                  </div>
                  <div v-if="snapshot.integrity.verified_at_unix_ms">
                    <dt>校验完成</dt>
                    <dd>{{ formatTime(snapshot.integrity.verified_at_unix_ms) }}</dd>
                  </div>
                </dl>
              </section>
            </div>

            <section v-if="commitDiff" class="snapshot-subsection diff-section">
              <header>
                <div>
                  <h2>Commit Diff</h2>
                  <p>
                    {{ commitDiff.base_commit?.commit_id ?? '根 Commit' }} →
                    {{ commitDiff.target_commit.commit_id }}
                  </p>
                </div>
              </header>
              <dl>
                <div>
                  <dt>新增</dt>
                  <dd>{{ formatCount(commitDiff.summary.files_added) }}</dd>
                </div>
                <div>
                  <dt>修改</dt>
                  <dd>{{ formatCount(commitDiff.summary.files_modified) }}</dd>
                </div>
                <div>
                  <dt>删除</dt>
                  <dd>{{ formatCount(commitDiff.summary.files_deleted) }}</dd>
                </div>
                <div>
                  <dt>重命名</dt>
                  <dd>{{ formatCount(commitDiff.summary.files_renamed) }}</dd>
                </div>
                <div>
                  <dt>增加</dt>
                  <dd>{{ formatBytes(commitDiff.summary.bytes_added) }}</dd>
                </div>
                <div>
                  <dt>移除</dt>
                  <dd>{{ formatBytes(commitDiff.summary.bytes_removed) }}</dd>
                </div>
              </dl>
            </section>

            <section class="snapshot-subsection identity-section">
              <header>
                <div>
                  <h2>Snapshot 身份</h2>
                  <p>{{ snapshot.snapshot_id }}</p>
                </div>
              </header>
              <dl>
                <div>
                  <dt>Project</dt>
                  <dd>{{ snapshot.project_id }}</dd>
                </div>
                <div>
                  <dt>Artifact</dt>
                  <dd>{{ snapshot.artifact_id }}</dd>
                </div>
                <div>
                  <dt>Commit</dt>
                  <dd>
                    <code>{{ snapshot.commit_id }}</code>
                  </dd>
                </div>
                <div>
                  <dt>创建时间</dt>
                  <dd>{{ formatTime(snapshot.created_at_unix_ms) }}</dd>
                </div>
              </dl>
            </section>
          </el-tab-pane>

          <el-tab-pane label="文件" name="files" :disabled="snapshot.state !== 'ready'">
            <section class="snapshot-files">
              <header>
                <div>
                  <h2>Snapshot 文件</h2>
                  <p>{{ formatCount(snapshot.logical_file_count) }} 个逻辑文件</p>
                </div>
                <form @submit.prevent="applyFileFilter">
                  <el-input
                    v-model="filePathInput"
                    clearable
                    placeholder="按路径前缀筛选"
                    aria-label="文件路径前缀"
                  />
                  <el-button native-type="submit" type="primary" :icon="Search">查询</el-button>
                </form>
              </header>

              <ApiProblemAlert
                v-if="fileQuery.error.value"
                :error="fileQuery.error.value"
                :retrying="fileQuery.isFetching.value"
                @retry="fileQuery.refetch"
              />
              <el-skeleton v-if="fileQuery.isPending.value" :rows="7" animated />
              <el-empty
                v-else-if="!fileQuery.data.value?.data.items.length"
                description="当前路径前缀下没有文件"
              />
              <template v-else>
                <el-table :data="fileQuery.data.value?.data.items" class="desktop-files">
                  <el-table-column label="路径" min-width="300">
                    <template #default="scope">
                      <span class="file-path"
                        ><Files /><code>{{ scope.row.path }}</code></span
                      >
                    </template>
                  </el-table-column>
                  <el-table-column label="类型" width="90">
                    <template #default="scope">{{ entryTypeLabel(scope.row.entry_type) }}</template>
                  </el-table-column>
                  <el-table-column label="格式" min-width="110">
                    <template #default="scope">{{ scope.row.format ?? '—' }}</template>
                  </el-table-column>
                  <el-table-column label="大小" min-width="120">
                    <template #default="scope"
                      ><strong>{{ fileSize(scope.row) }}</strong></template
                    >
                  </el-table-column>
                  <el-table-column label="行数" min-width="120">
                    <template #default="scope">{{ fileRows(scope.row) }}</template>
                  </el-table-column>
                  <el-table-column label="更新时间" min-width="160">
                    <template #default="scope">
                      {{
                        scope.row.updated_at_unix_ms
                          ? formatTime(scope.row.updated_at_unix_ms)
                          : '—'
                      }}
                    </template>
                  </el-table-column>
                </el-table>
                <div class="mobile-files">
                  <article v-for="file in fileQuery.data.value?.data.items" :key="file.path">
                    <code>{{ file.path }}</code>
                    <span>
                      {{ entryTypeLabel(file.entry_type) }} · {{ file.format ?? '—' }} ·
                      {{ fileSize(file) }}
                    </span>
                    <small>行数 {{ fileRows(file) }}</small>
                  </article>
                </div>
                <footer>
                  <span>当前页 {{ fileQuery.data.value?.data.items.length ?? 0 }} 项</span>
                  <PageCursor
                    :has-previous="fileCursorHistory.length > 0"
                    :has-next="Boolean(fileQuery.data.value?.data.next_cursor)"
                    :loading="fileQuery.isFetching.value"
                    @previous="previousFilePage"
                    @next="nextFilePage"
                  />
                </footer>
              </template>
            </section>
          </el-tab-pane>

          <el-tab-pane label="活动" name="activity">
            <section class="snapshot-activity">
              <header>
                <h2>交付活动</h2>
                <p>{{ snapshot.snapshot_id }}</p>
              </header>
              <ApiProblemAlert
                v-if="activityQuery.error.value"
                :error="activityQuery.error.value"
                :retrying="activityQuery.isFetching.value"
                @retry="activityQuery.refetch"
              />
              <el-skeleton v-if="activityQuery.isPending.value" :rows="6" animated />
              <el-empty
                v-else-if="!activityQuery.data.value?.data.items.length"
                description="暂无交付活动"
              />
              <template v-else>
                <el-timeline>
                  <el-timeline-item
                    v-for="activity in activityQuery.data.value?.data.items"
                    :key="activity.activity_id"
                    :timestamp="formatTime(activity.created_at_unix_ms)"
                    :type="snapshotActivityTagType(activity.activity_type)"
                  >
                    <strong>{{ snapshotActivityTypeLabel(activity.activity_type) }}</strong>
                    <p>{{ activity.summary }}</p>
                    <el-tag v-if="activity.phase" size="small" effect="plain">
                      {{ snapshotPhaseLabel(activity.phase) }}
                    </el-tag>
                    <el-alert
                      v-if="activity.issue"
                      :title="activity.issue.message"
                      :description="activity.issue.code"
                      type="error"
                      :closable="false"
                    />
                  </el-timeline-item>
                </el-timeline>
                <PageCursor
                  :has-previous="activityCursorHistory.length > 0"
                  :has-next="Boolean(activityQuery.data.value?.data.next_cursor)"
                  :loading="activityQuery.isFetching.value"
                  @previous="previousActivityPage"
                  @next="nextActivityPage"
                />
              </template>
            </section>
          </el-tab-pane>

          <el-tab-pane label="Dataset Profile" name="profile">
            <section class="snapshot-profile">
              <header>
                <div>
                  <h2>Dataset Profile</h2>
                  <p>{{ snapshot.snapshot_id }}</p>
                </div>
                <el-tag v-if="profile" :type="datasetProfileTagType(profile.state)" effect="plain">
                  {{ datasetProfileStateLabel(profile.state) }}
                </el-tag>
              </header>
              <ApiProblemAlert
                v-if="profileQuery.error.value"
                :error="profileQuery.error.value"
                :retrying="profileQuery.isFetching.value"
                @retry="profileQuery.refetch"
              />
              <el-skeleton v-if="profileQuery.isPending.value" :rows="8" animated />
              <template v-else-if="profile">
                <el-alert
                  v-if="profile.issue"
                  :title="profile.issue.message"
                  :description="profile.issue.code"
                  type="error"
                  :closable="false"
                  show-icon
                />
                <el-empty
                  v-if="profile.state === 'not_declared'"
                  description="此 Snapshot 未声明 Dataset Profile"
                />
                <template v-else>
                  <dl v-if="profile.summary" class="profile-summary">
                    <div>
                      <dt>格式</dt>
                      <dd>{{ profile.summary.format_count }}</dd>
                    </div>
                    <div>
                      <dt>文件</dt>
                      <dd>{{ formatCount(profile.summary.logical_file_count) }}</dd>
                    </div>
                    <div>
                      <dt>逻辑大小</dt>
                      <dd>{{ formatBytes(profile.summary.logical_size_bytes) }}</dd>
                    </div>
                    <div>
                      <dt>行数</dt>
                      <dd>
                        {{
                          profile.summary.row_count ? formatCount(profile.summary.row_count) : '—'
                        }}
                      </dd>
                    </div>
                    <div>
                      <dt>字段</dt>
                      <dd>{{ profile.summary.field_count ?? '—' }}</dd>
                    </div>
                  </dl>

                  <div class="profile-grid">
                    <section v-if="profile.schema" class="profile-panel schema-panel">
                      <header><h3>Schema</h3></header>
                      <el-table :data="profile.schema.fields">
                        <el-table-column prop="name" label="字段" min-width="140" />
                        <el-table-column prop="data_type" label="类型" min-width="120" />
                        <el-table-column label="可空" width="80">
                          <template #default="scope">{{
                            scope.row.nullable ? '是' : '否'
                          }}</template>
                        </el-table-column>
                        <el-table-column prop="description" label="描述" min-width="160">
                          <template #default="scope">{{ scope.row.description ?? '—' }}</template>
                        </el-table-column>
                      </el-table>
                    </section>

                    <section class="profile-panel profile-metrics">
                      <header><h3>统计与质量</h3></header>
                      <dl>
                        <div v-if="profile.statistics?.row_count">
                          <dt>统计行数</dt>
                          <dd>{{ formatCount(profile.statistics.row_count) }}</dd>
                        </div>
                        <div v-if="profile.statistics?.column_count !== undefined">
                          <dt>列数</dt>
                          <dd>{{ profile.statistics.column_count }}</dd>
                        </div>
                        <div v-if="profile.statistics?.null_value_count">
                          <dt>空值</dt>
                          <dd>{{ formatCount(profile.statistics.null_value_count) }}</dd>
                        </div>
                        <div v-if="profile.statistics?.distinct_value_count">
                          <dt>Distinct</dt>
                          <dd>{{ formatCount(profile.statistics.distinct_value_count) }}</dd>
                        </div>
                        <div v-if="profile.quality">
                          <dt>数据质量</dt>
                          <dd>
                            <el-tag :type="qualityTagType(profile.quality.state)" effect="plain">
                              {{ qualityStateLabel(profile.quality.state) }}
                            </el-tag>
                          </dd>
                        </div>
                        <div v-if="profile.quality">
                          <dt>检查</dt>
                          <dd>
                            {{ profile.quality.checks_passed }} /
                            {{ profile.quality.checks_total }} 通过
                          </dd>
                        </div>
                        <div v-if="profile.freshness">
                          <dt>观测时间</dt>
                          <dd>{{ formatTime(profile.freshness.observed_at_unix_ms) }}</dd>
                        </div>
                      </dl>
                    </section>
                  </div>
                </template>
              </template>
            </section>
          </el-tab-pane>
        </el-tabs>
      </section>
    </template>

    <div v-else-if="snapshotQuery.isPending.value" class="page-loading">
      <el-skeleton :rows="10" animated />
    </div>
  </div>
</template>

<style scoped>
.snapshot-summary {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  border: 1px solid var(--line);
  background: var(--surface);
}

.snapshot-summary > div {
  min-width: 0;
  min-height: 84px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 7px;
  padding: 14px;
  border-right: 1px solid var(--line);
}

.snapshot-summary > div:last-child {
  border-right: 0;
}

.snapshot-summary span {
  color: var(--muted);
  font-size: 10px;
}

.snapshot-summary strong {
  overflow-wrap: anywhere;
}

.state-value {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--green);
}

.state-value svg {
  width: 16px;
}

.state-value.is-creating {
  color: var(--amber);
}

.state-value.is-abnormal {
  color: var(--red);
}

.snapshot-issue {
  margin-top: 14px;
}

.snapshot-detail-shell {
  padding: 10px 20px 24px;
}

.snapshot-detail-shell :deep(.el-tabs__header) {
  margin-bottom: 20px;
}

.fixed-commit-band {
  display: grid;
  grid-template-columns: 42px minmax(260px, 1fr) minmax(180px, auto) auto;
  align-items: center;
  gap: 14px;
  padding: 16px;
  border: 1px solid #b9d4c8;
  border-left: 3px solid var(--green);
  background: #f1f8f4;
}

.fixed-commit-band__icon {
  width: 38px;
  height: 38px;
  display: grid;
  place-items: center;
  color: var(--green);
  background: #dcefe6;
}

.fixed-commit-band__icon svg {
  width: 20px;
}

.fixed-commit-band small,
.fixed-commit-band strong,
.fixed-commit-band code {
  display: block;
}

.fixed-commit-band small {
  color: var(--muted);
  font-size: 10px;
  text-transform: uppercase;
}

.fixed-commit-band strong {
  margin: 3px 0;
}

.fixed-commit-band__tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.overview-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.35fr) minmax(300px, 0.8fr);
  gap: 18px;
  margin-top: 18px;
}

.snapshot-subsection {
  border: 1px solid var(--line);
}

.snapshot-subsection > header,
.snapshot-files > header,
.snapshot-profile > header {
  min-height: 68px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  padding: 14px 16px;
  border-bottom: 1px solid var(--line);
}

.snapshot-subsection h2,
.snapshot-subsection p,
.snapshot-files h2,
.snapshot-files p,
.snapshot-activity h2,
.snapshot-activity p,
.snapshot-profile h2,
.snapshot-profile p,
.profile-panel h3 {
  margin: 0;
}

.snapshot-subsection h2,
.snapshot-files h2,
.snapshot-activity h2,
.snapshot-profile h2 {
  font-size: 14px;
}

.snapshot-subsection p,
.snapshot-files p,
.snapshot-activity p,
.snapshot-profile p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 11px;
}

.placement-facts,
.integrity-section dl,
.diff-section dl,
.identity-section dl,
.profile-summary,
.profile-metrics dl {
  margin: 0;
}

.placement-facts {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.placement-facts > div,
.integrity-section dl > div,
.identity-section dl > div,
.profile-metrics dl > div {
  min-width: 0;
  padding: 14px 16px;
  border-right: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.placement-facts > div:nth-child(2n),
.identity-section dl > div:nth-child(4n),
.profile-metrics dl > div:nth-child(2n) {
  border-right: 0;
}

.placement-facts > div:nth-last-child(-n + 2),
.identity-section dl > div,
.profile-metrics dl > div:nth-last-child(-n + 2) {
  border-bottom: 0;
}

.placement-facts dt,
.integrity-section dt,
.diff-section dt,
.identity-section dt,
.profile-summary dt,
.profile-metrics dt {
  color: var(--muted);
  font-size: 10px;
}

.placement-facts dd,
.integrity-section dd,
.diff-section dd,
.identity-section dd,
.profile-summary dd,
.profile-metrics dd {
  margin: 6px 0 0;
  font-weight: 650;
  overflow-wrap: anywhere;
}

.placement-facts dd {
  display: flex;
  align-items: center;
  gap: 6px;
}

.placement-facts svg {
  width: 15px;
  color: var(--green);
}

.integrity-section :deep(.el-progress) {
  padding: 18px 16px 4px;
}

.integrity-section dl {
  padding: 8px 16px 14px;
}

.integrity-section dl > div {
  padding: 9px 0;
  border: 0;
}

.diff-section,
.identity-section {
  margin-top: 18px;
}

.diff-section dl {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
}

.diff-section dl > div {
  min-width: 0;
  padding: 14px 16px;
  border-right: 1px solid var(--line);
}

.diff-section dl > div:last-child {
  border-right: 0;
}

.identity-section dl {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.snapshot-files,
.snapshot-activity,
.snapshot-profile {
  border: 1px solid var(--line);
}

.snapshot-files > header form {
  width: min(430px, 100%);
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 8px;
}

.file-path {
  display: flex;
  align-items: center;
  gap: 7px;
}

.file-path svg {
  flex: 0 0 auto;
  width: 15px;
  color: var(--green);
}

.snapshot-files > footer {
  min-height: 60px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 14px;
  border-top: 1px solid var(--line);
  color: var(--muted);
  font-size: 11px;
}

.mobile-files {
  display: none;
}

.snapshot-activity {
  padding: 18px 20px;
}

.snapshot-activity > header {
  margin-bottom: 20px;
}

.snapshot-activity :deep(.el-timeline) {
  padding-left: 8px;
}

.snapshot-activity :deep(.el-timeline-item__content) > p {
  margin: 5px 0 8px;
  color: var(--muted);
}

.snapshot-activity :deep(.el-alert) {
  margin-top: 10px;
}

.snapshot-activity :deep(.page-cursor) {
  justify-content: flex-end;
}

.profile-summary {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  border-bottom: 1px solid var(--line);
}

.profile-summary > div {
  min-width: 0;
  padding: 14px 16px;
  border-right: 1px solid var(--line);
}

.profile-summary > div:last-child {
  border-right: 0;
}

.profile-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.4fr) minmax(260px, 0.7fr);
  gap: 18px;
  padding: 18px;
}

.profile-panel {
  border: 1px solid var(--line);
}

.profile-panel > header {
  padding: 13px 15px;
  border-bottom: 1px solid var(--line);
}

.profile-panel h3 {
  font-size: 13px;
}

.profile-metrics dl {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.snapshot-profile > .el-alert,
.snapshot-files :deep(.api-problem),
.snapshot-activity :deep(.api-problem),
.snapshot-profile :deep(.api-problem) {
  margin: 14px 16px;
}

@media (max-width: 1000px) {
  .snapshot-summary {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .snapshot-summary > div:nth-child(3) {
    border-right: 0;
  }

  .snapshot-summary > div:nth-child(-n + 3) {
    border-bottom: 1px solid var(--line);
  }

  .fixed-commit-band {
    grid-template-columns: 42px minmax(0, 1fr) auto;
  }

  .fixed-commit-band__tags {
    grid-column: 2 / 4;
  }

  .overview-grid,
  .profile-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 760px) {
  .snapshot-detail-shell {
    padding-inline: 12px;
  }

  .desktop-files {
    display: none;
  }

  .mobile-files {
    display: grid;
  }

  .mobile-files article {
    min-width: 0;
    display: grid;
    gap: 5px;
    padding: 13px 14px;
    border-bottom: 1px solid var(--line);
  }

  .mobile-files code,
  .mobile-files span,
  .mobile-files small {
    overflow-wrap: anywhere;
  }

  .mobile-files span,
  .mobile-files small {
    color: var(--muted);
    font-size: 10px;
  }

  .diff-section dl,
  .profile-summary {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .diff-section dl > div:nth-child(3),
  .profile-summary > div:nth-child(3) {
    border-right: 0;
  }

  .diff-section dl > div:nth-child(-n + 3),
  .profile-summary > div:nth-child(-n + 3) {
    border-bottom: 1px solid var(--line);
  }

  .identity-section dl {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .identity-section dl > div:nth-child(-n + 2) {
    border-bottom: 1px solid var(--line);
  }
}

@media (max-width: 560px) {
  .snapshot-summary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .snapshot-summary > div:nth-child(2n) {
    border-right: 0;
  }

  .snapshot-summary > div:nth-child(3) {
    border-right: 1px solid var(--line);
  }

  .snapshot-summary > div:nth-child(-n + 4) {
    border-bottom: 1px solid var(--line);
  }

  .fixed-commit-band {
    grid-template-columns: 38px minmax(0, 1fr);
  }

  .fixed-commit-band__tags,
  .fixed-commit-band .el-button {
    grid-column: 2;
    justify-self: start;
  }

  .placement-facts,
  .identity-section dl,
  .profile-metrics dl,
  .profile-summary {
    grid-template-columns: 1fr;
  }

  .placement-facts > div,
  .identity-section dl > div,
  .profile-metrics dl > div,
  .profile-summary > div {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .placement-facts > div:last-child,
  .identity-section dl > div:last-child,
  .profile-metrics dl > div:last-child,
  .profile-summary > div:last-child {
    border-bottom: 0;
  }

  .snapshot-files > header {
    align-items: stretch;
    flex-direction: column;
  }

  .snapshot-files > header form {
    width: 100%;
  }

  .snapshot-files > footer {
    align-items: stretch;
    flex-direction: column;
  }

  .snapshot-files > footer :deep(.page-cursor) {
    justify-content: space-between;
  }
}
</style>
