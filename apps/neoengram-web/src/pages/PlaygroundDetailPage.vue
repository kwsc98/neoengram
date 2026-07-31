<script setup lang="ts">
import {
  Back,
  Check,
  CircleCheck,
  Files,
  RefreshRight,
  Search,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  queryPlayground,
  queryPlaygroundChangeList,
  queryPlaygroundDatasetProfile,
  queryPlaygroundFileList,
  queryPlaygroundFileMetadata,
  startPlaygroundPreCommit,
} from '@/api/operations';
import type { PlaygroundChangeEntry, StartPreCommitRequest } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  playgroundAvailabilityLabel,
  playgroundAvailabilityTagType,
} from '@/features/precommit/status';
import { useTenantsStore } from '@/stores/tenants';
import { formatBytes, formatCount, formatTime } from '@/utils/format';

type ChangeFilter = 'all' | PlaygroundChangeEntry['change_type'];

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

const workspaceTab = ref('changes');
const changeType = ref<ChangeFilter>('all');
const changePathInput = ref('');
const changePathPrefix = ref('');
const changeCursor = ref<string>();
const changeCursorHistory = ref<string[]>([]);
const filePathInput = ref('');
const filePathPrefix = ref('');
const fileFormatInput = ref('');
const fileFormat = ref('');
const fileCursor = ref<string>();
const fileCursorHistory = ref<string[]>([]);
const metadataDrawerOpen = ref(false);
const selectedFilePath = ref('');
const pendingStartRequest = ref<StartPreCommitRequest>();

const playgroundQuery = useQuery({
  queryKey: playgroundKey,
  queryFn: () =>
    queryPlayground(tenantId.value, projectId.value, artifactId.value, playgroundId.value),
  refetchInterval: (query) =>
    query.state.data?.data.playground.state === 'creating' ? 1_000 : false,
});
const playground = computed(() => playgroundQuery.data.value?.data.playground);
const playgroundIndexVersionKey = computed(() => {
  const indexVersion = playground.value?.index_version;
  return indexVersion ? `${indexVersion.revision}:${indexVersion.digest}` : '';
});

const changeQuery = useQuery({
  queryKey: computed(
    () =>
      [
        'playground-changes',
        tenantId.value,
        projectId.value,
        artifactId.value,
        playgroundId.value,
        playgroundIndexVersionKey.value,
        'workspace',
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
      page_size: 50,
      ...(changeType.value !== 'all' ? { change_type: changeType.value } : {}),
      ...(changePathPrefix.value ? { path_prefix: changePathPrefix.value } : {}),
      ...(changeCursor.value ? { cursor: changeCursor.value } : {}),
    }),
  enabled: computed(() => Boolean(playground.value)),
});

const fileQuery = useQuery({
  queryKey: computed(
    () =>
      [
        'playground-files',
        tenantId.value,
        projectId.value,
        artifactId.value,
        playgroundId.value,
        playgroundIndexVersionKey.value,
        filePathPrefix.value,
        fileFormat.value,
        fileCursor.value ?? '',
      ] as const,
  ),
  queryFn: () =>
    queryPlaygroundFileList({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      playground_id: playgroundId.value,
      page_size: 50,
      ...(filePathPrefix.value ? { path_prefix: filePathPrefix.value } : {}),
      ...(fileFormat.value ? { format: fileFormat.value } : {}),
      ...(fileCursor.value ? { cursor: fileCursor.value } : {}),
    }),
  enabled: computed(() => Boolean(playground.value)),
});

const profileQuery = useQuery({
  queryKey: computed(
    () =>
      [
        'playground-dataset-profile',
        tenantId.value,
        projectId.value,
        artifactId.value,
        playgroundId.value,
        playgroundIndexVersionKey.value,
      ] as const,
  ),
  queryFn: () =>
    queryPlaygroundDatasetProfile({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      playground_id: playgroundId.value,
    }),
  enabled: computed(() => Boolean(playground.value)),
});
const profile = computed(() => profileQuery.data.value?.data.profile);

const metadataQuery = useQuery({
  queryKey: computed(
    () =>
      [
        'playground-file-metadata',
        tenantId.value,
        projectId.value,
        artifactId.value,
        playgroundId.value,
        playgroundIndexVersionKey.value,
        selectedFilePath.value,
      ] as const,
  ),
  queryFn: () =>
    queryPlaygroundFileMetadata({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      playground_id: playgroundId.value,
      path: selectedFilePath.value,
    }),
  enabled: computed(() => metadataDrawerOpen.value && Boolean(selectedFilePath.value)),
});
const metadata = computed(() => metadataQuery.data.value?.data.metadata);

const startMutation = useMutation({ mutationFn: startPlaygroundPreCommit });
const hasCommitPermission = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('commit.create') ?? false,
);
const canStartPreCommit = computed(
  () =>
    hasCommitPermission.value &&
    playground.value?.state === 'ready' &&
    !playground.value.active_precommit_id,
);

const changeSummary = computed(() => changeQuery.data.value?.data.summary);

function resetChangeCursor(): void {
  changeCursor.value = undefined;
  changeCursorHistory.value = [];
}

function resetFileCursor(): void {
  fileCursor.value = undefined;
  fileCursorHistory.value = [];
}

watch(changeType, resetChangeCursor);
watch([tenantId, projectId, artifactId, playgroundId], () => {
  resetChangeCursor();
  resetFileCursor();
  selectedFilePath.value = '';
  metadataDrawerOpen.value = false;
  pendingStartRequest.value = undefined;
});
watch(playgroundIndexVersionKey, () => {
  resetChangeCursor();
  resetFileCursor();
  selectedFilePath.value = '';
  metadataDrawerOpen.value = false;
});

function applyChangeFilters(): void {
  changePathPrefix.value = changePathInput.value.trim();
  resetChangeCursor();
}

function applyFileFilters(): void {
  filePathPrefix.value = filePathInput.value.trim();
  fileFormat.value = fileFormatInput.value.trim();
  resetFileCursor();
}

function nextChangePage(): void {
  const next = changeQuery.data.value?.data.next_cursor;
  if (!next) return;
  changeCursorHistory.value.push(changeCursor.value ?? '');
  changeCursor.value = next;
}

function previousChangePage(): void {
  changeCursor.value = changeCursorHistory.value.pop() || undefined;
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

function changeImpact(row: PlaygroundChangeEntry): string {
  if (row.old_size_bytes === undefined && row.new_size_bytes === undefined) return '—';
  const oldSize = BigInt(row.old_size_bytes ?? '0');
  const newSize = BigInt(row.new_size_bytes ?? '0');
  if (newSize === oldSize) return '0 B';
  return `${newSize > oldSize ? '+' : '-'}${formatBytes((newSize > oldSize ? newSize - oldSize : oldSize - newSize).toString())}`;
}

function showFileMetadata(path: string): void {
  selectedFilePath.value = path;
  metadataDrawerOpen.value = true;
}

async function openCommitPage(): Promise<void> {
  await router.push({
    name: 'playground-commit',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      playgroundId: playgroundId.value,
    },
    ...(playground.value?.active_precommit_id
      ? { query: { precommit_id: playground.value.active_precommit_id } }
      : {}),
  });
}

async function startPreCommit(): Promise<void> {
  if (startMutation.isPending.value) return;
  const current = playground.value;
  if (!current || !canStartPreCommit.value) return;
  pendingStartRequest.value ??= {
    tenant_id: tenantId.value,
    project_id: projectId.value,
    artifact_id: artifactId.value,
    playground_id: playgroundId.value,
    precommit_request_id: `precommit-request-${globalThis.crypto.randomUUID()}`,
    expected_index_version: current.index_version,
  };
  try {
    await startMutation.mutateAsync(pendingStartRequest.value);
  } catch {
    return;
  }
  pendingStartRequest.value = undefined;
  await Promise.all([
    playgroundQuery.refetch(),
    queryClient.invalidateQueries({ queryKey: ['playgrounds', tenantId.value] }),
  ]);
  ElMessage.success('Pre-commit 已发起');
  await openCommitPage();
}

async function openHeadCommit(): Promise<void> {
  if (!playground.value?.head_commit_id) return;
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: projectId.value, artifactId: artifactId.value },
    query: { tab: 'commits', commit_id: playground.value.head_commit_id },
  });
}
</script>

<template>
  <div class="page playground-detail">
    <PageHeading
      :title="playground?.display_name ?? playgroundId"
      :description="`${projectId} / ${artifactId} / ${playgroundId}`"
    >
      <template #actions>
        <el-button
          v-if="canStartPreCommit"
          type="primary"
          :icon="Check"
          :loading="startMutation.isPending.value"
          @click="startPreCommit"
        >
          发起 Pre-commit
        </el-button>
        <el-button
          v-else-if="playground?.active_precommit_id"
          type="primary"
          plain
          @click="openCommitPage"
        >
          查看 Pre-commit
        </el-button>
        <el-button
          :icon="Back"
          @click="router.push({ name: 'playground-list', params: { tenantId } })"
        >
          返回列表
        </el-button>
        <el-button
          :icon="RefreshRight"
          :loading="playgroundQuery.isFetching.value"
          @click="playgroundQuery.refetch"
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
      v-if="startMutation.error.value"
      :error="startMutation.error.value"
      :retrying="startMutation.isPending.value"
      @retry="startPreCommit"
    />

    <el-skeleton v-if="playgroundQuery.isPending.value" :rows="7" animated />
    <template v-else-if="playground">
      <el-alert
        v-if="playground.issue"
        :title="playground.issue.message"
        :type="playground.issue.retryable ? 'warning' : 'error'"
        :closable="false"
        show-icon
      />

      <section class="resource-summary playground-summary">
        <div>
          <span>可用性</span>
          <el-tag :type="playgroundAvailabilityTagType(playground.state)" effect="plain">
            {{ playgroundAvailabilityLabel(playground.state) }}
          </el-tag>
        </div>
        <div>
          <span>当前操作</span>
          <el-tag v-if="playground.active_precommit_id" type="warning" effect="plain">
            活动 Pre-commit
          </el-tag>
          <strong v-else>空闲</strong>
        </div>
        <div>
          <span>Region</span><strong>{{ playground.region }}</strong>
        </div>
        <div>
          <span>Index revision</span><strong>{{ playground.index_version.revision }}</strong>
        </div>
        <div>
          <span>更新时间</span><strong>{{ formatTime(playground.updated_at_unix_ms) }}</strong>
        </div>
      </section>

      <section class="precommit-band" :class="{ 'is-active': playground.active_precommit_id }">
        <CircleCheck v-if="playground.state === 'ready'" />
        <WarningFilled v-else />
        <div>
          <strong v-if="playground.active_precommit_id">存在活动 Pre-commit</strong>
          <strong v-else-if="playground.state === 'ready'">Playground 可以发起 Pre-commit</strong>
          <strong v-else>Playground 当前不可提交</strong>
          <p v-if="playground.active_precommit_id">
            <code>{{ playground.active_precommit_id }}</code
            >，进入提交页面查看权威状态。
          </p>
          <p v-else>Pre-commit 会冻结当前 IndexVersion，并返回可审查的变化和检查结果。</p>
        </div>
        <el-button v-if="playground.active_precommit_id" type="primary" @click="openCommitPage">
          查看状态
        </el-button>
      </section>

      <section class="content-section playground-console">
        <div class="section-heading section-heading--inline">
          <div>
            <h2>工作区数据</h2>
            <p>服务端 Index 中的逻辑文件、变化和派生元数据</p>
          </div>
          <div class="section-actions">
            <el-button
              v-if="playground.head_commit_id"
              text
              type="primary"
              :icon="Files"
              @click="openHeadCommit"
            >
              查看 Head Commit
            </el-button>
          </div>
        </div>

        <el-tabs v-model="workspaceTab" class="workspace-tabs">
          <el-tab-pane label="变化" name="changes">
            <form class="data-toolbar" @submit.prevent="applyChangeFilters">
              <el-select v-model="changeType" aria-label="变化类型">
                <el-option label="全部变化" value="all" />
                <el-option label="新增" value="added" />
                <el-option label="修改" value="modified" />
                <el-option label="删除" value="deleted" />
                <el-option label="重命名" value="renamed" />
              </el-select>
              <el-input
                v-model="changePathInput"
                clearable
                aria-label="变化路径前缀"
                placeholder="路径前缀"
              />
              <el-button native-type="submit" :icon="Search">查询</el-button>
            </form>

            <ApiProblemAlert
              v-if="changeQuery.error.value"
              :error="changeQuery.error.value"
              :retrying="changeQuery.isFetching.value"
              @retry="changeQuery.refetch"
            />
            <div v-if="changeSummary" class="data-metrics" aria-label="变化摘要">
              <div>
                <span>新增</span><strong>{{ formatCount(changeSummary.files_added) }}</strong>
              </div>
              <div>
                <span>修改</span><strong>{{ formatCount(changeSummary.files_modified) }}</strong>
              </div>
              <div>
                <span>删除</span><strong>{{ formatCount(changeSummary.files_deleted) }}</strong>
              </div>
              <div>
                <span>重命名</span><strong>{{ formatCount(changeSummary.files_renamed) }}</strong>
              </div>
              <div>
                <span>增加</span><strong>{{ formatBytes(changeSummary.bytes_added) }}</strong>
              </div>
              <div>
                <span>移除</span><strong>{{ formatBytes(changeSummary.bytes_removed) }}</strong>
              </div>
            </div>
            <el-skeleton v-if="changeQuery.isPending.value" :rows="6" animated />
            <el-empty
              v-else-if="!changeQuery.data.value?.data.items.length"
              description="当前筛选下没有变化"
              :image-size="72"
            />
            <template v-else>
              <el-table :data="changeQuery.data.value?.data.items" class="desktop-table">
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
                <el-table-column label="当前大小" min-width="110">
                  <template #default="scope">{{ changeSize(scope.row) }}</template>
                </el-table-column>
                <el-table-column label="变化量" min-width="110">
                  <template #default="scope">{{ changeImpact(scope.row) }}</template>
                </el-table-column>
                <el-table-column width="78" align="right">
                  <template #default="scope">
                    <el-button
                      v-if="scope.row.change_type !== 'deleted'"
                      text
                      type="primary"
                      @click="showFileMetadata(scope.row.path)"
                    >
                      元数据
                    </el-button>
                  </template>
                </el-table-column>
              </el-table>
              <PageCursor
                :has-previous="changeCursorHistory.length > 0"
                :has-next="Boolean(changeQuery.data.value?.data.next_cursor)"
                :loading="changeQuery.isFetching.value"
                @previous="previousChangePage"
                @next="nextChangePage"
              />
            </template>
          </el-tab-pane>

          <el-tab-pane label="文件" name="files">
            <form class="data-toolbar data-toolbar--files" @submit.prevent="applyFileFilters">
              <el-input
                v-model="filePathInput"
                clearable
                aria-label="文件路径前缀"
                placeholder="路径前缀"
              />
              <el-input
                v-model="fileFormatInput"
                clearable
                aria-label="文件格式"
                placeholder="格式，例如 parquet"
              />
              <el-button native-type="submit" :icon="Search">查询</el-button>
            </form>
            <ApiProblemAlert
              v-if="fileQuery.error.value"
              :error="fileQuery.error.value"
              :retrying="fileQuery.isFetching.value"
              @retry="fileQuery.refetch"
            />
            <el-skeleton v-if="fileQuery.isPending.value" :rows="6" animated />
            <el-empty
              v-else-if="!fileQuery.data.value?.data.items.length"
              description="当前筛选下没有文件"
              :image-size="72"
            />
            <template v-else>
              <el-table :data="fileQuery.data.value?.data.items" class="desktop-table">
                <el-table-column label="逻辑路径" min-width="340">
                  <template #default="scope"
                    ><code class="long-code">{{ scope.row.path }}</code></template
                  >
                </el-table-column>
                <el-table-column prop="entry_type" label="类型" min-width="90" />
                <el-table-column prop="format" label="格式" min-width="110" />
                <el-table-column label="大小" min-width="110">
                  <template #default="scope">{{ formatBytes(scope.row.size_bytes) }}</template>
                </el-table-column>
                <el-table-column label="记录" min-width="100">
                  <template #default="scope">{{ formatCount(scope.row.row_count) }}</template>
                </el-table-column>
                <el-table-column label="更新时间" min-width="160">
                  <template #default="scope">{{
                    formatTime(scope.row.updated_at_unix_ms)
                  }}</template>
                </el-table-column>
                <el-table-column width="78" align="right">
                  <template #default="scope">
                    <el-button
                      v-if="scope.row.entry_type === 'file'"
                      text
                      type="primary"
                      @click="showFileMetadata(scope.row.path)"
                    >
                      元数据
                    </el-button>
                  </template>
                </el-table-column>
              </el-table>
              <PageCursor
                :has-previous="fileCursorHistory.length > 0"
                :has-next="Boolean(fileQuery.data.value?.data.next_cursor)"
                :loading="fileQuery.isFetching.value"
                @previous="previousFilePage"
                @next="nextFilePage"
              />
            </template>
          </el-tab-pane>

          <el-tab-pane label="Dataset Profile" name="profile">
            <ApiProblemAlert
              v-if="profileQuery.error.value"
              :error="profileQuery.error.value"
              :retrying="profileQuery.isFetching.value"
              @retry="profileQuery.refetch"
            />
            <el-skeleton v-if="profileQuery.isPending.value" :rows="6" animated />
            <template v-else-if="profile">
              <div class="profile-heading">
                <div>
                  <h3>派生数据概览</h3>
                  <p>
                    对应 Index revision {{ profileQuery.data.value?.data.index_version.revision }}
                  </p>
                </div>
                <el-tag
                  :type="
                    profile.state === 'ready'
                      ? 'success'
                      : profile.state === 'rejected'
                        ? 'danger'
                        : 'info'
                  "
                  effect="plain"
                >
                  {{ profile.state }}
                </el-tag>
              </div>
              <el-alert
                v-if="profile.issue"
                :title="profile.issue.message"
                type="warning"
                :closable="false"
                show-icon
              />
              <div v-if="profile.summary" class="data-metrics profile-summary">
                <div>
                  <span>格式</span><strong>{{ profile.summary.format_count }}</strong>
                </div>
                <div>
                  <span>逻辑文件</span
                  ><strong>{{ formatCount(profile.summary.logical_file_count) }}</strong>
                </div>
                <div>
                  <span>逻辑大小</span
                  ><strong>{{ formatBytes(profile.summary.logical_size_bytes) }}</strong>
                </div>
                <div>
                  <span>记录</span><strong>{{ formatCount(profile.summary.row_count) }}</strong>
                </div>
                <div>
                  <span>字段</span><strong>{{ profile.summary.field_count ?? '—' }}</strong>
                </div>
              </div>
              <div class="profile-grid">
                <section>
                  <h3>Schema</h3>
                  <el-empty
                    v-if="!profile.schema?.fields.length"
                    description="没有 Schema"
                    :image-size="54"
                  />
                  <el-table v-else :data="profile.schema.fields" size="small">
                    <el-table-column prop="name" label="字段" min-width="130" />
                    <el-table-column prop="data_type" label="类型" min-width="120" />
                    <el-table-column label="Nullable" width="90">
                      <template #default="scope">{{ scope.row.nullable ? '是' : '否' }}</template>
                    </el-table-column>
                  </el-table>
                </section>
                <section>
                  <h3>统计与质量</h3>
                  <dl class="definition-list">
                    <div>
                      <dt>记录</dt>
                      <dd>{{ formatCount(profile.statistics?.row_count) }}</dd>
                    </div>
                    <div>
                      <dt>列</dt>
                      <dd>{{ profile.statistics?.column_count ?? '—' }}</dd>
                    </div>
                    <div>
                      <dt>空值</dt>
                      <dd>{{ formatCount(profile.statistics?.null_value_count) }}</dd>
                    </div>
                    <div>
                      <dt>质量状态</dt>
                      <dd>{{ profile.quality?.state ?? 'not_evaluated' }}</dd>
                    </div>
                    <div>
                      <dt>检查通过</dt>
                      <dd>
                        {{
                          profile.quality
                            ? `${profile.quality.checks_passed} / ${profile.quality.checks_total}`
                            : '—'
                        }}
                      </dd>
                    </div>
                    <div>
                      <dt>观测时间</dt>
                      <dd>{{ formatTime(profile.freshness?.observed_at_unix_ms) }}</dd>
                    </div>
                  </dl>
                </section>
              </div>
            </template>
          </el-tab-pane>
        </el-tabs>
      </section>
    </template>

    <el-drawer
      v-model="metadataDrawerOpen"
      title="文件元数据"
      size="min(560px, 92vw)"
      destroy-on-close
    >
      <ApiProblemAlert
        v-if="metadataQuery.error.value"
        :error="metadataQuery.error.value"
        :retrying="metadataQuery.isFetching.value"
        @retry="metadataQuery.refetch"
      />
      <el-skeleton v-if="metadataQuery.isPending.value" :rows="7" animated />
      <template v-else-if="metadata">
        <code class="drawer-path">{{ metadata.path }}</code>
        <dl class="definition-list metadata-overview">
          <div>
            <dt>格式</dt>
            <dd>{{ metadata.format }}</dd>
          </div>
          <div>
            <dt>Media type</dt>
            <dd>{{ metadata.media_type ?? '—' }}</dd>
          </div>
          <div>
            <dt>大小</dt>
            <dd>{{ formatBytes(metadata.size_bytes) }}</dd>
          </div>
          <div>
            <dt>记录</dt>
            <dd>{{ formatCount(metadata.row_count) }}</dd>
          </div>
          <div>
            <dt>质量状态</dt>
            <dd>{{ metadata.quality?.state ?? 'not_evaluated' }}</dd>
          </div>
          <div>
            <dt>观测时间</dt>
            <dd>{{ formatTime(metadata.freshness?.observed_at_unix_ms) }}</dd>
          </div>
        </dl>
        <h3>Schema</h3>
        <el-empty
          v-if="!metadata.schema?.fields.length"
          description="没有 Schema"
          :image-size="52"
        />
        <el-table v-else :data="metadata.schema.fields" size="small">
          <el-table-column prop="name" label="字段" min-width="130" />
          <el-table-column prop="data_type" label="类型" min-width="120" />
          <el-table-column label="Nullable" width="90">
            <template #default="scope">{{ scope.row.nullable ? '是' : '否' }}</template>
          </el-table-column>
        </el-table>
      </template>
    </el-drawer>
  </div>
</template>

<style scoped>
.playground-detail {
  width: min(1240px, 100%);
}

.playground-summary {
  grid-template-columns: repeat(5, minmax(0, 1fr));
}

.precommit-band {
  display: grid;
  grid-template-columns: 34px minmax(0, 1fr) auto;
  align-items: center;
  gap: 14px;
  margin: 14px 0 18px;
  padding: 15px 18px;
  border: 1px solid var(--line);
  border-left: 3px solid var(--green);
  background: #f4f8f6;
}

.precommit-band.is-active {
  border-left-color: #ad761f;
  background: #fff9ec;
}

.precommit-band > svg {
  width: 22px;
  color: var(--green);
}

.precommit-band strong,
.precommit-band p {
  display: block;
  margin: 0;
}

.precommit-band p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 11px;
  overflow-wrap: anywhere;
}

.playground-console {
  min-width: 0;
}

.data-toolbar {
  display: grid;
  grid-template-columns: 180px minmax(220px, 1fr) auto;
  gap: 10px;
  margin-bottom: 14px;
}

.data-toolbar--files {
  grid-template-columns: minmax(220px, 1fr) minmax(180px, 0.5fr) auto;
}

.data-metrics {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  margin-bottom: 14px;
  border: 1px solid var(--line);
  background: #f8faf9;
}

.data-metrics > div {
  min-width: 0;
  padding: 12px;
  border-right: 1px solid var(--line);
}

.data-metrics > div:last-child {
  border-right: 0;
}

.data-metrics span,
.data-metrics strong {
  display: block;
}

.data-metrics span {
  color: var(--muted);
  font-size: 10px;
}

.data-metrics strong {
  margin-top: 5px;
  font-size: 14px;
  overflow-wrap: anywhere;
}

.path-cell {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 3px;
}

.path-cell code,
.long-code,
.drawer-path {
  overflow-wrap: anywhere;
}

.path-cell small {
  color: var(--muted);
}

.profile-heading {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 14px;
  margin-bottom: 14px;
}

.profile-heading h3,
.profile-heading p {
  margin: 0;
}

.profile-heading p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 11px;
}

.profile-summary {
  grid-template-columns: repeat(5, minmax(0, 1fr));
  margin-top: 14px;
}

.profile-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.4fr) minmax(280px, 0.8fr);
  gap: 18px;
}

.profile-grid > section {
  min-width: 0;
  padding-top: 8px;
}

.profile-grid h3 {
  margin: 0 0 10px;
  font-size: 13px;
}

.definition-list {
  margin: 0;
  border-top: 1px solid var(--line);
}

.definition-list > div {
  display: grid;
  grid-template-columns: minmax(100px, 0.7fr) minmax(0, 1fr);
  gap: 12px;
  padding: 10px 0;
  border-bottom: 1px solid var(--line);
}

.definition-list dt {
  color: var(--muted);
  font-size: 10px;
}

.definition-list dd {
  min-width: 0;
  margin: 0;
  font-size: 11px;
  overflow-wrap: anywhere;
}

.drawer-path {
  display: block;
  margin-bottom: 16px;
  padding: 12px;
  background: #f3f6f4;
}

.metadata-overview {
  margin-bottom: 22px;
}

@media (max-width: 900px) {
  .playground-summary,
  .data-metrics,
  .profile-summary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .data-metrics > div:nth-child(2n) {
    border-right: 0;
  }

  .profile-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 640px) {
  .precommit-band,
  .data-toolbar,
  .data-toolbar--files {
    grid-template-columns: 1fr;
  }

  .precommit-band > svg {
    display: none;
  }

  .playground-summary,
  .data-metrics,
  .profile-summary {
    grid-template-columns: 1fr;
  }

  .data-metrics > div {
    border-right: 0;
  }
}
</style>
