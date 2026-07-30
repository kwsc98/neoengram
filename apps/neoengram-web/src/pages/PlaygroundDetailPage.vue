<script setup lang="ts">
import {
  Back,
  Box,
  Check,
  CircleCheck,
  CircleClose,
  Clock,
  DataAnalysis,
  Files,
  RefreshRight,
  Search,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { queryPlayground } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  cancelPrototypePreCommit,
  getActivePreCommit,
  playgroundAvailabilityLabel,
  playgroundAvailabilityTagType,
  preCommitScopeKey,
  preCommitPhaseLabels,
  startPrototypePreCommit,
} from '@/features/precommit/prototype';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const playgroundId = computed(() => String(route.params.playgroundId ?? ''));
const playgroundQuery = useQuery({
  queryKey: computed(() => [
    'playground',
    tenantId.value,
    projectId.value,
    artifactId.value,
    playgroundId.value,
  ]),
  queryFn: () =>
    queryPlayground(tenantId.value, projectId.value, artifactId.value, playgroundId.value),
  refetchInterval: (query) =>
    query.state.data?.data.playground.state === 'creating' ? 1_000 : false,
});
const playground = computed(() => playgroundQuery.data.value?.data.playground);
const playgroundPreCommitKey = computed(() =>
  preCommitScopeKey(tenantId.value, projectId.value, artifactId.value, playgroundId.value),
);
const workspaceTab = ref('changes');
const diffFilter = ref('all');
const diffSearch = ref('');
const showAllDiffRows = ref(false);
const metadataDrawerOpen = ref(false);
const selectedDiffPath = ref('');
const activePreCommit = computed(() => getActivePreCommit(playgroundPreCommitKey.value));
const canStartPreCommit = computed(
  () =>
    (tenants.byId(tenantId.value)?.permissions.includes('commit.create') ?? false) &&
    playground.value?.state === 'ready' &&
    !activePreCommit.value,
);

type ChangeType = 'added' | 'modified' | 'deleted' | 'renamed';

interface WorkspaceDiffRow {
  type: ChangeType;
  path: string;
  previousPath?: string;
  size: string;
  impact: string;
  objectCount: number;
  digest: string;
  previousDigest?: string;
  format: string;
}

const workspaceDiffRows: WorkspaceDiffRow[] = [
  {
    type: 'modified',
    path: 'dataset/index.json',
    size: '3.1 MiB',
    impact: '+312 KiB',
    objectCount: 3,
    digest: 'b3:61f7a82c4fe909d8',
    previousDigest: 'b3:1b42a8fd87d116c0',
    format: 'JSON',
  },
  {
    type: 'added',
    path: 'dataset/night-rain/part-0042.parquet',
    size: '18.6 GiB',
    impact: '+18.6 GiB',
    objectCount: 147,
    digest: 'b3:84c86a69ca3e714f',
    format: 'Parquet',
  },
  {
    type: 'renamed',
    path: 'labels/reviewed/night-v4.jsonl',
    previousPath: 'labels/reviewed/night-final.jsonl',
    size: '1.4 GiB',
    impact: '0 B',
    objectCount: 12,
    digest: 'b3:79c7a3c7cf3e3eb1',
    previousDigest: 'b3:79c7a3c7cf3e3eb1',
    format: 'JSONL',
  },
  {
    type: 'deleted',
    path: 'labels/drafts/night-v3.tmp',
    size: '620 MiB',
    impact: '-620 MiB',
    objectCount: 6,
    digest: '—',
    previousDigest: 'b3:a7ee0f2c53a26798',
    format: 'Binary',
  },
  {
    type: 'added',
    path: 'images/night-rain/shard-023.tar',
    size: '8.3 GiB',
    impact: '+8.3 GiB',
    objectCount: 66,
    digest: 'b3:85c84f4b756e43a2',
    format: 'TAR',
  },
  {
    type: 'modified',
    path: 'annotations/partition=night/date=2026-07-28/labels.parquet',
    size: '2.7 GiB',
    impact: '+420 MiB',
    objectCount: 23,
    digest: 'b3:29a36f8751c76409',
    previousDigest: 'b3:a76b12bc8dd58fcb',
    format: 'Parquet',
  },
  {
    type: 'modified',
    path: 'schemas/road-scene.avsc',
    size: '14 KiB',
    impact: '+862 B',
    objectCount: 1,
    digest: 'b3:ce546f01d8d09c64',
    previousDigest: 'b3:51c94b670a3fef39',
    format: 'Avro Schema',
  },
  {
    type: 'added',
    path: 'profiles/night-v4.dataset-profile.json',
    size: '480 KiB',
    impact: '+480 KiB',
    objectCount: 1,
    digest: 'b3:d87c5f31ac08a9fe',
    format: 'Dataset Profile',
  },
  {
    type: 'added',
    path: 'dataset/night-fog/part-0018.parquet',
    size: '6.1 GiB',
    impact: '+6.1 GiB',
    objectCount: 49,
    digest: 'b3:ed148eb557d22270',
    format: 'Parquet',
  },
  {
    type: 'modified',
    path: 'labels/quality/night-scores.parquet',
    size: '920 MiB',
    impact: '+74 MiB',
    objectCount: 8,
    digest: 'b3:8392cc09c1a040ae',
    previousDigest: 'b3:7f526b649db3f0e5',
    format: 'Parquet',
  },
  {
    type: 'deleted',
    path: 'cache/preview.sqlite',
    size: '128 MiB',
    impact: '-128 MiB',
    objectCount: 2,
    digest: '—',
    previousDigest: 'b3:fb5fd40293914467',
    format: 'SQLite',
  },
  {
    type: 'renamed',
    path: 'docs/data-license-v4.txt',
    previousPath: 'docs/data-license-latest.txt',
    size: '12 KiB',
    impact: '0 B',
    objectCount: 1,
    digest: 'b3:56ff72e0b4d639ac',
    previousDigest: 'b3:56ff72e0b4d639ac',
    format: 'Text',
  },
  {
    type: 'modified',
    path: 'dataset/partitions.json',
    size: '76 KiB',
    impact: '+4 KiB',
    objectCount: 1,
    digest: 'b3:12876041e465dfe6',
    previousDigest: 'b3:63fa66301e6531b2',
    format: 'JSON',
  },
  {
    type: 'added',
    path: 'lineage/source-map.jsonl',
    size: '84 MiB',
    impact: '+84 MiB',
    objectCount: 2,
    digest: 'b3:ea84db4594f7c0a4',
    format: 'JSONL',
  },
];

const indexedFiles = [
  { path: 'dataset/night-rain', files: '6,284', size: '421.8 GiB', share: 50 },
  { path: 'images/night-rain', files: '8,962', size: '286.4 GiB', share: 34 },
  { path: 'annotations', files: '2,114', size: '96.7 GiB', share: 11 },
  { path: 'labels', files: '1,046', size: '32.8 GiB', share: 4 },
  { path: 'schemas / profiles / docs', files: '148', size: '8.5 GiB', share: 1 },
];

const metadataFiles = [
  {
    path: 'dataset/night-rain/part-0042.parquet',
    format: 'Parquet',
    size: '18.6 GiB',
    rows: '12,842,731',
    objects: 147,
    manifest: 'mf-84c86a69',
  },
  {
    path: 'dataset/night-fog/part-0018.parquet',
    format: 'Parquet',
    size: '6.1 GiB',
    rows: '4,109,882',
    objects: 49,
    manifest: 'mf-ed148eb5',
  },
  {
    path: 'annotations/partition=night/date=2026-07-28/labels.parquet',
    format: 'Parquet',
    size: '2.7 GiB',
    rows: '18,409,216',
    objects: 23,
    manifest: 'mf-29a36f87',
  },
  {
    path: 'labels/reviewed/night-v4.jsonl',
    format: 'JSONL',
    size: '1.4 GiB',
    rows: '3,942,118',
    objects: 12,
    manifest: 'mf-79c7a3c7',
  },
  {
    path: 'labels/quality/night-scores.parquet',
    format: 'Parquet',
    size: '920 MiB',
    rows: '18,409,216',
    objects: 8,
    manifest: 'mf-8392cc09',
  },
  {
    path: 'lineage/source-map.jsonl',
    format: 'JSONL',
    size: '84 MiB',
    rows: '18,554',
    objects: 2,
    manifest: 'mf-ea84db45',
  },
  {
    path: 'dataset/index.json',
    format: 'JSON',
    size: '3.1 MiB',
    rows: '18,554 files',
    objects: 3,
    manifest: 'mf-61f7a82c',
  },
  {
    path: 'profiles/night-v4.dataset-profile.json',
    format: 'Profile',
    size: '480 KiB',
    rows: '14 fields',
    objects: 1,
    manifest: 'mf-d87c5f31',
  },
  {
    path: 'schemas/road-scene.avsc',
    format: 'Avro',
    size: '14 KiB',
    rows: '36 fields',
    objects: 1,
    manifest: 'mf-ce546f01',
  },
];

const workspaceActivities = [
  {
    time: '今天 15:42',
    title: 'Index 扫描完成',
    detail: 'revision 31 · 18,554 文件 · agent-sh-07',
    type: 'success',
  },
  {
    time: '今天 15:39',
    title: '开始扫描全部路径',
    detail: 'job-scan-road-scenes-031 · write lease 127',
    type: 'running',
  },
  {
    time: '今天 14:18',
    title: '外部写入观测',
    detail: '检测到 NFS mtime 变化，中心 Index 可能已过期',
    type: 'warning',
  },
  {
    time: '今天 11:42',
    title: 'Commit 已发布',
    detail: 'commit-main-3 · Tags: dataset/v4, release-candidate',
    type: 'success',
  },
  {
    time: '今天 11:41',
    title: '对象耐久性校验完成',
    detail: '1,842 objects · Central S3 verified',
    type: 'success',
  },
  {
    time: '昨天 18:22',
    title: 'Playground 写租约续期',
    detail: 'owner generation 12 · fencing token 126',
    type: 'info',
  },
  {
    time: '昨天 16:08',
    title: 'Dataset Profile 更新',
    detail: 'training-dataset-v2 · Ready',
    type: 'info',
  },
  {
    time: '07-27 09:31',
    title: 'Playground 创建',
    detail: 'base commit-main-2 · cn-shanghai',
    type: 'info',
  },
];

const schemaFields = [
  { name: 'scene_id', before: 'string', after: 'string', change: 'unchanged' },
  { name: 'captured_at', before: 'timestamp_ms', after: 'timestamp_us', change: 'modified' },
  { name: 'weather', before: 'string?', after: 'enum<string>', change: 'modified' },
  { name: 'illumination_lux', before: '—', after: 'float32?', change: 'added' },
  { name: 'rain_intensity', before: '—', after: 'float32?', change: 'added' },
  { name: 'review_status', before: 'string', after: 'string', change: 'unchanged' },
];

const filteredDiffRows = computed(() => {
  const query = diffSearch.value.trim().toLowerCase();
  const rows = workspaceDiffRows.filter(
    (row) =>
      (diffFilter.value === 'all' || row.type === diffFilter.value) &&
      (!query || row.path.toLowerCase().includes(query)),
  );
  return showAllDiffRows.value ? rows : rows.slice(0, 8);
});
const filteredMetadataFiles = computed(() => {
  const query = diffSearch.value.trim().toLowerCase();
  return metadataFiles.filter((file) => !query || file.path.toLowerCase().includes(query));
});
const selectedDiff = computed(() =>
  workspaceDiffRows.find((row) => row.path === selectedDiffPath.value),
);

function changeTypeLabel(type: ChangeType): string {
  return { added: '新增', modified: '修改', deleted: '删除', renamed: '重命名' }[type];
}

function changeTagType(type: ChangeType): 'success' | 'warning' | 'danger' | 'info' {
  if (type === 'added') return 'success';
  if (type === 'modified') return 'warning';
  if (type === 'deleted') return 'danger';
  return 'info';
}

function showFileMetadata(path: string): void {
  selectedDiffPath.value = path;
  metadataDrawerOpen.value = true;
}

async function openCommitWorkbench(): Promise<void> {
  await router.push({
    name: 'playground-commit-prototype',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      playgroundId: playgroundId.value,
    },
    query: activePreCommit.value
      ? {
          resume_phase: activePreCommit.value.phase,
          precommit_job_id: activePreCommit.value.jobId,
        }
      : {},
  });
}

async function restartPreCommit(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      '当前 Pre-commit Candidate 和执行进度会被丢弃，并创建一个新的任务。',
      '重新发起 Pre-commit',
      {
        confirmButtonText: '重新发起',
        cancelButtonText: '保留当前任务',
        type: 'warning',
      },
    );
  } catch {
    return;
  }
  startPrototypePreCommit(playgroundPreCommitKey.value);
  ElMessage.success('新的 Pre-commit 已发起');
  await openCommitWorkbench();
}

async function cancelPreCommit(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      '取消后会停止当前任务并丢弃尚未提交的 Candidate，不会影响 Playground 文件。',
      '取消 Pre-commit',
      {
        confirmButtonText: '确认取消',
        cancelButtonText: '继续执行',
        type: 'warning',
      },
    );
  } catch {
    return;
  }
  cancelPrototypePreCommit(playgroundPreCommitKey.value);
  ElMessage.success('Pre-commit 已取消，Playground 保持可用');
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
  <div class="page">
    <PageHeading
      :title="playground?.display_name ?? playgroundId"
      :description="`${projectId} / ${artifactId} / ${playgroundId}`"
    >
      <template #actions>
        <el-button
          v-if="canStartPreCommit"
          type="primary"
          :icon="Check"
          @click="openCommitWorkbench"
        >
          发起 Pre-commit
        </el-button>
        <el-button v-else-if="activePreCommit" type="primary" plain @click="openCommitWorkbench">
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
          >刷新</el-button
        >
      </template>
    </PageHeading>
    <ApiProblemAlert
      v-if="playgroundQuery.error.value"
      :error="playgroundQuery.error.value"
      :retrying="playgroundQuery.isFetching.value"
      @retry="playgroundQuery.refetch"
    />
    <template v-if="playground">
      <section class="resource-summary playground-summary">
        <div>
          <span>可用性</span
          ><el-tag :type="playgroundAvailabilityTagType(playground.state)" effect="plain">{{
            playgroundAvailabilityLabel(playground.state)
          }}</el-tag>
        </div>
        <div>
          <span>当前操作</span>
          <el-tag v-if="activePreCommit" type="warning" effect="plain">
            Pre-commit · {{ preCommitPhaseLabels[activePreCommit.phase] }}
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
      <section
        class="precommit-status-band"
        :class="{
          'is-running': activePreCommit,
          'is-unavailable': playground.state === 'abnormal',
          'is-creating': playground.state === 'creating',
        }"
        aria-label="Pre-commit 状态"
      >
        <span class="precommit-status-band__icon">
          <Clock v-if="activePreCommit" />
          <WarningFilled v-else-if="playground.state === 'abnormal'" />
          <Clock v-else-if="playground.state === 'creating'" />
          <CircleCheck v-else />
        </span>
        <div class="precommit-status-band__body">
          <template v-if="activePreCommit">
            <strong>Pre-commit 正在{{ preCommitPhaseLabels[activePreCommit.phase] }}</strong>
            <p>
              {{ activePreCommit.jobId }} · {{ activePreCommit.filesCompleted }} /
              {{ activePreCommit.filesTotal }} 文件 · {{ activePreCommit.startedAt }}发起
            </p>
            <el-progress :percentage="activePreCommit.progress" :stroke-width="5" />
          </template>
          <template v-else-if="playground.state === 'abnormal'">
            <strong>当前无法发起 Pre-commit</strong>
            <p>中心元数据仍可查看；恢复 Storage、Agent 和挂载条件后才能准备新 Commit。</p>
          </template>
          <template v-else-if="playground.state === 'creating'">
            <strong>工作区正在创建</strong>
            <p>中心正在目标 StorageVolume 上初始化目录并恢复基线 Commit，完成后即可编辑。</p>
          </template>
          <template v-else>
            <strong>工作区可用，当前没有运行中的 Pre-commit</strong>
            <p>
              发起后将扫描工作区、固化 Index 候选并执行一致性检查；正式 Commit 前只短暂申请写租约。
            </p>
          </template>
        </div>
        <dl>
          <div>
            <dt>Storage</dt>
            <dd>Ready</dd>
          </div>
          <div>
            <dt>Agent</dt>
            <dd>Reachable</dd>
          </div>
          <div>
            <dt>Mount</dt>
            <dd>Mounted</dd>
          </div>
          <div>
            <dt>Index</dt>
            <dd>Fresh</dd>
          </div>
        </dl>
        <div v-if="activePreCommit" class="precommit-status-band__actions">
          <el-button :icon="RefreshRight" @click="restartPreCommit">重新发起</el-button>
          <el-button :icon="CircleClose" type="danger" plain @click="cancelPreCommit">
            取消
          </el-button>
        </div>
      </section>
      <section class="content-section playground-console">
        <div class="section-heading section-heading--inline">
          <div>
            <h2>工作区控制台</h2>
            <p>中心 Index、文件元数据与下一次 Commit 的变化</p>
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
            <el-button
              text
              type="primary"
              :icon="Box"
              @click="
                router.push({
                  name: 'artifact-detail',
                  params: { tenantId, projectId, artifactId },
                })
              "
              >查看 Artifact</el-button
            >
          </div>
        </div>

        <el-tabs v-model="workspaceTab" class="workspace-tabs">
          <el-tab-pane label="变化" name="changes">
            <div class="freshness-bar" :class="{ 'is-running': activePreCommit }">
              <Clock v-if="activePreCommit" />
              <CircleCheck v-else />
              <div>
                <strong v-if="activePreCommit">正在准备新的 Index 候选</strong>
                <strong v-else>中心 Index 与最近一次 Agent 扫描一致</strong>
                <span v-if="activePreCommit">
                  当前仍展示已发布 revision {{ playground.index_version.revision }} 的 Diff
                </span>
                <span v-else>
                  2 分钟前完成 · revision {{ playground.index_version.revision }} · 18,554 个文件
                </span>
              </div>
              <el-tag :type="activePreCommit ? 'warning' : 'success'" effect="plain">
                {{ activePreCommit ? 'Pre-commit 处理中' : '可发起 Pre-commit' }}
              </el-tag>
            </div>

            <div class="workspace-metrics" aria-label="变化摘要">
              <div><span>变化文件</span><strong>128</strong><small>占全部文件 0.69%</small></div>
              <div><span>新增</span><strong>83</strong><small>+34.1 GiB</small></div>
              <div><span>修改</span><strong>31</strong><small>+806 MiB</small></div>
              <div><span>删除</span><strong>9</strong><small>-748 MiB</small></div>
              <div><span>重命名</span><strong>5</strong><small>对象完全复用</small></div>
              <div><span>对象复用率</span><strong>82%</strong><small>节省 126.4 GiB</small></div>
            </div>

            <div class="workspace-grid">
              <main class="workspace-diff-panel">
                <header class="workspace-panel-heading">
                  <div>
                    <h3>Index vs Head Commit</h3>
                    <p>
                      <code>{{ playground.head_commit_id }}</code> → revision
                      {{ playground.index_version.revision }}
                    </p>
                  </div>
                  <span>中心元数据计算</span>
                </header>

                <div class="diff-toolbar">
                  <el-select v-model="diffFilter" aria-label="变化类型" placeholder="全部变化">
                    <el-option label="全部变化 · 128" value="all" />
                    <el-option label="新增 · 83" value="added" />
                    <el-option label="修改 · 31" value="modified" />
                    <el-option label="删除 · 9" value="deleted" />
                    <el-option label="重命名 · 5" value="renamed" />
                  </el-select>
                  <el-input
                    v-model="diffSearch"
                    aria-label="搜索变化路径"
                    clearable
                    :prefix-icon="Search"
                    placeholder="按路径筛选"
                  />
                </div>

                <div
                  class="workspace-diff-table desktop-workspace-diff"
                  role="table"
                  aria-label="工作区文件变化"
                >
                  <div class="workspace-diff-table__head" role="row">
                    <span>变化</span><span>文件路径</span><span>格式</span><span>当前大小</span
                    ><span>影响</span><span />
                  </div>
                  <div
                    v-for="row in filteredDiffRows"
                    :key="row.path"
                    class="workspace-diff-table__row"
                    role="row"
                  >
                    <span
                      ><el-tag :type="changeTagType(row.type)" size="small" effect="plain">{{
                        changeTypeLabel(row.type)
                      }}</el-tag></span
                    >
                    <span class="file-path">
                      <code>{{ row.path }}</code>
                      <small v-if="row.previousPath">原路径 {{ row.previousPath }}</small>
                    </span>
                    <span>{{ row.format }}</span>
                    <span>{{ row.size }}</span>
                    <strong>{{ row.impact }}</strong>
                    <el-button text type="primary" @click="showFileMetadata(row.path)"
                      >元数据</el-button
                    >
                  </div>
                </div>

                <div class="mobile-workspace-diff">
                  <button
                    v-for="row in filteredDiffRows"
                    :key="row.path"
                    type="button"
                    @click="showFileMetadata(row.path)"
                  >
                    <span
                      ><el-tag :type="changeTagType(row.type)" size="small" effect="plain">{{
                        changeTypeLabel(row.type)
                      }}</el-tag
                      ><strong>{{ row.impact }}</strong></span
                    >
                    <code>{{ row.path }}</code>
                    <small>{{ row.format }} · {{ row.size }} · {{ row.objectCount }} objects</small>
                  </button>
                </div>

                <div class="diff-footer">
                  <span
                    >当前显示 {{ filteredDiffRows.length }} 项，原型数据共
                    {{ workspaceDiffRows.length }} 项</span
                  >
                  <el-button
                    v-if="!showAllDiffRows && filteredDiffRows.length < workspaceDiffRows.length"
                    text
                    type="primary"
                    @click="showAllDiffRows = true"
                  >
                    展开更多模拟数据
                  </el-button>
                </div>
              </main>

              <aside class="workspace-side-panel">
                <header class="workspace-panel-heading">
                  <div>
                    <h3>目录容量变化</h3>
                    <p>当前 Index 逻辑分布</p>
                  </div>
                </header>
                <div class="directory-bars">
                  <div v-for="folder in indexedFiles" :key="folder.path">
                    <span
                      ><code>{{ folder.path }}</code
                      ><strong>{{ folder.size }}</strong></span
                    >
                    <i><b :style="{ width: `${folder.share}%` }" /></i>
                    <small>{{ folder.files }} files · {{ folder.share }}%</small>
                  </div>
                </div>
                <div class="visibility-note">
                  <WarningFilled />
                  <div>
                    <strong>中心只展示已扫描变化</strong>
                    <p>工作区磁盘上的新变化，需要 Agent 再次扫描后才会进入 Index 和本页 Diff。</p>
                  </div>
                </div>
              </aside>
            </div>
          </el-tab-pane>

          <el-tab-pane label="文件" name="files">
            <div class="workspace-panel-heading files-heading">
              <div>
                <h3>中心 Index 文件清单</h3>
                <p>展示可提交的规范化路径与 Manifest 摘要</p>
              </div>
              <el-input
                v-model="diffSearch"
                aria-label="搜索 Index 文件"
                clearable
                :prefix-icon="Search"
                placeholder="搜索 Index 文件"
              />
            </div>
            <div class="metadata-file-table desktop-metadata-files">
              <div class="metadata-file-table__head">
                <span>逻辑路径</span><span>格式</span><span>大小</span><span>记录</span
                ><span>对象</span><span>Manifest</span>
              </div>
              <button
                v-for="file in filteredMetadataFiles"
                :key="file.path"
                type="button"
                @click="showFileMetadata(file.path)"
              >
                <code>{{ file.path }}</code
                ><span>{{ file.format }}</span
                ><span>{{ file.size }}</span
                ><span>{{ file.rows }}</span
                ><span>{{ file.objects }}</span
                ><code>{{ file.manifest }}</code>
              </button>
            </div>
            <div class="mobile-metadata-files">
              <button
                v-for="file in filteredMetadataFiles"
                :key="file.path"
                type="button"
                @click="showFileMetadata(file.path)"
              >
                <code>{{ file.path }}</code
                ><span>{{ file.format }} · {{ file.size }} · {{ file.rows }}</span
                ><small>{{ file.objects }} objects · {{ file.manifest }}</small>
              </button>
            </div>
          </el-tab-pane>

          <el-tab-pane label="元数据" name="metadata">
            <div class="metadata-layout">
              <section>
                <header class="workspace-panel-heading">
                  <div>
                    <h3>版本与 Index</h3>
                    <p>中心权威的可提交状态</p>
                  </div>
                  <DataAnalysis />
                </header>
                <dl class="metadata-definition">
                  <div>
                    <dt>IndexVersion</dt>
                    <dd>revision {{ playground.index_version.revision }}</dd>
                  </div>
                  <div>
                    <dt>Index digest</dt>
                    <dd>
                      <code>{{ playground.index_version.digest }}</code>
                    </dd>
                  </div>
                  <div>
                    <dt>Base Commit</dt>
                    <dd>
                      <code>{{ playground.base_commit_id ?? '—' }}</code>
                    </dd>
                  </div>
                  <div>
                    <dt>Head Commit</dt>
                    <dd>
                      <code>{{ playground.head_commit_id ?? '—' }}</code>
                    </dd>
                  </div>
                  <div>
                    <dt>文件 / Directory</dt>
                    <dd>18,554 / 482</dd>
                  </div>
                  <div>
                    <dt>Manifest / Object</dt>
                    <dd>18,407 / 148,296</dd>
                  </div>
                  <div>
                    <dt>逻辑大小</dt>
                    <dd>846.2 GiB</dd>
                  </div>
                </dl>
              </section>
              <section>
                <header class="workspace-panel-heading">
                  <div>
                    <h3>Placement 与执行</h3>
                    <p>存储归属和最新 Agent 观测</p>
                  </div>
                  <Box />
                </header>
                <dl class="metadata-definition">
                  <div>
                    <dt>Region</dt>
                    <dd>{{ playground.region }}</dd>
                  </div>
                  <div>
                    <dt>EdgeCluster</dt>
                    <dd><code>cluster-cn-east-1</code></dd>
                  </div>
                  <div>
                    <dt>StorageVolume</dt>
                    <dd>
                      <code>{{ playground.storage_volume_id }}</code>
                    </dd>
                  </div>
                  <div>
                    <dt>Volume Owner</dt>
                    <dd><code>agent-sh-07</code></dd>
                  </div>
                  <div>
                    <dt>Owner generation</dt>
                    <dd>12</dd>
                  </div>
                  <div>
                    <dt>写租约</dt>
                    <dd>
                      <el-tag type="success" size="small" effect="plain">active · 27 min</el-tag>
                    </dd>
                  </div>
                  <div>
                    <dt>最后观测</dt>
                    <dd>43 秒前</dd>
                  </div>
                  <div>
                    <dt>Mount generation</dt>
                    <dd>41</dd>
                  </div>
                </dl>
              </section>
              <section class="metadata-wide">
                <header class="workspace-panel-heading">
                  <div>
                    <h3>Dataset Profile</h3>
                    <p>用于结构化数据验证和可复现读取</p>
                  </div>
                  <el-tag type="success" effect="plain">Ready</el-tag>
                </header>
                <div class="profile-metrics">
                  <div><span>Profile</span><strong>training-dataset-v2</strong></div>
                  <div><span>Schema fields</span><strong>36</strong></div>
                  <div><span>Partitions</span><strong>weather / date / city</strong></div>
                  <div><span>Estimated rows</span><strong>84.7 million</strong></div>
                  <div><span>Null alerts</span><strong>2 warnings</strong></div>
                </div>
              </section>
            </div>
          </el-tab-pane>

          <el-tab-pane label="活动" name="activity">
            <div class="activity-layout">
              <div class="activity-timeline">
                <article
                  v-for="event in workspaceActivities"
                  :key="`${event.time}:${event.title}`"
                  :class="`is-${event.type}`"
                >
                  <span class="activity-dot" />
                  <time>{{ event.time }}</time>
                  <div>
                    <strong>{{ event.title }}</strong>
                    <p>{{ event.detail }}</p>
                  </div>
                </article>
              </div>
              <aside class="activity-summary">
                <header class="workspace-panel-heading">
                  <div>
                    <h3>最近 24 小时</h3>
                    <p>中心控制面记录</p>
                  </div>
                  <Clock />
                </header>
                <dl>
                  <div>
                    <dt>扫描任务</dt>
                    <dd>4</dd>
                  </div>
                  <div>
                    <dt>Commit</dt>
                    <dd>2</dd>
                  </div>
                  <div>
                    <dt>对象上传</dt>
                    <dd>34.1 GiB</dd>
                  </div>
                  <div>
                    <dt>CAS 冲突</dt>
                    <dd>0</dd>
                  </div>
                  <div>
                    <dt>失败任务</dt>
                    <dd>0</dd>
                  </div>
                </dl>
              </aside>
            </div>
          </el-tab-pane>
        </el-tabs>
      </section>
    </template>
    <div v-else-if="playgroundQuery.isPending.value" class="page-loading">
      <el-skeleton :rows="8" animated />
    </div>

    <el-drawer v-model="metadataDrawerOpen" title="文件元数据" size="min(720px, 100vw)">
      <template v-if="selectedDiff">
        <section class="file-metadata-heading">
          <el-tag :type="changeTagType(selectedDiff.type)" effect="plain">
            {{ changeTypeLabel(selectedDiff.type) }}
          </el-tag>
          <code>{{ selectedDiff.path }}</code>
          <p v-if="selectedDiff.previousPath">原路径 {{ selectedDiff.previousPath }}</p>
        </section>

        <section class="drawer-section">
          <div class="workspace-panel-heading">
            <div>
              <h3>FileRecord / Manifest</h3>
              <p>中心 Index 中的规范化文件记录</p>
            </div>
            <el-tag effect="plain">{{ selectedDiff.format }}</el-tag>
          </div>
          <dl class="metadata-definition metadata-definition--drawer">
            <div>
              <dt>逻辑大小</dt>
              <dd>{{ selectedDiff.size }}</dd>
            </div>
            <div>
              <dt>Object 数量</dt>
              <dd>{{ selectedDiff.objectCount }}</dd>
            </div>
            <div>
              <dt>Manifest ID</dt>
              <dd>
                <code>manifest:{{ selectedDiff.digest.replace('b3:', '') }}</code>
              </dd>
            </div>
            <div>
              <dt>当前 Digest</dt>
              <dd>
                <code>{{ selectedDiff.digest }}</code>
              </dd>
            </div>
            <div>
              <dt>父版本 Digest</dt>
              <dd>
                <code>{{ selectedDiff.previousDigest ?? '—' }}</code>
              </dd>
            </div>
            <div>
              <dt>路径规范</dt>
              <dd>NFC · repository-relative</dd>
            </div>
          </dl>
        </section>

        <section class="drawer-section">
          <div class="workspace-panel-heading">
            <div>
              <h3>Chunk / Object 分布</h3>
              <p>内容寻址对象的逻辑布局预览</p>
            </div>
            <span>{{ selectedDiff.objectCount }} objects</span>
          </div>
          <div class="chunk-map" aria-label="Chunk 分布">
            <span
              v-for="index in 18"
              :key="index"
              :class="{ 'is-reused': index % 4 !== 0 }"
              :style="{ flexGrow: (index % 5) + 1 }"
            />
          </div>
          <div class="chunk-legend">
            <span><i class="is-reused" />父版本复用 · 82%</span>
            <span><i />本次新增 · 18%</span>
          </div>
        </section>

        <section v-if="selectedDiff.format === 'Parquet'" class="drawer-section">
          <div class="workspace-panel-heading">
            <div>
              <h3>Schema Diff</h3>
              <p>Dataset Profile 提取的字段变化</p>
            </div>
            <el-tag type="warning" effect="plain">2 新增 · 2 修改</el-tag>
          </div>
          <div class="schema-table">
            <div class="schema-table__head">
              <span>字段</span><span>父版本</span><span>当前 Index</span>
            </div>
            <div v-for="field in schemaFields" :key="field.name" :class="`is-${field.change}`">
              <code>{{ field.name }}</code
              ><span>{{ field.before }}</span
              ><strong>{{ field.after }}</strong>
            </div>
          </div>
        </section>

        <section class="drawer-section">
          <div class="workspace-panel-heading">
            <div>
              <h3>数据统计</h3>
              <p>模拟 Profile 数据，不进入 Commit 内容身份</p>
            </div>
          </div>
          <div class="profile-metrics profile-metrics--drawer">
            <div><span>Rows</span><strong>12,842,731</strong></div>
            <div><span>Columns</span><strong>36</strong></div>
            <div><span>Row groups</span><strong>148</strong></div>
            <div><span>Null ratio</span><strong>0.18%</strong></div>
            <div><span>Compression</span><strong>ZSTD · 4.7x</strong></div>
            <div><span>Profile state</span><strong>Ready</strong></div>
          </div>
        </section>
      </template>
    </el-drawer>
  </div>
</template>

<style scoped>
.playground-console {
  padding-bottom: 18px;
}

.playground-summary {
  grid-template-columns: repeat(5, minmax(0, 1fr));
}

.precommit-status-band {
  display: grid;
  grid-template-columns: 38px minmax(260px, 1fr) minmax(360px, 1.2fr);
  align-items: center;
  gap: 14px;
  margin-bottom: 18px;
  padding: 15px 18px;
  border: 1px solid #aac5b9;
  border-left: 3px solid var(--green);
  background: #edf6f1;
}

.precommit-status-band.is-running {
  grid-template-columns: 38px minmax(220px, 1fr) minmax(320px, 1.1fr) auto;
  border-color: #dbc28f;
  border-left-color: #b57413;
  background: #fff9ec;
}

.precommit-status-band.is-unavailable {
  border-color: #e2b5b1;
  border-left-color: #b64239;
  background: #fff4f3;
}

.precommit-status-band.is-creating {
  border-color: #dbc28f;
  border-left-color: var(--amber);
  background: #fff9ec;
}

.precommit-status-band__icon {
  width: 36px;
  height: 36px;
  display: grid;
  place-items: center;
  color: var(--green);
  background: #d9ebe2;
}

.is-running .precommit-status-band__icon {
  color: #9d650f;
  background: #f4e3bd;
}

.is-unavailable .precommit-status-band__icon {
  color: #a43c34;
  background: #f2d8d5;
}

.is-creating .precommit-status-band__icon {
  color: #9d650f;
  background: #f4e3bd;
}

.precommit-status-band__icon svg {
  width: 19px;
}

.precommit-status-band__body {
  min-width: 0;
}

.precommit-status-band__body strong,
.precommit-status-band__body p {
  margin: 0;
}

.precommit-status-band__body strong {
  font-size: 12px;
}

.precommit-status-band__body p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 10px;
  line-height: 1.5;
}

.precommit-status-band__body :deep(.el-progress) {
  margin-top: 8px;
}

.precommit-status-band dl {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin: 0;
  border-left: 1px solid rgba(95, 112, 103, 0.18);
}

.precommit-status-band dl > div {
  min-width: 0;
  padding: 5px 12px;
}

.precommit-status-band dt {
  color: var(--muted);
  font-size: 9px;
}

.precommit-status-band dd {
  margin: 4px 0 0;
  font-size: 10px;
  font-weight: 700;
}

.precommit-status-band__actions {
  display: flex;
  flex-direction: column;
  gap: 7px;
}

.workspace-tabs :deep(.el-tabs__header) {
  margin: 0 -24px 20px;
  padding: 0 24px;
}

.freshness-bar {
  display: grid;
  grid-template-columns: 24px minmax(0, 1fr) auto;
  align-items: center;
  gap: 11px;
  padding: 13px 15px;
  border-left: 3px solid var(--green);
  background: #edf6f1;
}

.freshness-bar > svg {
  width: 20px;
  color: var(--green);
}

.freshness-bar.is-running {
  border-left-color: #b57413;
  background: #fff9ec;
}

.freshness-bar.is-running > svg {
  color: #b57413;
}

.freshness-bar strong,
.freshness-bar span {
  display: block;
}

.freshness-bar strong {
  font-size: 12px;
}

.freshness-bar span {
  margin-top: 3px;
  color: var(--muted);
  font-size: 10px;
}

.workspace-metrics {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  margin-top: 16px;
  border: 1px solid var(--line);
}

.workspace-metrics > div {
  min-width: 0;
  padding: 14px;
  border-right: 1px solid var(--line);
}

.workspace-metrics > div:last-child {
  border-right: 0;
}

.workspace-metrics span,
.workspace-metrics strong,
.workspace-metrics small {
  display: block;
}

.workspace-metrics span,
.workspace-metrics small {
  color: var(--muted);
  font-size: 10px;
}

.workspace-metrics strong {
  margin: 5px 0 3px;
  font-size: 17px;
}

.workspace-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 300px;
  gap: 16px;
  margin-top: 16px;
}

.workspace-diff-panel,
.workspace-side-panel,
.metadata-layout > section,
.activity-timeline,
.activity-summary {
  min-width: 0;
  border: 1px solid var(--line);
  background: #fff;
}

.workspace-diff-panel,
.workspace-side-panel,
.metadata-layout > section,
.activity-summary {
  padding: 18px;
}

.workspace-panel-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  margin-bottom: 14px;
}

.workspace-panel-heading h3,
.workspace-panel-heading p {
  margin: 0;
}

.workspace-panel-heading h3 {
  font-size: 13px;
}

.workspace-panel-heading p,
.workspace-panel-heading > span {
  margin-top: 4px;
  color: var(--muted);
  font-size: 10px;
}

.workspace-panel-heading > svg {
  width: 22px;
  color: var(--green);
}

.diff-toolbar {
  display: grid;
  grid-template-columns: 180px minmax(220px, 1fr);
  gap: 9px;
  margin-bottom: 12px;
}

.workspace-diff-table {
  border-top: 1px solid var(--line);
}

.workspace-diff-table__head,
.workspace-diff-table__row {
  min-width: 0;
  display: grid;
  grid-template-columns: 68px minmax(230px, 1fr) 90px 82px 82px 58px;
  align-items: center;
  gap: 9px;
  padding: 10px 6px;
  border-bottom: 1px solid var(--line);
}

.workspace-diff-table__head {
  color: var(--muted);
  background: #f5f7f6;
  font-size: 9px;
}

.workspace-diff-table__row {
  min-height: 48px;
  font-size: 10px;
}

.workspace-diff-table__row:hover {
  background: #f8faf9;
}

.file-path {
  min-width: 0;
}

.file-path code,
.file-path small {
  display: block;
  overflow-wrap: anywhere;
}

.file-path small {
  margin-top: 4px;
  color: var(--muted);
  font-size: 9px;
}

.workspace-diff-table__row > strong {
  text-align: right;
}

.mobile-workspace-diff,
.mobile-metadata-files {
  display: none;
}

.diff-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding-top: 12px;
  color: var(--muted);
  font-size: 10px;
}

.directory-bars > div {
  padding: 9px 0;
  border-bottom: 1px solid var(--line);
}

.directory-bars > div:last-child {
  border-bottom: 0;
}

.directory-bars span {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  font-size: 10px;
}

.directory-bars code {
  max-width: 170px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.directory-bars i {
  height: 5px;
  display: block;
  margin-top: 7px;
  overflow: hidden;
  background: #e8ecea;
}

.directory-bars b {
  height: 100%;
  display: block;
  background: var(--green);
}

.directory-bars small {
  display: block;
  margin-top: 5px;
  color: var(--muted);
  font-size: 9px;
}

.visibility-note,
.identity-note {
  display: grid;
  grid-template-columns: 20px minmax(0, 1fr);
  gap: 9px;
  margin-top: 16px;
  padding: 12px;
  border-left: 3px solid #c5811a;
  background: #fff9ec;
}

.visibility-note > svg {
  width: 18px;
  color: #b57413;
}

.visibility-note strong,
.visibility-note p {
  margin: 0;
}

.visibility-note strong {
  font-size: 10px;
}

.visibility-note p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 9px;
  line-height: 1.5;
}

.files-heading .el-input {
  width: min(320px, 100%);
}

.metadata-file-table {
  border-top: 1px solid var(--line);
}

.metadata-file-table__head,
.metadata-file-table > button {
  width: 100%;
  min-width: 0;
  display: grid;
  grid-template-columns: minmax(260px, 1fr) 85px 80px 100px 65px 110px;
  align-items: center;
  gap: 10px;
  border: 0;
  border-bottom: 1px solid var(--line);
  padding: 12px 8px;
  background: transparent;
  font-size: 10px;
  text-align: left;
}

.metadata-file-table__head {
  color: var(--muted);
  background: #f5f7f6;
  font-size: 9px;
}

.metadata-file-table > button {
  cursor: pointer;
}

.metadata-file-table > button:hover {
  background: #f8faf9;
}

.metadata-layout {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 16px;
}

.metadata-layout .metadata-wide {
  grid-column: 1 / -1;
}

.metadata-definition {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  margin: 0;
  border-top: 1px solid var(--line);
}

.metadata-definition > div {
  min-width: 0;
  padding: 12px 8px;
  border-bottom: 1px solid var(--line);
}

.metadata-definition dt {
  color: var(--muted);
  font-size: 9px;
}

.metadata-definition dd {
  margin: 5px 0 0;
  font-size: 10px;
  font-weight: 650;
  overflow-wrap: anywhere;
}

.profile-metrics {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  border: 1px solid var(--line);
}

.profile-metrics > div {
  min-width: 0;
  padding: 14px;
  border-right: 1px solid var(--line);
}

.profile-metrics > div:last-child {
  border-right: 0;
}

.profile-metrics span,
.profile-metrics strong {
  display: block;
}

.profile-metrics span {
  color: var(--muted);
  font-size: 9px;
}

.profile-metrics strong {
  margin-top: 5px;
  font-size: 11px;
  overflow-wrap: anywhere;
}

.activity-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 280px;
  gap: 16px;
}

.activity-timeline {
  padding: 18px 18px 8px;
}

.activity-timeline article {
  position: relative;
  min-width: 0;
  display: grid;
  grid-template-columns: 10px 100px minmax(0, 1fr);
  gap: 10px;
  padding-bottom: 20px;
}

.activity-timeline article::before {
  content: '';
  position: absolute;
  left: 4px;
  top: 12px;
  bottom: 0;
  width: 1px;
  background: var(--line);
}

.activity-timeline article:last-child::before {
  display: none;
}

.activity-dot {
  position: relative;
  z-index: 1;
  width: 9px;
  height: 9px;
  margin-top: 3px;
  border: 2px solid #fff;
  border-radius: 50%;
  background: #83908a;
  box-shadow: 0 0 0 1px #a8b2ad;
}

.activity-timeline article.is-success .activity-dot {
  background: var(--green);
  box-shadow: 0 0 0 1px #5f9d84;
}

.activity-timeline article.is-warning .activity-dot {
  background: #c5811a;
  box-shadow: 0 0 0 1px #d2a45d;
}

.activity-timeline article.is-running .activity-dot {
  background: #3c7fa7;
  box-shadow: 0 0 0 1px #6ba0c0;
}

.activity-timeline time {
  color: var(--muted);
  font-size: 10px;
}

.activity-timeline strong,
.activity-timeline p {
  margin: 0;
}

.activity-timeline strong {
  font-size: 11px;
}

.activity-timeline p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 10px;
}

.activity-summary dl {
  margin: 0;
  border-top: 1px solid var(--line);
}

.activity-summary dl > div {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  padding: 11px 2px;
  border-bottom: 1px solid var(--line);
  font-size: 10px;
}

.activity-summary dt {
  color: var(--muted);
}

.activity-summary dd {
  margin: 0;
  font-weight: 700;
}

.file-metadata-heading {
  padding-bottom: 20px;
  border-bottom: 1px solid var(--line);
}

.file-metadata-heading code,
.file-metadata-heading p {
  display: block;
  margin-top: 10px;
  overflow-wrap: anywhere;
}

.file-metadata-heading p {
  color: var(--muted);
  font-size: 10px;
}

.drawer-section {
  padding: 22px 0;
  border-bottom: 1px solid var(--line);
}

.metadata-definition--drawer {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.chunk-map {
  height: 48px;
  display: flex;
  gap: 3px;
  padding: 7px;
  border: 1px solid var(--line);
  background: #f2f5f3;
}

.chunk-map span {
  min-width: 4px;
  background: #d19432;
}

.chunk-map span.is-reused {
  background: var(--green);
}

.chunk-legend {
  display: flex;
  gap: 16px;
  margin-top: 9px;
  color: var(--muted);
  font-size: 9px;
}

.chunk-legend span {
  display: inline-flex;
  align-items: center;
  gap: 5px;
}

.chunk-legend i {
  width: 8px;
  height: 8px;
  background: #d19432;
}

.chunk-legend i.is-reused {
  background: var(--green);
}

.schema-table {
  border-top: 1px solid var(--line);
}

.schema-table__head,
.schema-table > div {
  display: grid;
  grid-template-columns: minmax(150px, 1fr) 130px 130px;
  gap: 10px;
  padding: 10px 6px;
  border-bottom: 1px solid var(--line);
  font-size: 10px;
}

.schema-table__head {
  color: var(--muted);
  background: #f5f7f6;
  font-size: 9px;
}

.schema-table > div.is-added {
  background: #edf6f1;
}

.schema-table > div.is-modified {
  background: #fff9ec;
}

.profile-metrics--drawer {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.profile-metrics--drawer > div:nth-child(3) {
  border-right: 0;
}

.profile-metrics--drawer > div:nth-child(-n + 3) {
  border-bottom: 1px solid var(--line);
}

@media (max-width: 1000px) {
  .playground-summary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .playground-summary > div:nth-child(2n) {
    border-right: 0;
  }

  .playground-summary > div {
    border-bottom: 1px solid var(--line);
  }

  .playground-summary > div:last-child {
    grid-column: 1 / -1;
    border-right: 0;
    border-bottom: 0;
  }

  .precommit-status-band {
    grid-template-columns: 38px minmax(0, 1fr);
  }

  .precommit-status-band.is-running {
    grid-template-columns: 38px minmax(0, 1fr);
  }

  .precommit-status-band dl {
    grid-column: 2;
    border-left: 0;
  }

  .precommit-status-band__actions {
    grid-column: 2;
    flex-direction: row;
  }

  .workspace-metrics {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .workspace-metrics > div:nth-child(3) {
    border-right: 0;
  }

  .workspace-metrics > div:nth-child(-n + 3) {
    border-bottom: 1px solid var(--line);
  }

  .workspace-grid,
  .activity-layout {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 700px) {
  :deep(.page-heading) {
    flex-direction: column;
  }

  :deep(.page-heading__actions) {
    width: 100%;
    justify-content: flex-start;
  }

  .playground-summary {
    grid-template-columns: 1fr;
  }

  .playground-summary > div {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .playground-summary > div:last-child {
    grid-column: auto;
    border-bottom: 0;
  }

  .precommit-status-band {
    grid-template-columns: 32px minmax(0, 1fr);
    align-items: start;
    padding: 14px;
  }

  .precommit-status-band.is-running {
    grid-template-columns: 32px minmax(0, 1fr);
  }

  .precommit-status-band__icon {
    width: 30px;
    height: 30px;
  }

  .precommit-status-band dl {
    grid-column: 1 / -1;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    border-top: 1px solid rgba(95, 112, 103, 0.18);
  }

  .precommit-status-band__actions {
    grid-column: 1 / -1;
    width: 100%;
  }

  .precommit-status-band__actions .el-button {
    flex: 1 1 0;
    margin: 0;
  }

  .workspace-tabs :deep(.el-tabs__header) {
    margin: 0 -15px 16px;
    padding: 0 15px;
  }

  .freshness-bar {
    grid-template-columns: 21px minmax(0, 1fr);
  }

  .freshness-bar .el-tag {
    grid-column: 2;
    justify-self: start;
  }

  .workspace-metrics {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .workspace-metrics > div:nth-child(3) {
    border-right: 1px solid var(--line);
  }

  .workspace-metrics > div:nth-child(2n) {
    border-right: 0;
  }

  .workspace-metrics > div:nth-child(-n + 4) {
    border-bottom: 1px solid var(--line);
  }

  .workspace-diff-panel,
  .workspace-side-panel,
  .metadata-layout > section,
  .activity-summary {
    padding: 14px;
  }

  .diff-toolbar,
  .metadata-layout,
  .metadata-definition,
  .profile-metrics,
  .profile-metrics--drawer {
    grid-template-columns: 1fr;
  }

  .desktop-workspace-diff,
  .desktop-metadata-files {
    display: none;
  }

  .mobile-workspace-diff,
  .mobile-metadata-files {
    display: block;
    border-top: 1px solid var(--line);
  }

  .mobile-workspace-diff button,
  .mobile-metadata-files button {
    width: 100%;
    min-width: 0;
    display: block;
    border: 0;
    border-bottom: 1px solid var(--line);
    padding: 12px 2px;
    background: transparent;
    text-align: left;
  }

  .mobile-workspace-diff button > span {
    display: flex;
    align-items: center;
    justify-content: space-between;
  }

  .mobile-workspace-diff code,
  .mobile-workspace-diff small,
  .mobile-metadata-files code,
  .mobile-metadata-files span,
  .mobile-metadata-files small {
    display: block;
    margin-top: 7px;
    font-size: 9px;
    overflow-wrap: anywhere;
  }

  .mobile-workspace-diff small,
  .mobile-metadata-files span,
  .mobile-metadata-files small {
    color: var(--muted);
  }

  .diff-footer,
  .files-heading {
    align-items: flex-start;
    flex-direction: column;
  }

  .files-heading .el-input {
    width: 100%;
  }

  .metadata-layout .metadata-wide {
    grid-column: auto;
  }

  .profile-metrics > div,
  .profile-metrics--drawer > div {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .activity-timeline article {
    grid-template-columns: 10px minmax(0, 1fr);
  }

  .activity-timeline time {
    grid-column: 2;
  }

  .activity-timeline article > div {
    grid-column: 2;
  }

  .schema-table__head,
  .schema-table > div {
    grid-template-columns: minmax(100px, 1fr) 90px 90px;
  }
}
</style>
