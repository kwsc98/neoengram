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
} from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { queryArtifactCommitDiff, querySnapshot } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { commitTagNames } from '@/utils/commit';
import { formatBytes, formatCount, formatTime } from '@/utils/format';

interface SnapshotFile {
  path: string;
  format: string;
  size: string;
  objects: string;
  digest: string;
}

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const commitId = computed(() => String(route.params.commitId ?? ''));
const activeTab = ref('overview');
const fileSearch = ref('');

const snapshotQuery = useQuery({
  queryKey: computed(() => [
    'snapshot',
    tenantId.value,
    projectId.value,
    artifactId.value,
    commitId.value,
  ]),
  queryFn: () => querySnapshot(tenantId.value, projectId.value, artifactId.value, commitId.value),
});
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
});

const snapshot = computed(() => snapshotQuery.data.value?.data.snapshot);
const commitDiff = computed(() => commitQuery.data.value?.data.diff);
const snapshotTags = computed(() =>
  commitTagNames(commitDiff.value?.target_commit.ref_names ?? snapshot.value?.ref_names ?? []),
);
const refreshing = computed(() => snapshotQuery.isFetching.value || commitQuery.isFetching.value);

const roadSceneFiles: SnapshotFile[] = [
  {
    path: 'dataset/index.json',
    format: 'JSON',
    size: '3.1 MiB',
    objects: '3',
    digest: 'b3:61f7a82c4fe909d8',
  },
  {
    path: 'dataset/night-rain/part-0042.parquet',
    format: 'Parquet',
    size: '18.6 GiB',
    objects: '147',
    digest: 'b3:84c86a69ca3e714f',
  },
  {
    path: 'images/night-rain/shard-023.tar',
    format: 'TAR',
    size: '8.3 GiB',
    objects: '66',
    digest: 'b3:85c84f4b756e43a2',
  },
  {
    path: 'annotations/partition=night/date=2026-07-28/labels.parquet',
    format: 'Parquet',
    size: '2.7 GiB',
    objects: '23',
    digest: 'b3:29a36f8751c76409',
  },
  {
    path: 'schemas/road-scene.avsc',
    format: 'Avro',
    size: '14 KiB',
    objects: '1',
    digest: 'b3:ce546f01d8d09c64',
  },
  {
    path: 'profiles/night-v4.dataset-profile.json',
    format: 'Profile',
    size: '480 KiB',
    objects: '1',
    digest: 'b3:d87c5f31ac08a9fe',
  },
];

const genericFiles: SnapshotFile[] = [
  {
    path: 'dataset/manifest.json',
    format: 'JSON',
    size: '1.8 MiB',
    objects: '2',
    digest: 'b3:4f1c81f220829bef',
  },
  {
    path: 'dataset/part-0001.parquet',
    format: 'Parquet',
    size: '2.1 GiB',
    objects: '32',
    digest: 'b3:724c3618a61f8c7d',
  },
  {
    path: 'schemas/dataset.avsc',
    format: 'Avro',
    size: '9 KiB',
    objects: '1',
    digest: 'b3:b82370a577e559a1',
  },
];

const snapshotFiles = computed(() =>
  artifactId.value === 'road-scenes' ? roadSceneFiles : genericFiles,
);
const filteredFiles = computed(() => {
  const query = fileSearch.value.trim().toLowerCase();
  return snapshotFiles.value.filter((file) => !query || file.path.toLowerCase().includes(query));
});
const profileName = computed(() =>
  artifactId.value === 'road-scenes' ? 'training-dataset-v2' : '未声明',
);

async function refresh(): Promise<void> {
  await Promise.all([snapshotQuery.refetch(), commitQuery.refetch()]);
}

async function openCommit(): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: projectId.value, artifactId: artifactId.value },
    query: { tab: 'commits', commit_id: commitId.value },
  });
}
</script>

<template>
  <div class="page snapshot-detail">
    <PageHeading :title="snapshot?.message ?? commitId" :description="`Snapshot · ${commitId}`">
      <template #actions>
        <el-tag type="success" effect="plain">只读 · Ready</el-tag>
        <el-button
          :icon="Back"
          @click="router.push({ name: 'snapshot-list', params: { tenantId } })"
        >
          返回列表
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

    <template v-if="snapshot">
      <section class="snapshot-summary" aria-label="Snapshot 摘要">
        <div>
          <span>状态</span><strong class="ready-value"><CircleCheck />Ready</strong>
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
          <span>创建时间</span><strong>{{ formatTime(snapshot.created_at_unix_ms) }}</strong>
        </div>
      </section>

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

            <div class="snapshot-overview-grid">
              <section class="snapshot-subsection">
                <header>
                  <div>
                    <h2>存储位置</h2>
                    <p>该 Snapshot 固定在一个区域和一个 StorageVolume</p>
                  </div>
                  <span>单区域 Ready</span>
                </header>
                <div class="placement-table">
                  <div class="placement-table__header">
                    <span>区域</span><span>状态</span><span>StorageVolume</span><span>物化方式</span
                    ><span>完整性</span>
                  </div>
                  <div>
                    <strong><Location />{{ snapshot.region }}</strong>
                    <el-tag type="success" effect="plain">Ready</el-tag>
                    <code>{{ snapshot.storage_volume_id }}</code>
                    <span>本地对象复用</span>
                    <span class="placement-integrity"> <CircleCheck />100% · 3 分钟前 </span>
                  </div>
                </div>
              </section>

              <section class="snapshot-subsection snapshot-policy">
                <header>
                  <div>
                    <h2>完整性与策略</h2>
                    <p>读取行为固定，不随后续 Commit 变化</p>
                  </div>
                </header>
                <dl>
                  <div>
                    <dt>Manifest digest</dt>
                    <dd><code>b3:9e2b74f5d5b89173</code></dd>
                  </div>
                  <div>
                    <dt>对象</dt>
                    <dd>148,296 · verified</dd>
                  </div>
                  <div>
                    <dt>访问模式</dt>
                    <dd>只读</dd>
                  </div>
                  <div>
                    <dt>保留策略</dt>
                    <dd>180 天</dd>
                  </div>
                  <div>
                    <dt>Dataset Profile</dt>
                    <dd>{{ profileName }}</dd>
                  </div>
                  <div>
                    <dt>父 Commit</dt>
                    <dd>
                      <code>{{ commitDiff?.target_commit.parent_commit_id ?? '根 Commit' }}</code>
                    </dd>
                  </div>
                </dl>
              </section>
            </div>

            <section class="snapshot-subsection snapshot-identity">
              <header>
                <div>
                  <h2>复合身份</h2>
                  <p>Snapshot 由 Artifact 与 Commit 唯一确定</p>
                </div>
              </header>
              <dl>
                <div>
                  <dt>Tenant</dt>
                  <dd>{{ snapshot.tenant_id }}</dd>
                </div>
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
              </dl>
            </section>
          </el-tab-pane>

          <el-tab-pane label="文件" name="files">
            <section class="snapshot-files">
              <header>
                <div>
                  <h2>Snapshot 文件清单</h2>
                  <p>只读 Manifest 中的代表性文件</p>
                </div>
                <el-input
                  v-model="fileSearch"
                  clearable
                  :prefix-icon="Search"
                  placeholder="按路径搜索"
                />
              </header>
              <div class="snapshot-file-table desktop-snapshot-files">
                <div class="snapshot-file-table__header">
                  <span>路径</span><span>格式</span><span>大小</span><span>对象</span
                  ><span>Digest</span>
                </div>
                <div v-for="file in filteredFiles" :key="file.path">
                  <span
                    ><Files /><code>{{ file.path }}</code></span
                  >
                  <span>{{ file.format }}</span>
                  <strong>{{ file.size }}</strong>
                  <span>{{ file.objects }}</span>
                  <code>{{ file.digest }}</code>
                </div>
              </div>
              <div class="mobile-snapshot-files">
                <article v-for="file in filteredFiles" :key="file.path">
                  <code>{{ file.path }}</code>
                  <span>{{ file.format }} · {{ file.size }} · {{ file.objects }} objects</span>
                  <small>{{ file.digest }}</small>
                </article>
              </div>
              <footer>
                当前显示 {{ filteredFiles.length }} 个代表文件，共
                {{ formatCount(snapshot.logical_file_count) }} 个文件
              </footer>
            </section>
          </el-tab-pane>

          <el-tab-pane label="活动" name="activity">
            <section class="snapshot-activity">
              <header>
                <h2>交付活动</h2>
                <p>Snapshot 创建、区域物化和完整性校验记录</p>
              </header>
              <el-timeline>
                <el-timeline-item timestamp="今天 16:18" type="success">
                  <strong>Snapshot Ready</strong>
                  <p>{{ snapshot.region }} 已通过 Manifest 与对象完整性校验</p>
                </el-timeline-item>
                <el-timeline-item timestamp="今天 16:13" type="success">
                  <strong>{{ snapshot.region }} 物化完成</strong>
                  <p>已写入 {{ snapshot.storage_volume_id }}</p>
                </el-timeline-item>
                <el-timeline-item timestamp="今天 16:09" type="primary">
                  <strong>区域交付任务开始</strong>
                  <p>目标区域 {{ snapshot.region }}</p>
                </el-timeline-item>
                <el-timeline-item :timestamp="formatTime(snapshot.created_at_unix_ms)">
                  <strong>Snapshot 创建</strong>
                  <p>固定到 {{ snapshot.commit_id }}</p>
                </el-timeline-item>
              </el-timeline>
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
  grid-template-columns: repeat(5, minmax(0, 1fr));
  border: 1px solid var(--line);
  background: var(--surface);
}

.snapshot-summary > div {
  min-width: 0;
  min-height: 88px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 7px;
  padding: 16px;
  border-right: 1px solid var(--line);
}

.snapshot-summary > div:last-child {
  border-right: 0;
}

.snapshot-summary span {
  color: var(--muted);
  font-size: 11px;
}

.snapshot-summary strong {
  overflow-wrap: anywhere;
}

.ready-value {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--green);
}

.ready-value svg {
  width: 16px;
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

.snapshot-overview-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.55fr) minmax(280px, 0.75fr);
  gap: 18px;
  margin-top: 18px;
}

.snapshot-subsection {
  border: 1px solid var(--line);
}

.snapshot-subsection > header,
.snapshot-files > header {
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
.snapshot-activity p {
  margin: 0;
}

.snapshot-subsection h2,
.snapshot-files h2,
.snapshot-activity h2 {
  font-size: 14px;
}

.snapshot-subsection p,
.snapshot-files p,
.snapshot-activity p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 11px;
}

.snapshot-subsection > header > span {
  color: var(--green);
  font-size: 11px;
  font-weight: 650;
}

.placement-table__header,
.placement-table > div {
  display: grid;
  grid-template-columns: 130px 88px minmax(160px, 1fr) 120px minmax(160px, 1fr);
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
}

.placement-table__header {
  color: var(--muted);
  background: var(--surface-soft);
  font-size: 10px;
}

.placement-table > div:not(.placement-table__header) {
  min-height: 58px;
  border-top: 1px solid var(--line);
  font-size: 11px;
}

.placement-table > div:nth-child(2) {
  border-top: 0;
}

.placement-table strong,
.placement-integrity {
  display: flex;
  align-items: center;
  gap: 5px;
}

.placement-table strong svg,
.placement-integrity svg {
  width: 14px;
  color: var(--green);
}

.snapshot-policy dl,
.snapshot-identity dl {
  margin: 0;
}

.snapshot-policy dl > div {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  padding: 11px 16px;
  border-bottom: 1px solid var(--line);
}

.snapshot-policy dl > div:last-child {
  border-bottom: 0;
}

.snapshot-policy dt {
  color: var(--muted);
  font-size: 11px;
}

.snapshot-policy dd {
  margin: 0;
  font-size: 11px;
  font-weight: 650;
  text-align: right;
  overflow-wrap: anywhere;
}

.snapshot-identity {
  margin-top: 18px;
}

.snapshot-identity dl {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.snapshot-identity dl > div {
  min-width: 0;
  padding: 14px 16px;
  border-right: 1px solid var(--line);
}

.snapshot-identity dl > div:last-child {
  border-right: 0;
}

.snapshot-identity dt {
  margin-bottom: 5px;
  color: var(--muted);
  font-size: 10px;
}

.snapshot-identity dd {
  margin: 0;
  font-weight: 650;
  overflow-wrap: anywhere;
}

.snapshot-files {
  border: 1px solid var(--line);
}

.snapshot-files > header .el-input {
  width: min(320px, 45vw);
}

.snapshot-file-table__header,
.snapshot-file-table > div {
  display: grid;
  grid-template-columns: minmax(300px, 1.5fr) 100px 100px 80px minmax(190px, 1fr);
  align-items: center;
  gap: 12px;
  padding: 12px 16px;
}

.snapshot-file-table__header {
  color: var(--muted);
  background: var(--surface-soft);
  font-size: 10px;
}

.snapshot-file-table > div:not(.snapshot-file-table__header) {
  min-height: 54px;
  border-top: 1px solid var(--line);
  font-size: 11px;
}

.snapshot-file-table > div:nth-child(2) {
  border-top: 0;
}

.snapshot-file-table > div > span:first-child {
  min-width: 0;
  display: flex;
  align-items: center;
  gap: 7px;
}

.snapshot-file-table svg {
  width: 14px;
  flex: 0 0 auto;
  color: var(--green);
}

.snapshot-file-table code {
  overflow-wrap: anywhere;
}

.snapshot-files > footer {
  padding: 12px 16px;
  border-top: 1px solid var(--line);
  color: var(--muted);
  font-size: 11px;
}

.mobile-snapshot-files {
  display: none;
}

.snapshot-activity {
  max-width: 820px;
  padding: 8px 8px 0;
}

.snapshot-activity > header {
  margin-bottom: 24px;
}

.snapshot-activity strong {
  font-size: 13px;
}

.snapshot-activity :deep(.el-timeline-item__timestamp) {
  color: var(--muted);
}

@media (max-width: 1100px) {
  .snapshot-summary {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .snapshot-summary > div:nth-child(3) {
    border-right: 0;
  }

  .snapshot-summary > div:nth-child(n + 4) {
    border-top: 1px solid var(--line);
  }

  .snapshot-overview-grid {
    grid-template-columns: 1fr;
  }

  .placement-table__header,
  .placement-table > div {
    grid-template-columns: 120px 84px minmax(150px, 1fr) 110px minmax(150px, 1fr);
  }
}

@media (max-width: 700px) {
  .snapshot-summary {
    grid-template-columns: 1fr 1fr;
  }

  .snapshot-summary > div,
  .snapshot-summary > div:nth-child(3) {
    min-height: 76px;
    border-right: 1px solid var(--line);
    border-top: 1px solid var(--line);
  }

  .snapshot-summary > div:nth-child(-n + 2) {
    border-top: 0;
  }

  .snapshot-summary > div:nth-child(even) {
    border-right: 0;
  }

  .snapshot-summary > div:last-child {
    grid-column: 1 / -1;
    border-right: 0;
  }

  .snapshot-detail-shell {
    padding: 8px 12px 18px;
  }

  .fixed-commit-band {
    grid-template-columns: 36px minmax(0, 1fr);
  }

  .fixed-commit-band__tags,
  .fixed-commit-band .el-button {
    grid-column: 1 / -1;
  }

  .snapshot-subsection > header,
  .snapshot-files > header {
    align-items: flex-start;
    flex-direction: column;
  }

  .placement-table > .placement-table__header {
    display: none;
  }

  .placement-table > div:not(.placement-table__header) {
    grid-template-columns: 1fr auto;
    gap: 8px 12px;
    padding: 14px;
  }

  .placement-table > div > code,
  .placement-table > div > span:nth-child(4),
  .placement-integrity {
    grid-column: 1 / -1;
  }

  .snapshot-identity dl {
    grid-template-columns: 1fr;
  }

  .snapshot-policy dl > div {
    flex-direction: column;
    gap: 5px;
  }

  .snapshot-policy dd {
    max-width: 100%;
    text-align: left;
  }

  .snapshot-identity dl > div {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .snapshot-identity dl > div:last-child {
    border-bottom: 0;
  }

  .snapshot-files > header .el-input {
    width: 100%;
  }

  .desktop-snapshot-files {
    display: none;
  }

  .mobile-snapshot-files {
    display: block;
  }

  .mobile-snapshot-files article {
    padding: 14px;
    border-bottom: 1px solid var(--line);
  }

  .mobile-snapshot-files code,
  .mobile-snapshot-files span,
  .mobile-snapshot-files small {
    display: block;
    overflow-wrap: anywhere;
  }

  .mobile-snapshot-files span,
  .mobile-snapshot-files small {
    margin-top: 6px;
    color: var(--muted);
    font-size: 10px;
  }
}
</style>
