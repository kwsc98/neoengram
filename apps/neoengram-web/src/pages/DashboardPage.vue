<script setup lang="ts">
import {
  ArrowRight,
  Box,
  Collection,
  Connection,
  DataLine,
  DocumentCopy,
  RefreshRight,
  Warning,
} from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { liveProbe, queryApiVersion, queryTenant, readyProbe } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const tenantQuery = useQuery({
  queryKey: computed(() => ['tenant', tenantId.value]),
  queryFn: () => queryTenant(tenantId.value),
});
const versionQuery = useQuery({ queryKey: ['system', 'version'], queryFn: queryApiVersion });
const liveQuery = useQuery({
  queryKey: ['health', 'live'],
  queryFn: liveProbe,
  refetchInterval: 15_000,
});
const readyQuery = useQuery({
  queryKey: ['health', 'ready'],
  queryFn: readyProbe,
  refetchInterval: 15_000,
});
const tenant = computed(() => tenantQuery.data.value?.data.tenant);
const version = computed(() => versionQuery.data.value?.data);
const firstError = computed(
  () =>
    tenantQuery.error.value ??
    versionQuery.error.value ??
    liveQuery.error.value ??
    readyQuery.error.value,
);
const refreshing = computed(
  () =>
    tenantQuery.isFetching.value ||
    versionQuery.isFetching.value ||
    liveQuery.isFetching.value ||
    readyQuery.isFetching.value,
);

const resourceLinks = [
  {
    name: 'artifact-list',
    label: '数据资产',
    value: '12',
    meta: '3 个今日更新',
    icon: Box,
  },
  {
    name: 'playground-list',
    label: '活跃工作区',
    value: '5',
    meta: '2 个存在待提交变化',
    icon: Collection,
  },
  {
    name: 'snapshot-list',
    label: '可用快照',
    value: '28',
    meta: '覆盖 3 个区域',
    icon: DocumentCopy,
  },
];

const attentionItems = [
  {
    title: '标注工作区存在 128 个待提交文件',
    meta: 'road-scenes / labeling · 2 分钟前完成扫描',
    action: 'playground-list',
    level: 'warning',
  },
  {
    title: '广州区域快照正在物化',
    meta: 'road-scenes@commit-main-3 · 64% · 预计 6 分钟',
    action: 'snapshot-list',
    level: 'progress',
  },
  {
    title: '北京归档存储没有可用执行 Agent',
    meta: 'volume-beijing-archive · 已阻止新的交付任务',
    action: 'storage-volume-list',
    level: 'danger',
  },
];

const recentVersions = [
  {
    title: '补充夜间道路场景',
    artifact: 'road-scenes',
    commit: 'commit-main-3',
    tags: ['dataset/v4', 'release-candidate'],
    size: '+18.6 GiB',
    time: '今天 11:42',
  },
  {
    title: '完成安全对话首轮复核',
    artifact: 'dialog-corpus',
    commit: 'dialog-commit-2',
    tags: ['safety-reviewed'],
    size: '+2.1 GiB',
    time: '今天 09:18',
  },
  {
    title: '建立评测集基线',
    artifact: 'evaluation-suite',
    commit: 'eval-commit-7',
    tags: ['baseline/v1'],
    size: '+840 MiB',
    time: '昨天 18:07',
  },
];

async function refresh(): Promise<void> {
  await Promise.all([
    tenantQuery.refetch(),
    versionQuery.refetch(),
    liveQuery.refetch(),
    readyQuery.refetch(),
  ]);
}

async function openResource(name: string): Promise<void> {
  await router.push({ name, params: { tenantId: tenantId.value } });
}
</script>

<template>
  <div class="page tenant-home">
    <PageHeading
      :title="tenant?.display_name ?? '租户概览'"
      description="数据版本、工作区与区域交付的当前状态"
    >
      <template #actions>
        <span class="control-health">
          <span class="status-dot" :class="readyQuery.data.value ? 'status-dot--ok' : ''" />
          {{ readyQuery.data.value ? '控制面正常' : '状态检查中' }}
        </span>
        <el-button :icon="RefreshRight" :loading="refreshing" @click="refresh">刷新</el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="firstError"
      :error="firstError"
      :retrying="refreshing"
      @retry="refresh"
    />

    <section class="home-metrics" aria-label="核心资源">
      <button
        v-for="item in resourceLinks"
        :key="item.name"
        type="button"
        @click="openResource(item.name)"
      >
        <span class="home-metrics__icon"><component :is="item.icon" /></span>
        <span class="home-metrics__copy">
          <small>{{ item.label }}</small>
          <strong>{{ item.value }}</strong>
          <span>{{ item.meta }}</span>
        </span>
        <ArrowRight />
      </button>
    </section>

    <div class="home-layout">
      <section class="home-section" aria-labelledby="attention-title">
        <header>
          <div>
            <h2 id="attention-title">需要关注</h2>
            <p>阻塞提交或区域交付的事项</p>
          </div>
          <el-tag type="warning" effect="plain">3 项</el-tag>
        </header>
        <div class="attention-list">
          <button
            v-for="item in attentionItems"
            :key="item.title"
            type="button"
            @click="openResource(item.action)"
          >
            <span class="attention-list__signal" :class="`is-${item.level}`">
              <Warning />
            </span>
            <span>
              <strong>{{ item.title }}</strong>
              <small>{{ item.meta }}</small>
            </span>
            <ArrowRight />
          </button>
        </div>
      </section>

      <section class="home-section" aria-labelledby="delivery-title">
        <header>
          <div>
            <h2 id="delivery-title">区域交付</h2>
            <p>固定数据版本在计算区域的可用性</p>
          </div>
          <el-button text type="primary" @click="openResource('snapshot-list')">查看全部</el-button>
        </header>
        <div class="region-readiness">
          <div>
            <span><i class="readiness-dot is-ready" />cn-shanghai</span>
            <strong>12 Ready</strong>
          </div>
          <div>
            <span><i class="readiness-dot is-progress" />cn-guangzhou</span>
            <strong>9 Ready · 1 进行中</strong>
          </div>
          <div>
            <span><i class="readiness-dot is-blocked" />cn-beijing</span>
            <strong>7 Ready · 当前不可调度</strong>
          </div>
        </div>
      </section>
    </div>

    <section class="home-section home-section--versions" aria-labelledby="versions-title">
      <header>
        <div>
          <h2 id="versions-title">最近数据版本</h2>
          <p>由 Playground 发布的不可变 Commit</p>
        </div>
        <el-button text type="primary" @click="openResource('artifact-list')">浏览资产</el-button>
      </header>
      <div class="version-list">
        <div v-for="versionItem in recentVersions" :key="versionItem.title">
          <span class="version-list__branch"><DataLine /></span>
          <span>
            <strong>{{ versionItem.title }}</strong>
            <small
              ><code>{{ versionItem.artifact }}</code> · {{ versionItem.commit }}</small
            >
            <span class="version-list__tags">
              <el-tag
                v-for="tagName in versionItem.tags"
                :key="tagName"
                size="small"
                effect="plain"
              >
                {{ tagName }}
              </el-tag>
            </span>
          </span>
          <span class="version-list__size">{{ versionItem.size }}</span>
          <time>{{ versionItem.time }}</time>
        </div>
      </div>
    </section>

    <footer class="system-strip">
      <span><Connection /> API v{{ version?.api_versions.join(', ') ?? '—' }}</span>
      <span><i class="status-dot status-dot--ok" />服务存活</span>
      <span><i class="status-dot status-dot--ok" />Authority 就绪</span>
      <code>{{ tenantId }}</code>
    </footer>
  </div>
</template>

<style scoped>
.tenant-home {
  --home-border: #d8dfdc;
}

.control-health {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: var(--muted);
  font-size: 12px;
}

.home-metrics {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  border: 1px solid var(--home-border);
  background: #fff;
}

.home-metrics button {
  min-width: 0;
  min-height: 126px;
  display: grid;
  grid-template-columns: 40px minmax(0, 1fr) 18px;
  align-items: center;
  gap: 14px;
  border: 0;
  border-right: 1px solid var(--home-border);
  padding: 20px;
  background: transparent;
  cursor: pointer;
  text-align: left;
}

.home-metrics button:last-child {
  border-right: 0;
}

.home-metrics button:hover {
  background: #f7faf8;
}

.home-metrics__icon {
  width: 40px;
  height: 40px;
  display: grid;
  place-items: center;
  border-radius: 6px;
  color: var(--green);
  background: #e5f0eb;
  font-size: 19px;
}

.home-metrics__copy,
.home-metrics__copy > * {
  display: block;
}

.home-metrics__copy small,
.home-metrics__copy span {
  color: var(--muted);
  font-size: 12px;
}

.home-metrics__copy strong {
  margin: 4px 0;
  font-size: 25px;
}

.home-metrics button > svg {
  width: 16px;
  color: #89938f;
}

.home-layout {
  display: grid;
  grid-template-columns: minmax(0, 1.35fr) minmax(320px, 0.85fr);
  gap: 20px;
  margin-top: 20px;
}

.home-section {
  border: 1px solid var(--home-border);
  background: #fff;
}

.home-section > header {
  min-height: 74px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 17px 20px;
  border-bottom: 1px solid var(--home-border);
}

.home-section h2,
.home-section p {
  margin: 0;
}

.home-section h2 {
  font-size: 15px;
}

.home-section p {
  margin-top: 5px;
  color: var(--muted);
  font-size: 12px;
}

.attention-list button {
  width: 100%;
  min-width: 0;
  display: grid;
  grid-template-columns: 30px minmax(0, 1fr) 18px;
  align-items: center;
  gap: 12px;
  border: 0;
  border-bottom: 1px solid #e5e9e7;
  padding: 15px 20px;
  background: transparent;
  cursor: pointer;
  text-align: left;
}

.attention-list button:last-child {
  border-bottom: 0;
}

.attention-list button:hover {
  background: #f8faf9;
}

.attention-list__signal {
  width: 28px;
  height: 28px;
  display: grid;
  place-items: center;
  border-radius: 50%;
  color: #a96708;
  background: #fff4d9;
}

.attention-list__signal.is-progress {
  color: #286a91;
  background: #e4f1f8;
}

.attention-list__signal.is-danger {
  color: #a53838;
  background: #fae8e8;
}

.attention-list__signal svg,
.attention-list button > svg {
  width: 15px;
}

.attention-list strong,
.attention-list small {
  display: block;
}

.attention-list strong {
  font-size: 13px;
}

.attention-list small {
  margin-top: 5px;
  color: var(--muted);
  font-size: 11px;
}

.attention-list button > svg {
  color: #89938f;
}

.region-readiness {
  padding: 7px 20px;
}

.region-readiness > div {
  padding: 14px 0;
  border-bottom: 1px solid #e5e9e7;
}

.region-readiness > div:last-child {
  border-bottom: 0;
}

.region-readiness span,
.region-readiness strong {
  display: flex;
  align-items: center;
}

.region-readiness span {
  gap: 8px;
  color: var(--muted);
  font-size: 11px;
}

.region-readiness strong {
  margin: 6px 0 0 14px;
  font-size: 13px;
}

.readiness-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: #2a946d;
}

.readiness-dot.is-progress {
  background: #d08a1e;
}

.readiness-dot.is-blocked {
  background: #b64a4a;
}

.home-section--versions {
  margin-top: 20px;
}

.version-list > div {
  min-width: 0;
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr) 100px 110px;
  align-items: center;
  gap: 12px;
  padding: 14px 20px;
  border-bottom: 1px solid #e5e9e7;
}

.version-list > div:last-child {
  border-bottom: 0;
}

.version-list__branch {
  width: 26px;
  height: 26px;
  display: grid;
  place-items: center;
  border-radius: 50%;
  color: var(--green);
  background: #e5f0eb;
}

.version-list strong,
.version-list small {
  display: block;
}

.version-list strong {
  font-size: 13px;
}

.version-list small,
.version-list time,
.version-list__size {
  color: var(--muted);
  font-size: 11px;
}

.version-list small {
  margin-top: 4px;
}

.version-list__tags {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  margin-top: 7px;
}

.version-list time,
.version-list__size {
  text-align: right;
}

.system-strip {
  display: flex;
  align-items: center;
  gap: 18px;
  margin-top: 20px;
  padding: 11px 14px;
  color: var(--muted);
  background: #e9eeeb;
  font-size: 11px;
}

.system-strip span {
  display: inline-flex;
  align-items: center;
  gap: 7px;
}

.system-strip svg {
  width: 14px;
}

.system-strip code {
  margin-left: auto;
}

@media (max-width: 900px) {
  .home-layout {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 650px) {
  .home-metrics {
    grid-template-columns: 1fr;
  }

  .home-metrics button {
    min-height: 96px;
    border-right: 0;
    border-bottom: 1px solid var(--home-border);
  }

  .home-metrics button:last-child {
    border-bottom: 0;
  }

  .control-health {
    display: none;
  }

  .version-list > div {
    grid-template-columns: 28px minmax(0, 1fr) auto;
  }

  .version-list__size {
    display: none;
  }

  .version-list time {
    max-width: 62px;
  }

  .system-strip {
    flex-wrap: wrap;
    gap: 9px 14px;
  }

  .system-strip code {
    width: 100%;
    margin-left: 0;
  }
}
</style>
