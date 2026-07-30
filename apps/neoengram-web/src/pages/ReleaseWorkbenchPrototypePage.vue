<script setup lang="ts">
import {
  ArrowRight,
  Back,
  CircleCheck,
  CircleClose,
  Files,
  Plus,
  RefreshRight,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useMutation, useQuery } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, onBeforeUnmount, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  cancelPlaygroundPreCommit,
  commitPlayground,
  queryPlayground,
  queryPlaygroundPreCommit,
  startPlaygroundPreCommit,
} from '@/api/operations';
import type { PreCommitView } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
  advancePrototypePreCommit,
  cancelPrototypePreCommit,
  getActivePreCommit,
  preCommitScopeKey,
  startPrototypePreCommit,
  type PreCommitPhase,
} from '@/features/precommit/prototype';

interface PreparationStep {
  phase: PreCommitPhase;
  label: string;
  detail: string;
  progress: number;
}

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const playgroundId = computed(() => String(route.params.playgroundId ?? ''));
const playgroundPreCommitKey = computed(() =>
  preCommitScopeKey(tenantId.value, projectId.value, artifactId.value, playgroundId.value),
);

const playgroundQuery = useQuery({
  queryKey: computed(() => [
    'playground',
    tenantId.value,
    projectId.value,
    artifactId.value,
    playgroundId.value,
    'precommit',
  ]),
  queryFn: () =>
    queryPlayground(tenantId.value, projectId.value, artifactId.value, playgroundId.value),
});
const playground = computed(() => playgroundQuery.data.value?.data.playground);
const commitMutation = useMutation({ mutationFn: commitPlayground });
const preparationIndex = ref(0);
const preparationStarted = ref(false);
const commitDialogOpen = ref(false);
const commitMessage = ref('补充夜间道路场景数据');
const commitDescription = ref('补充低照度和雨夜场景，修正 17 条标注记录，并更新训练集索引。');
const tagInput = ref('');
const tagNames = ref<string[]>([]);
const commitError = ref('');
const createdCommitId = ref('');
const createdParentCommitId = ref('');
const apiPreCommit = ref<PreCommitView>();
let preparationTimer: ReturnType<typeof globalThis.setTimeout> | undefined;

const preparationSteps: PreparationStep[] = [
  { phase: 'queued', label: '等待 Agent', detail: '已绑定目标 Playground', progress: 8 },
  { phase: 'scanning', label: '正在扫描工作区', detail: '读取路径、大小和文件身份', progress: 28 },
  {
    phase: 'hashing',
    label: '正在计算内容摘要',
    detail: '生成 Manifest 并识别复用对象',
    progress: 52,
  },
  { phase: 'uploading', label: '正在上传新增对象', detail: '写入中心耐久对象存储', progress: 71 },
  {
    phase: 'validating',
    label: '正在执行中心校验',
    detail: '校验 Index 和对象完整性',
    progress: 88,
  },
  { phase: 'ready', label: '预检测完成', detail: 'Diff 与提交检查已经就绪', progress: 100 },
];

const diffRows = [
  {
    type: '修改',
    tagType: 'warning' as const,
    path: 'dataset/index.json',
    previous: '2.8 MiB',
    current: '3.1 MiB',
    impact: '+312 KiB',
  },
  {
    type: '新增',
    tagType: 'success' as const,
    path: 'dataset/night-rain/part-0042.parquet',
    previous: '—',
    current: '18.6 GiB',
    impact: '+18.6 GiB',
  },
  {
    type: '重命名',
    tagType: 'info' as const,
    path: 'labels/reviewed/night-v4.jsonl',
    previous: '1.4 GiB',
    current: '1.4 GiB',
    impact: '0 B',
  },
  {
    type: '删除',
    tagType: 'danger' as const,
    path: 'labels/drafts/night-v3.tmp',
    previous: '620 MiB',
    current: '—',
    impact: '-620 MiB',
  },
  {
    type: '新增',
    tagType: 'success' as const,
    path: 'images/night-rain/shard-023.tar',
    previous: '—',
    current: '8.3 GiB',
    impact: '+8.3 GiB',
  },
  {
    type: '修改',
    tagType: 'warning' as const,
    path: 'annotations/partition=night/date=2026-07-28/labels.parquet',
    previous: '2.3 GiB',
    current: '2.7 GiB',
    impact: '+420 MiB',
  },
];

const currentPreparation = computed(
  () => preparationSteps[preparationIndex.value] ?? preparationSteps[0]!,
);
const preCommitReady = computed(
  () =>
    currentPreparation.value.phase === 'ready' &&
    apiPreCommit.value?.state === 'ready' &&
    Boolean(apiPreCommit.value.candidate_index_version),
);
const activePreCommit = computed(() => getActivePreCommit(playgroundPreCommitKey.value));
const preCommitJobId = computed(
  () =>
    activePreCommit.value?.jobId ??
    (String(route.query.precommit_job_id ?? '') || `precommit-${playgroundId.value}-0729`),
);
const commitCreated = computed(() => Boolean(createdCommitId.value));

function schedulePreparation(): void {
  if (preparationIndex.value >= preparationSteps.length - 1) return;
  preparationTimer = globalThis.setTimeout(async () => {
    preparationIndex.value += 1;
    advancePrototypePreCommit(playgroundPreCommitKey.value, currentPreparation.value.phase);
    if (currentPreparation.value.phase === 'ready' && apiPreCommit.value) {
      const result = await queryPlaygroundPreCommit(
        tenantId.value,
        apiPreCommit.value.precommit_id,
      );
      apiPreCommit.value = result.data.precommit;
    }
    schedulePreparation();
  }, 420);
}

async function startPreparation(resumeFromRoute = false): Promise<void> {
  if (preparationTimer) globalThis.clearTimeout(preparationTimer);
  const current = playground.value;
  if (!current) return;
  if (current.active_precommit_id) {
    const existing = await queryPlaygroundPreCommit(tenantId.value, current.active_precommit_id);
    apiPreCommit.value = existing.data.precommit;
  } else {
    const started = await startPlaygroundPreCommit({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      playground_id: playgroundId.value,
      precommit_request_id: `precommit-request-${globalThis.crypto.randomUUID()}`,
      expected_index_version: current.index_version,
    });
    apiPreCommit.value = started.data.precommit;
  }
  const requestedPhase = resumeFromRoute ? String(route.query.resume_phase ?? '') : '';
  const requestedIndex = preparationSteps.findIndex((step) => step.phase === requestedPhase);
  preparationIndex.value = requestedIndex >= 0 ? requestedIndex : 0;
  if (resumeFromRoute && activePreCommit.value) {
    advancePrototypePreCommit(playgroundPreCommitKey.value, currentPreparation.value.phase);
  } else {
    startPrototypePreCommit(playgroundPreCommitKey.value, {
      phase: currentPreparation.value.phase,
      ...(resumeFromRoute && route.query.precommit_job_id
        ? { jobId: String(route.query.precommit_job_id) }
        : {}),
    });
  }
  preparationStarted.value = true;
  schedulePreparation();
}

function resetCommitForm(): void {
  commitMessage.value = '补充夜间道路场景数据';
  commitDescription.value = '补充低照度和雨夜场景，修正 17 条标注记录，并更新训练集索引。';
  tagInput.value = '';
  tagNames.value = [];
  commitError.value = '';
  commitDialogOpen.value = false;
  createdCommitId.value = '';
  createdParentCommitId.value = '';
}

async function restartPreCommit(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      '当前预检测结果会被丢弃，并创建一个新的 Pre-commit 任务。',
      '重新检测',
      {
        confirmButtonText: '重新检测',
        cancelButtonText: '保留当前结果',
        type: 'warning',
      },
    );
  } catch {
    return;
  }
  resetCommitForm();
  if (apiPreCommit.value && apiPreCommit.value.state !== 'committed') {
    await cancelPlaygroundPreCommit({
      tenant_id: tenantId.value,
      precommit_id: apiPreCommit.value.precommit_id,
      cancel_request_id: `cancel-request-${globalThis.crypto.randomUUID()}`,
    });
  }
  apiPreCommit.value = undefined;
  await playgroundQuery.refetch();
  await startPreparation(false);
  ElMessage.success('新的 Pre-commit 已发起');
}

async function cancelPreparation(): Promise<void> {
  try {
    await ElMessageBox.confirm(
      '取消后会停止预检测并丢弃 Candidate，不会影响 Playground 文件。',
      '取消 Pre-commit',
      {
        confirmButtonText: '确认取消',
        cancelButtonText: '继续检测',
        type: 'warning',
      },
    );
  } catch {
    return;
  }
  if (preparationTimer) globalThis.clearTimeout(preparationTimer);
  if (apiPreCommit.value && apiPreCommit.value.state !== 'committed') {
    await cancelPlaygroundPreCommit({
      tenant_id: tenantId.value,
      precommit_id: apiPreCommit.value.precommit_id,
      cancel_request_id: `cancel-request-${globalThis.crypto.randomUUID()}`,
    });
  }
  cancelPrototypePreCommit(playgroundPreCommitKey.value);
  ElMessage.success('Pre-commit 已取消，Playground 保持可用');
  await backToPlayground();
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
  commitError.value = '';
  return true;
}

function removeTag(tagName: string): void {
  tagNames.value = tagNames.value.filter((item) => item !== tagName);
}

function openCommitDialog(): void {
  if (!preCommitReady.value) return;
  commitError.value = '';
  commitDialogOpen.value = true;
}

async function createCommit(): Promise<void> {
  commitError.value = '';
  if (!commitMessage.value.trim()) {
    commitError.value = '请输入 Commit 标题';
    return;
  }
  if (!addTag()) return;
  const current = playground.value;
  const precommit = apiPreCommit.value;
  if (!current || !precommit?.candidate_index_version) return;
  createdParentCommitId.value = current.head_commit_id ?? '';
  try {
    const result = await commitMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      playground_id: playgroundId.value,
      commit_request_id: `commit-request-${globalThis.crypto.randomUUID()}`,
      precommit_id: precommit.precommit_id,
      expected_candidate_index_version: precommit.candidate_index_version,
      message: commitMessage.value.trim(),
      ...(commitDescription.value.trim() ? { description: commitDescription.value.trim() } : {}),
      ...(tagNames.value.length ? { tag_names: tagNames.value } : {}),
    });
    createdCommitId.value = result.data.commit.commit_id;
    commitDialogOpen.value = false;
    cancelPrototypePreCommit(playgroundPreCommitKey.value);
    ElMessage.success('Commit 已创建，数据资产的当前版本已更新');
  } catch (error) {
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
  await router.push({
    name: 'artifact-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
    },
    query: { tab: 'commits', commit_id: createdCommitId.value },
  });
}

async function createSnapshotDelivery(): Promise<void> {
  await router.push({
    name: 'snapshot-delivery-prototype',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
    },
    query: {
      commit_id: createdCommitId.value,
      commit_title: commitMessage.value,
      source_playground: playgroundId.value,
    },
  });
}

watch(
  playground,
  (current) => {
    if (current && !preparationStarted.value) void startPreparation(true);
  },
  { immediate: true },
);

onBeforeUnmount(() => {
  if (preparationTimer) globalThis.clearTimeout(preparationTimer);
});
</script>

<template>
  <div class="page commit-workbench">
    <PageHeading
      title="提交 Playground"
      :description="`${projectId} / ${artifactId} / ${playgroundId}`"
    >
      <template #actions>
        <el-button :icon="Back" @click="backToPlayground">返回 Playground</el-button>
        <el-button
          v-if="!commitCreated && activePreCommit"
          :icon="CircleClose"
          type="danger"
          plain
          @click="cancelPreparation"
        >
          取消 Pre-commit
        </el-button>
        <el-tooltip v-if="!commitCreated && activePreCommit" content="重新检测" placement="top">
          <el-button :icon="RefreshRight" aria-label="重新检测" @click="restartPreCommit" />
        </el-tooltip>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="playgroundQuery.error.value"
      :error="playgroundQuery.error.value"
      :retrying="playgroundQuery.isFetching.value"
      @retry="playgroundQuery.refetch"
    />

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
          <dd>{{ playground?.region ?? '—' }}</dd>
        </div>
        <div>
          <dt>StorageVolume</dt>
          <dd>
            <code>{{ playground?.storage_volume_id ?? '—' }}</code>
          </dd>
        </div>
        <div>
          <dt>IndexVersion</dt>
          <dd>revision {{ playground?.index_version.revision ?? '—' }}</dd>
        </div>
        <div>
          <dt>当前 Head</dt>
          <dd>
            <code>{{ playground?.head_commit_id ?? '尚无 Commit' }}</code>
          </dd>
        </div>
      </dl>
    </section>

    <template v-if="!commitCreated">
      <section class="preflight-status" :class="{ 'is-ready': preCommitReady }">
        <span class="preflight-status__icon">
          <CircleCheck v-if="preCommitReady" />
          <RefreshRight v-else class="is-spinning" />
        </span>
        <div class="preflight-status__body">
          <small>{{ preCommitJobId }}</small>
          <strong>{{ currentPreparation.label }}</strong>
          <p>{{ currentPreparation.detail }} · Playground 保持 Ready</p>
          <el-progress
            :percentage="currentPreparation.progress"
            :stroke-width="6"
            :status="preCommitReady ? 'success' : undefined"
            :show-text="false"
          />
        </div>
        <strong>{{ currentPreparation.progress }}%</strong>
        <el-button v-if="preCommitReady" type="primary" @click="openCommitDialog">
          填写 Commit 信息
        </el-button>
      </section>

      <section v-if="!preCommitReady" class="preflight-running">
        <div>
          <CircleCheck /><span
            ><strong>Storage</strong><small>Ready · {{ playground?.region ?? '—' }}</small></span
          >
        </div>
        <div>
          <CircleCheck /><span
            ><strong>Agent</strong><small>selected agent · reachable</small></span
          >
        </div>
        <div>
          <CircleCheck /><span><strong>Mount</strong><small>RW capable · mounted</small></span>
        </div>
        <div>
          <CircleCheck /><span
            ><strong>Index 基线</strong
            ><small>revision {{ playground?.index_version.revision ?? '—' }}</small></span
          >
        </div>
      </section>

      <template v-else>
        <section class="change-metrics" aria-label="变化摘要">
          <div><span>变化文件</span><strong>128</strong><small>83 新增 · 31 修改</small></div>
          <div><span>新增数据</span><strong>34.1 GiB</strong><small>332 个新对象</small></div>
          <div><span>移除数据</span><strong>620 MiB</strong><small>9 个删除路径</small></div>
          <div><span>对象复用率</span><strong>82%</strong><small>节省 126.4 GiB</small></div>
        </section>

        <div class="diff-layout">
          <main class="diff-panel">
            <header class="panel-heading">
              <div>
                <small>PRE-COMMIT DIFF</small>
                <h2>相对父版本的变化</h2>
                <p>
                  <code>{{ playground?.head_commit_id ?? 'root' }}</code> → Index revision
                  {{ playground?.index_version.revision ?? '—' }}
                </p>
              </div>
              <el-tag type="success" effect="plain"><CircleCheck /> 0 项阻断</el-tag>
            </header>

            <div class="diff-table desktop-diff" role="table" aria-label="Pre-commit 文件 Diff">
              <div class="diff-table__head" role="row">
                <span>变化</span><span>路径</span><span>父版本</span><span>当前</span
                ><span>影响</span>
              </div>
              <div v-for="row in diffRows" :key="row.path" class="diff-table__row" role="row">
                <el-tag :type="row.tagType" size="small" effect="plain">{{ row.type }}</el-tag>
                <code>{{ row.path }}</code>
                <span>{{ row.previous }}</span>
                <span>{{ row.current }}</span>
                <strong>{{ row.impact }}</strong>
              </div>
            </div>

            <div class="mobile-diff">
              <div v-for="row in diffRows" :key="row.path">
                <span
                  ><el-tag :type="row.tagType" size="small" effect="plain">{{ row.type }}</el-tag
                  ><strong>{{ row.impact }}</strong></span
                >
                <code>{{ row.path }}</code>
                <small>{{ row.previous }} → {{ row.current }}</small>
              </div>
            </div>
          </main>

          <aside class="diff-aside">
            <section>
              <header><h3>提交检查</h3></header>
              <ul class="validation-list">
                <li>
                  <CircleCheck /><span
                    ><strong>对象完整性</strong><small>1,842 个对象均可读取</small></span
                  >
                </li>
                <li>
                  <CircleCheck /><span
                    ><strong>Index 一致性</strong
                    ><small
                      >expected revision {{ playground?.index_version.revision ?? '—' }}</small
                    ></span
                  >
                </li>
                <li>
                  <CircleCheck /><span
                    ><strong>路径规范</strong><small>没有冲突或保留路径</small></span
                  >
                </li>
                <li>
                  <CircleCheck /><span
                    ><strong>写租约</strong><small>Commit 发布时短暂申请</small></span
                  >
                </li>
              </ul>
            </section>
            <section class="parent-summary">
              <header><h3>父 Commit</h3></header>
              <strong>补充道路标注质量结果</strong>
              <code>{{ playground?.head_commit_id ?? '根 Commit' }}</code>
              <dl>
                <div>
                  <dt>创建者</dt>
                  <dd>li.ming</dd>
                </div>
                <div>
                  <dt>时间</dt>
                  <dd>2026-07-28 16:42</dd>
                </div>
                <div>
                  <dt>文件</dt>
                  <dd>18,426</dd>
                </div>
              </dl>
            </section>
          </aside>
        </div>

        <footer class="workflow-actions">
          <span>Commit 将固化本次预检测生成的 Index Candidate。</span>
          <el-button :icon="RefreshRight" @click="restartPreCommit">重新检测</el-button>
          <el-button type="primary" :icon="ArrowRight" @click="openCommitDialog">
            填写 Commit 信息
          </el-button>
        </footer>
      </template>
    </template>

    <section v-else class="commit-result">
      <span class="commit-result__icon"><CircleCheck /></span>
      <div>
        <small>COMMIT CREATED</small>
        <h2>Commit 已创建</h2>
        <p>不可变版本已发布，数据资产的当前 Commit 已更新。</p>
      </div>
      <dl>
        <div>
          <dt>Commit ID</dt>
          <dd>
            <code>{{ createdCommitId }}</code>
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
            <el-tag v-for="tagName in tagNames" :key="tagName" effect="plain">
              {{ tagName }}
            </el-tag>
            <span v-if="tagNames.length === 0">—</span>
          </dd>
        </div>
        <div>
          <dt>IndexVersion</dt>
          <dd>revision {{ playground?.index_version.revision ?? '—' }}</dd>
        </div>
        <div>
          <dt>对象耐久性</dt>
          <dd>Central S3 · verified</dd>
        </div>
        <div>
          <dt>区域交付</dt>
          <dd>尚未创建 Snapshot</dd>
        </div>
      </dl>
      <div class="result-next">
        <div>
          <strong>让这个版本可被训练任务消费</strong>
          <p>创建固定到该 Commit 的只读 Snapshot，再选择目标区域。</p>
        </div>
        <el-button type="primary" :icon="ArrowRight" @click="createSnapshotDelivery">
          创建并交付 Snapshot
        </el-button>
      </div>
      <div class="result-actions">
        <el-button @click="backToPlayground">返回 Playground</el-button>
        <el-button @click="openVersionHistory">查看版本历史</el-button>
      </div>
    </section>

    <el-dialog
      v-model="commitDialogOpen"
      class="commit-dialog"
      title="创建 Commit"
      width="min(620px, calc(100vw - 32px))"
      :close-on-click-modal="false"
    >
      <ApiProblemAlert v-if="commitMutation.error.value" :error="commitMutation.error.value" />
      <el-alert v-if="commitError" :title="commitError" type="error" :closable="false" />
      <section class="dialog-context">
        <div>
          <span>Parent</span><code>{{ playground?.head_commit_id ?? '根 Commit' }}</code>
        </div>
        <div>
          <span>Index</span
          ><strong>revision {{ playground?.index_version.revision ?? '—' }}</strong>
        </div>
        <div><span>变化</span><strong>128 文件 · +33.5 GiB</strong></div>
      </section>
      <el-form label-position="top" class="commit-form">
        <el-form-item label="Commit 标题" required>
          <el-input
            v-model="commitMessage"
            maxlength="256"
            show-word-limit
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
            placeholder="记录变化背景、范围和验证结论"
          />
        </el-form-item>
        <el-form-item label="Tags">
          <div class="tag-editor">
            <div class="tag-editor__input">
              <el-input
                v-model="tagInput"
                aria-label="Commit Tags"
                placeholder="输入 Tag 后按 Enter"
                @keyup.enter.prevent="addTag"
              />
              <el-tooltip content="添加 Tag" placement="top">
                <el-button :icon="Plus" aria-label="添加 Commit Tag" @click="addTag" />
              </el-tooltip>
            </div>
            <div class="tag-editor__values">
              <el-tag
                v-for="tagName in tagNames"
                :key="tagName"
                closable
                @close="removeTag(tagName)"
                >{{ tagName }}</el-tag
              >
            </div>
          </div>
        </el-form-item>
      </el-form>
      <div class="cas-note">
        <WarningFilled />
        <p>发布前会重新校验 expected Head 和 IndexVersion；变化时返回冲突，不覆盖其他人的提交。</p>
      </div>
      <template #footer>
        <el-button @click="commitDialogOpen = false">取消</el-button>
        <el-button type="primary" :loading="commitMutation.isPending.value" @click="createCommit">
          确认 Commit
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.commit-workbench {
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
  display: flex;
  flex-direction: column;
  justify-content: center;
  padding: 13px 16px;
  border-right: 1px solid var(--line);
}

.commit-context dl > div:last-child {
  border-right: 0;
}

.commit-context dt {
  margin-bottom: 5px;
}

.commit-context dd {
  margin: 0;
  font-size: 11px;
  font-weight: 650;
  overflow-wrap: anywhere;
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

.preflight-status.is-ready {
  border-color: #aac5b9;
  border-left-color: var(--green);
  background: #edf6f1;
}

.preflight-status__icon {
  width: 36px;
  height: 36px;
  display: grid;
  place-items: center;
  color: #9d650f;
  background: #f4e3bd;
}

.is-ready .preflight-status__icon {
  color: var(--green);
  background: #d9ebe2;
}

.preflight-status__icon svg {
  width: 20px;
}

.preflight-status__body {
  min-width: 0;
}

.preflight-status__body small,
.preflight-status__body strong,
.preflight-status__body p {
  display: block;
  margin: 0;
}

.preflight-status__body small {
  color: var(--muted);
  font-size: 9px;
}

.preflight-status__body strong {
  margin-top: 3px;
  font-size: 13px;
}

.preflight-status__body p {
  margin-top: 3px;
  color: var(--muted);
  font-size: 10px;
}

.preflight-status__body :deep(.el-progress) {
  margin-top: 8px;
}

.preflight-status > strong {
  font-size: 18px;
}

.preflight-running {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin-top: 14px;
  border: 1px solid var(--line);
  background: #fff;
}

.preflight-running > div {
  display: grid;
  grid-template-columns: 18px minmax(0, 1fr);
  gap: 8px;
  padding: 14px;
  border-right: 1px solid var(--line);
}

.preflight-running > div:last-child {
  border-right: 0;
}

.preflight-running svg,
.validation-list svg {
  width: 16px;
  color: var(--green);
}

.preflight-running strong,
.preflight-running small {
  display: block;
}

.preflight-running strong {
  font-size: 10px;
}

.preflight-running small {
  margin-top: 3px;
  color: var(--muted);
  font-size: 9px;
}

.change-metrics {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
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
.change-metrics strong,
.change-metrics small {
  display: block;
}

.change-metrics span,
.change-metrics small {
  color: var(--muted);
  font-size: 10px;
}

.change-metrics strong {
  margin: 5px 0 3px;
  font-size: 16px;
}

.diff-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 300px;
  gap: 18px;
  margin-top: 18px;
}

.diff-panel,
.diff-aside > section {
  border: 1px solid var(--line);
  background: #fff;
}

.diff-panel {
  min-width: 0;
  padding: 22px;
}

.diff-aside {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.diff-aside > section {
  padding: 18px;
}

.panel-heading {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 18px;
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
  font-size: 17px;
}

.panel-heading p {
  margin-top: 5px;
  color: var(--muted);
  font-size: 10px;
}

.panel-heading :deep(.el-tag svg) {
  width: 13px;
  margin-right: 4px;
}

.diff-table {
  border-top: 1px solid var(--line);
}

.diff-table__head,
.diff-table__row {
  display: grid;
  grid-template-columns: 72px minmax(220px, 1fr) 80px 80px 80px;
  align-items: center;
  gap: 10px;
  padding: 11px 6px;
  border-bottom: 1px solid var(--line);
}

.diff-table__head {
  color: var(--muted);
  background: #f5f7f6;
  font-size: 10px;
}

.diff-table__row {
  font-size: 11px;
}

.diff-table__row code {
  font-size: 10px;
  overflow-wrap: anywhere;
}

.diff-table__row strong {
  text-align: right;
}

.mobile-diff {
  display: none;
}

.diff-aside header {
  margin-bottom: 12px;
}

.diff-aside h3 {
  margin: 0;
  font-size: 13px;
}

.validation-list {
  margin: 0;
  padding: 0;
  list-style: none;
}

.validation-list li {
  display: grid;
  grid-template-columns: 18px minmax(0, 1fr);
  gap: 9px;
  padding: 10px 0;
  border-bottom: 1px solid var(--line);
}

.validation-list li:last-child {
  border-bottom: 0;
}

.validation-list strong,
.validation-list small {
  display: block;
}

.validation-list strong {
  font-size: 11px;
}

.validation-list small {
  margin-top: 3px;
  color: var(--muted);
  font-size: 10px;
}

.parent-summary > strong,
.parent-summary > code {
  display: block;
}

.parent-summary > strong {
  font-size: 12px;
}

.parent-summary > code {
  margin-top: 5px;
  color: var(--muted);
  font-size: 10px;
}

.parent-summary dl {
  margin: 14px 0 0;
  border-top: 1px solid var(--line);
}

.parent-summary dl > div {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  padding: 9px 0;
  border-bottom: 1px solid var(--line);
  font-size: 10px;
}

.parent-summary dt {
  color: var(--muted);
}

.parent-summary dd {
  margin: 0;
  text-align: right;
}

.workflow-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 18px;
  padding: 15px 18px;
  border: 1px solid var(--line);
  background: #fff;
}

.workflow-actions > span {
  margin-right: auto;
  color: var(--muted);
  font-size: 11px;
}

.dialog-context {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  margin-bottom: 18px;
  border: 1px solid var(--line);
  background: #f5f7f6;
}

.dialog-context > div {
  min-width: 0;
  padding: 12px;
  border-right: 1px solid var(--line);
}

.dialog-context > div:last-child {
  border-right: 0;
}

.dialog-context span,
.dialog-context strong,
.dialog-context code {
  display: block;
}

.dialog-context span {
  margin-bottom: 5px;
  color: var(--muted);
  font-size: 9px;
}

.dialog-context strong,
.dialog-context code {
  font-size: 10px;
  overflow-wrap: anywhere;
}

.tag-editor,
.tag-editor__input {
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
  gap: 7px;
  margin-top: 9px;
}

.cas-note {
  display: grid;
  grid-template-columns: 20px minmax(0, 1fr);
  gap: 9px;
  padding: 12px;
  border-left: 3px solid #c5811a;
  background: #fff9ec;
}

.cas-note svg {
  width: 18px;
  color: #b57413;
}

.cas-note p {
  margin: 0;
  color: var(--muted);
  font-size: 10px;
  line-height: 1.5;
}

.commit-result {
  padding: 28px;
  border: 1px solid var(--line);
  background: #fff;
}

.commit-result__icon {
  width: 44px;
  height: 44px;
  display: grid;
  place-items: center;
  color: #fff;
  background: var(--green);
}

.commit-result > div:first-of-type {
  margin-top: 16px;
}

.commit-result small {
  color: var(--green);
  font-size: 9px;
  font-weight: 800;
}

.commit-result h2,
.commit-result p {
  margin: 0;
}

.commit-result h2 {
  margin-top: 5px;
  font-size: 21px;
}

.commit-result p {
  margin-top: 6px;
  color: var(--muted);
  font-size: 11px;
}

.commit-result > dl {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  margin: 22px 0 0;
  border-top: 1px solid var(--line);
  border-left: 1px solid var(--line);
}

.commit-result > dl > div {
  min-width: 0;
  padding: 14px;
  border-right: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.commit-result dt {
  color: var(--muted);
  font-size: 9px;
}

.commit-result dd {
  margin: 5px 0 0;
  font-size: 10px;
  font-weight: 650;
  overflow-wrap: anywhere;
}

.result-next {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  margin-top: 22px;
  padding-top: 18px;
  border-top: 1px solid var(--line);
}

.result-next strong {
  font-size: 12px;
}

.result-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 18px;
}

.is-spinning {
  animation: preflight-spin 1.1s linear infinite;
}

@keyframes preflight-spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 960px) {
  .commit-context,
  .diff-layout {
    grid-template-columns: 1fr;
  }

  .commit-context__identity {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .diff-aside {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 650px) {
  :global(.commit-dialog) {
    max-height: calc(100dvh - 24px);
    display: flex;
    flex-direction: column;
    margin: 12px auto !important;
  }

  :global(.commit-dialog .el-dialog__body) {
    min-height: 0;
    overflow-y: auto;
  }

  :global(.commit-dialog .el-dialog__footer) {
    flex: 0 0 auto;
  }

  .commit-context dl,
  .change-metrics,
  .preflight-running {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .commit-context dl > div:nth-child(2),
  .change-metrics > div:nth-child(2),
  .preflight-running > div:nth-child(2) {
    border-right: 0;
  }

  .commit-context dl > div:nth-child(-n + 2),
  .change-metrics > div:nth-child(-n + 2),
  .preflight-running > div:nth-child(-n + 2) {
    border-bottom: 1px solid var(--line);
  }

  .preflight-status {
    grid-template-columns: 32px minmax(0, 1fr) auto;
    align-items: start;
    padding: 14px;
  }

  .preflight-status__icon {
    width: 30px;
    height: 30px;
  }

  .preflight-status .el-button {
    grid-column: 2 / -1;
    justify-self: start;
  }

  .diff-panel,
  .diff-aside > section,
  .commit-result {
    padding: 16px;
  }

  .diff-aside {
    grid-template-columns: 1fr;
  }

  .desktop-diff {
    display: none;
  }

  .mobile-diff {
    display: block;
    border-top: 1px solid var(--line);
  }

  .mobile-diff > div {
    padding: 12px 2px;
    border-bottom: 1px solid var(--line);
  }

  .mobile-diff span {
    display: flex;
    align-items: center;
    justify-content: space-between;
  }

  .mobile-diff code,
  .mobile-diff small {
    display: block;
    margin-top: 7px;
    font-size: 10px;
    overflow-wrap: anywhere;
  }

  .mobile-diff small {
    color: var(--muted);
  }

  .workflow-actions,
  .result-next,
  .result-actions {
    flex-wrap: wrap;
  }

  .workflow-actions > span,
  .result-next > div,
  .result-next .el-button {
    width: 100%;
  }

  .dialog-context,
  .commit-result > dl {
    grid-template-columns: 1fr;
  }

  .dialog-context > div {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .dialog-context > div:last-child {
    border-bottom: 0;
  }
}
</style>
