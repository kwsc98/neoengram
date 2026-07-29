<script setup lang="ts">
import {
  ArrowRight,
  Back,
  CircleCheck,
  Clock,
  DocumentCopy,
  Location,
  RefreshRight,
  WarningFilled,
} from '@element-plus/icons-vue';
import { ElCheckbox, ElMessage } from 'element-plus';
import { computed, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import PageHeading from '@/components/PageHeading.vue';

type Stage = 'settings' | 'placement' | 'activity';
type DeliveryState = 'draft' | 'materializing' | 'ready';

interface RegionPlacement {
  id: string;
  region: string;
  state: string;
  volume: string;
  cluster: string;
  agent: string;
  transfer: string;
  eta: string;
  disabled?: boolean;
}

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const commitId = computed(() => String(route.query.commit_id ?? 'commit-road-scenes-v4-preview'));
const commitTitle = computed(() => String(route.query.commit_title ?? '补充夜间道路场景数据'));
const sourcePlayground = computed(() => String(route.query.source_playground ?? 'labeling'));
const activeStage = ref<Stage>('settings');
const deliveryState = ref<DeliveryState>('draft');
const purpose = ref('夜间道路模型训练基线');
const retention = ref('180d');
const profile = ref('training-dataset-v2');
const selectedPlacementIds = ref(['shanghai', 'guangzhou']);
const showInfrastructure = ref(false);

const placements: RegionPlacement[] = [
  {
    id: 'shanghai',
    region: 'cn-shanghai',
    state: '本区域已有可复用对象副本',
    volume: 'volume-shanghai-vision',
    cluster: 'cluster-cn-east-1',
    agent: 'agent-sh-07 · ready',
    transfer: '本地对象复用 · 6.2 GiB',
    eta: '< 1 min',
  },
  {
    id: 'guangzhou',
    region: 'cn-guangzhou',
    state: '目标区域已配置交付存储',
    volume: 'volume-guangzhou-training',
    cluster: 'cluster-cn-south-1',
    agent: 'agent-gz-03 · ready',
    transfer: '跨区域物化 · 28.4 GiB',
    eta: '约 9 min',
  },
  {
    id: 'beijing',
    region: 'cn-beijing',
    state: '目标区域当前不可调度',
    volume: 'volume-beijing-archive',
    cluster: 'cluster-cn-north-2',
    agent: '无可用 Agent',
    transfer: '等待基础设施恢复',
    eta: '不可用',
    disabled: true,
  },
];

const stages: Array<{ key: Stage; index: number; label: string; detail: string }> = [
  { key: 'settings', index: 1, label: '固定版本', detail: '用途与保留策略' },
  { key: 'placement', index: 2, label: '区域交付', detail: '选择计算区域' },
  { key: 'activity', index: 3, label: '可用状态', detail: '物化与完整性校验' },
];

const activeStageIndex = computed(
  () => stages.find((stage) => stage.key === activeStage.value)?.index ?? 1,
);
const selectedPlacements = computed(() =>
  placements.filter((placement) => selectedPlacementIds.value.includes(placement.id)),
);

function goToStage(stage: Stage, index: number): void {
  if (index >= activeStageIndex.value || activeStage.value === 'activity') return;
  activeStage.value = stage;
}

function togglePlacement(placementId: string, selected: boolean | string | number): void {
  if (selected) {
    if (!selectedPlacementIds.value.includes(placementId)) {
      selectedPlacementIds.value.push(placementId);
    }
    return;
  }
  selectedPlacementIds.value = selectedPlacementIds.value.filter((item) => item !== placementId);
}

function continueToPlacement(): void {
  if (!purpose.value.trim()) {
    ElMessage.warning('请输入 Snapshot 用途');
    return;
  }
  activeStage.value = 'placement';
}

function startDelivery(): void {
  if (selectedPlacementIds.value.length === 0) {
    ElMessage.warning('请选择至少一个目标区域');
    return;
  }
  deliveryState.value = 'materializing';
  activeStage.value = 'activity';
  ElMessage.success('Snapshot 已创建，区域物化任务开始执行');
}

function advancePrototype(): void {
  deliveryState.value = 'ready';
  ElMessage.success('所有目标区域均已 Ready');
}

function resetPrototype(): void {
  activeStage.value = 'settings';
  deliveryState.value = 'draft';
  purpose.value = '夜间道路模型训练基线';
  retention.value = '180d';
  profile.value = 'training-dataset-v2';
  selectedPlacementIds.value = ['shanghai', 'guangzhou'];
  showInfrastructure.value = false;
  ElMessage.info('Snapshot 交付原型已重置');
}

function placementStatus(placementId: string): string {
  if (deliveryState.value === 'ready') return 'ready';
  return placementId === 'shanghai' ? 'ready' : 'materializing · 64%';
}

async function backToVersionHistory(): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
    },
    query: { tab: 'commits' },
  });
}

async function openSnapshotList(): Promise<void> {
  await router.push({
    name: 'snapshot-list',
    params: { tenantId: tenantId.value },
    query: { project_id: projectId.value, artifact_id: artifactId.value },
  });
}
</script>

<template>
  <div class="page snapshot-delivery">
    <PageHeading title="交付只读 Snapshot" :description="`${projectId} / ${artifactId}`">
      <template #actions>
        <el-tag type="warning" effect="plain">产品原型</el-tag>
        <el-button :icon="Back" @click="backToVersionHistory">返回版本历史</el-button>
        <el-button :icon="RefreshRight" aria-label="重置 Snapshot 原型" @click="resetPrototype" />
      </template>
    </PageHeading>

    <ol class="delivery-steps" aria-label="Snapshot 交付流程">
      <li
        v-for="stage in stages"
        :key="stage.key"
        :class="{
          'is-active': activeStage === stage.key,
          'is-complete': activeStageIndex > stage.index,
        }"
      >
        <button type="button" @click="goToStage(stage.key, stage.index)">
          <span>{{ activeStageIndex > stage.index ? '✓' : stage.index }}</span>
          <strong>{{ stage.label }}</strong>
          <small>{{ stage.detail }}</small>
        </button>
      </li>
    </ol>

    <section class="version-source">
      <span class="version-source__icon"><DocumentCopy /></span>
      <div>
        <small>FIXED COMMIT</small>
        <strong>{{ commitTitle }}</strong>
        <code>{{ commitId }}</code>
      </div>
      <dl>
        <div>
          <dt>Artifact</dt>
          <dd>{{ artifactId }}</dd>
        </div>
        <div>
          <dt>来源 Playground</dt>
          <dd>{{ sourcePlayground }}</dd>
        </div>
        <div>
          <dt>内容身份</dt>
          <dd>artifact + commit</dd>
        </div>
        <div>
          <dt>访问模式</dt>
          <dd>只读</dd>
        </div>
      </dl>
    </section>

    <template v-if="activeStage === 'settings'">
      <div class="delivery-layout">
        <main class="delivery-main">
          <section class="delivery-section">
            <header class="section-title">
              <div>
                <span>SNAPSHOT</span>
                <h2>定义数据消费上下文</h2>
              </div>
              <el-tag type="success" effect="plain">Commit 已验证</el-tag>
            </header>

            <el-form label-position="top" class="snapshot-form">
              <el-form-item label="用途" required>
                <el-input
                  v-model="purpose"
                  maxlength="256"
                  show-word-limit
                  placeholder="例如：夜间道路模型训练基线"
                />
              </el-form-item>
              <div class="form-grid">
                <el-form-item label="保留策略">
                  <el-select v-model="retention">
                    <el-option label="30 天" value="30d" />
                    <el-option label="180 天" value="180d" />
                    <el-option label="长期保留" value="pinned" />
                  </el-select>
                </el-form-item>
                <el-form-item label="Dataset Profile">
                  <el-select v-model="profile">
                    <el-option label="training-dataset-v2" value="training-dataset-v2" />
                    <el-option label="不声明 Profile" value="none" />
                  </el-select>
                </el-form-item>
              </div>
            </el-form>

            <div class="profile-validation">
              <CircleCheck />
              <div>
                <strong>Dataset Profile 校验通过</strong>
                <p>schema、source 摘要和 shard 配置均可用于可复现训练读取。</p>
              </div>
              <el-tag type="success" effect="plain">Ready</el-tag>
            </div>
          </section>
        </main>

        <aside class="delivery-aside">
          <section>
            <header><h3>版本摘要</h3></header>
            <dl class="summary-list">
              <div>
                <dt>文件</dt>
                <dd>18,554</dd>
              </div>
              <div>
                <dt>逻辑大小</dt>
                <dd>846.2 GiB</dd>
              </div>
              <div>
                <dt>新增对象</dt>
                <dd>28.4 GiB</dd>
              </div>
              <div>
                <dt>内容复用</dt>
                <dd>82%</dd>
              </div>
              <div>
                <dt>Profile</dt>
                <dd>Ready</dd>
              </div>
            </dl>
          </section>
          <section class="identity-note">
            <WarningFilled />
            <div>
              <strong>Snapshot 始终固定到这个 Commit</strong>
              <p>后续产生新 Commit 也不会改变本次训练、恢复和审计内容。</p>
            </div>
          </section>
        </aside>
      </div>

      <footer class="delivery-actions">
        <span>下一步只选择计算区域，底层 Volume 由 Snapshot 交付策略决定。</span>
        <el-button type="primary" :icon="ArrowRight" @click="continueToPlacement"
          >选择交付区域</el-button
        >
      </footer>
    </template>

    <template v-else-if="activeStage === 'placement'">
      <section class="delivery-section placement-section">
        <header class="section-title">
          <div>
            <span>PLACEMENT</span>
            <h2>选择需要就绪的计算区域</h2>
          </div>
          <el-switch
            v-model="showInfrastructure"
            inline-prompt
            active-text="详细"
            inactive-text="简洁"
          />
        </header>

        <div class="placement-list">
          <article
            v-for="placement in placements"
            :key="placement.id"
            :class="{
              'is-disabled': placement.disabled,
              'is-selected': selectedPlacementIds.includes(placement.id),
            }"
          >
            <el-checkbox
              :model-value="selectedPlacementIds.includes(placement.id)"
              :disabled="Boolean(placement.disabled)"
              :aria-label="`选择 ${placement.region} 区域`"
              @change="(selected) => togglePlacement(placement.id, selected)"
            />
            <span class="placement-icon"><Location /></span>
            <div class="placement-copy">
              <span
                ><strong>{{ placement.region }}</strong
                ><el-tag v-if="placement.id === 'shanghai'" size="small" effect="plain"
                  >源区域</el-tag
                ></span
              >
              <p>{{ placement.state }}</p>
              <dl v-if="showInfrastructure">
                <div>
                  <dt>EdgeCluster</dt>
                  <dd>
                    <code>{{ placement.cluster }}</code>
                  </dd>
                </div>
                <div>
                  <dt>StorageVolume</dt>
                  <dd>
                    <code>{{ placement.volume }}</code>
                  </dd>
                </div>
                <div>
                  <dt>调度 Agent</dt>
                  <dd>{{ placement.agent }}</dd>
                </div>
              </dl>
            </div>
            <div class="placement-estimate">
              <strong>{{ placement.transfer }}</strong>
              <small><Clock /> {{ placement.eta }}</small>
            </div>
          </article>
        </div>

        <div class="placement-summary">
          <span
            >已选择 <strong>{{ selectedPlacementIds.length }}</strong> 个区域</span
          >
          <span>预计跨区域传输 <strong>28.4 GiB</strong></span>
          <span>源对象完整性 <strong>verified</strong></span>
        </div>
      </section>

      <footer class="delivery-actions">
        <el-button @click="activeStage = 'settings'">返回 Snapshot 设置</el-button>
        <el-button type="primary" :icon="ArrowRight" @click="startDelivery"
          >创建 Snapshot 并开始交付</el-button
        >
      </footer>
    </template>

    <template v-else>
      <section class="delivery-activity">
        <header>
          <span class="activity-icon" :class="{ 'is-ready': deliveryState === 'ready' }">
            <CircleCheck v-if="deliveryState === 'ready'" />
            <RefreshRight v-else />
          </span>
          <div>
            <small>{{ deliveryState === 'ready' ? 'READY' : 'MATERIALIZING' }}</small>
            <h2>
              {{ deliveryState === 'ready' ? 'Snapshot 已在目标区域就绪' : '正在交付 Snapshot' }}
            </h2>
            <p>
              {{
                deliveryState === 'ready'
                  ? '2 个区域已通过完整性校验，可以创建读取 Lease。'
                  : '上海已就绪，广州正在下载并验证缺失对象。'
              }}
            </p>
          </div>
          <el-button v-if="deliveryState !== 'ready'" @click="advancePrototype">推进模拟</el-button>
        </header>

        <div class="activity-table">
          <div v-for="placement in selectedPlacements" :key="placement.id">
            <span class="activity-region"
              ><Location /><strong>{{ placement.region }}</strong></span
            >
            <code>{{ placement.volume }}</code>
            <span>{{ placement.transfer }}</span>
            <el-tag
              :type="placementStatus(placement.id) === 'ready' ? 'success' : 'warning'"
              effect="plain"
            >
              {{ placementStatus(placement.id) }}
            </el-tag>
          </div>
        </div>

        <section class="read-contract">
          <div>
            <dt>Snapshot</dt>
            <dd>
              <code>{{ artifactId }}@{{ commitId }}</code>
            </dd>
          </div>
          <div>
            <dt>访问</dt>
            <dd>Read-only · Lease required</dd>
          </div>
          <div>
            <dt>Retention</dt>
            <dd>{{ retention }}</dd>
          </div>
          <div>
            <dt>Dataset Profile</dt>
            <dd>{{ profile === 'none' ? '未声明' : 'Ready' }}</dd>
          </div>
        </section>

        <footer>
          <el-button @click="backToVersionHistory">查看 Artifact</el-button>
          <el-button type="primary" @click="openSnapshotList">打开快照与交付</el-button>
        </footer>
      </section>
    </template>
  </div>
</template>

<style scoped>
.snapshot-delivery {
  width: min(1240px, 100%);
}

.delivery-steps {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  margin: 0 0 18px;
  padding: 0;
  border: 1px solid var(--line);
  background: #fff;
  list-style: none;
}

.delivery-steps li {
  border-right: 1px solid var(--line);
}

.delivery-steps li:last-child {
  border-right: 0;
}

.delivery-steps button {
  width: 100%;
  min-height: 78px;
  display: grid;
  grid-template-columns: 30px minmax(0, 1fr);
  grid-template-rows: auto auto;
  align-content: center;
  gap: 2px 10px;
  border: 0;
  padding: 13px 18px;
  background: transparent;
  cursor: default;
  text-align: left;
}

.delivery-steps li.is-complete button {
  cursor: pointer;
}

.delivery-steps button > span {
  grid-row: 1 / 3;
  align-self: center;
  width: 28px;
  height: 28px;
  display: grid;
  place-items: center;
  border: 1px solid #bdc7c2;
  border-radius: 50%;
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}

.delivery-steps strong {
  font-size: 13px;
}

.delivery-steps small {
  color: var(--muted);
  font-size: 11px;
}

.delivery-steps li.is-active {
  box-shadow: 0 -3px 0 var(--green) inset;
}

.delivery-steps li.is-active button > span,
.delivery-steps li.is-complete button > span {
  border-color: var(--green);
  color: #fff;
  background: var(--green);
}

.version-source {
  display: grid;
  grid-template-columns: 38px minmax(250px, 0.9fr) minmax(520px, 1.7fr);
  align-items: center;
  gap: 12px;
  border: 1px solid var(--line);
  padding-left: 18px;
  background: #eef2f0;
}

.version-source__icon {
  width: 36px;
  height: 36px;
  display: grid;
  place-items: center;
  border-radius: 5px;
  color: var(--green);
  background: #dcebe4;
}

.version-source > div {
  min-width: 0;
  padding: 15px 0;
}

.version-source small,
.version-source strong,
.version-source code {
  display: block;
}

.version-source small,
.version-source code,
.version-source dt {
  color: var(--muted);
  font-size: 10px;
}

.version-source strong {
  margin: 4px 0;
  font-size: 13px;
}

.version-source dl {
  align-self: stretch;
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin: 0;
  border-left: 1px solid var(--line);
}

.version-source dl > div {
  min-width: 0;
  display: flex;
  flex-direction: column;
  justify-content: center;
  padding: 13px 15px;
  border-right: 1px solid var(--line);
}

.version-source dl > div:last-child {
  border-right: 0;
}

.version-source dt {
  margin-bottom: 5px;
}

.version-source dd {
  margin: 0;
  font-size: 11px;
  font-weight: 650;
  overflow-wrap: anywhere;
}

.delivery-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 300px;
  gap: 18px;
  margin-top: 18px;
}

.delivery-section,
.delivery-aside > section,
.delivery-activity {
  border: 1px solid var(--line);
  background: #fff;
}

.delivery-section {
  padding: 22px;
}

.delivery-aside {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.delivery-aside > section {
  padding: 18px;
}

.section-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 20px;
}

.section-title span,
.section-title h2,
.delivery-aside h3 {
  margin: 0;
}

.section-title > div > span {
  color: var(--green);
  font-size: 10px;
  font-weight: 800;
}

.section-title h2 {
  margin-top: 5px;
  font-size: 17px;
}

.snapshot-form .el-select {
  width: 100%;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
}

.profile-validation {
  display: grid;
  grid-template-columns: 23px minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  margin-top: 6px;
  padding: 14px;
  border-left: 3px solid var(--green);
  background: #edf6f1;
}

.profile-validation > svg {
  width: 20px;
  color: var(--green);
}

.profile-validation strong,
.profile-validation p {
  margin: 0;
}

.profile-validation strong {
  font-size: 12px;
}

.profile-validation p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 10px;
}

.delivery-aside header {
  margin-bottom: 13px;
}

.delivery-aside h3 {
  font-size: 13px;
}

.summary-list {
  margin: 0;
  border-top: 1px solid var(--line);
}

.summary-list > div {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  padding: 10px 0;
  border-bottom: 1px solid var(--line);
  font-size: 10px;
}

.summary-list dt {
  color: var(--muted);
}

.summary-list dd {
  margin: 0;
  font-weight: 650;
}

.identity-note {
  display: grid;
  grid-template-columns: 22px minmax(0, 1fr);
  gap: 10px;
  border-left: 3px solid #c5811a !important;
  background: #fff9ec !important;
}

.identity-note > svg {
  width: 19px;
  color: #b57413;
}

.identity-note strong,
.identity-note p {
  margin: 0;
}

.identity-note strong {
  font-size: 11px;
}

.identity-note p {
  margin-top: 5px;
  color: var(--muted);
  font-size: 10px;
  line-height: 1.5;
}

.delivery-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 12px;
  margin-top: 18px;
  padding: 15px 18px;
  border: 1px solid var(--line);
  background: #fff;
}

.delivery-actions > span {
  margin-right: auto;
  color: var(--muted);
  font-size: 11px;
}

.placement-section {
  margin-top: 18px;
}

.placement-list {
  border-top: 1px solid var(--line);
}

.placement-list article {
  min-width: 0;
  display: grid;
  grid-template-columns: 24px 36px minmax(0, 1fr) 190px;
  align-items: center;
  gap: 13px;
  padding: 17px 8px;
  border-bottom: 1px solid var(--line);
}

.placement-list article.is-selected {
  background: #f4f9f6;
}

.placement-list article.is-disabled {
  opacity: 0.55;
}

.placement-icon {
  width: 34px;
  height: 34px;
  display: grid;
  place-items: center;
  border-radius: 5px;
  color: var(--green);
  background: #e2efe9;
}

.placement-copy > span {
  display: flex;
  align-items: center;
  gap: 8px;
}

.placement-copy strong {
  font-size: 13px;
}

.placement-copy p {
  margin: 4px 0 0;
  color: var(--muted);
  font-size: 10px;
}

.placement-copy dl {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
  margin: 10px 0 0;
}

.placement-copy dl > div {
  padding: 8px;
  background: #edf1ef;
}

.placement-copy dt {
  color: var(--muted);
  font-size: 9px;
}

.placement-copy dd {
  margin: 4px 0 0;
  font-size: 9px;
  overflow-wrap: anywhere;
}

.placement-estimate {
  text-align: right;
}

.placement-estimate strong,
.placement-estimate small {
  display: block;
}

.placement-estimate strong {
  font-size: 11px;
}

.placement-estimate small {
  margin-top: 5px;
  color: var(--muted);
  font-size: 10px;
}

.placement-estimate svg {
  width: 11px;
  vertical-align: -2px;
}

.placement-summary {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 18px;
  margin-top: 14px;
  padding: 12px 14px;
  background: #edf1ef;
  color: var(--muted);
  font-size: 10px;
}

.placement-summary span:first-child {
  margin-right: auto;
}

.placement-summary strong {
  color: var(--ink);
}

.delivery-activity {
  margin-top: 18px;
  padding: 26px;
}

.delivery-activity > header {
  display: grid;
  grid-template-columns: 44px minmax(0, 1fr) auto;
  align-items: center;
  gap: 14px;
}

.activity-icon {
  width: 42px;
  height: 42px;
  display: grid;
  place-items: center;
  border-radius: 50%;
  color: #fff;
  background: #c28627;
  font-size: 21px;
}

.activity-icon.is-ready {
  background: var(--green);
}

.activity-icon:not(.is-ready) svg {
  animation: rotate 1.4s linear infinite;
}

@keyframes rotate {
  to {
    transform: rotate(360deg);
  }
}

.delivery-activity header small {
  color: var(--green);
  font-size: 10px;
  font-weight: 800;
}

.delivery-activity header h2,
.delivery-activity header p {
  margin: 0;
}

.delivery-activity header h2 {
  margin-top: 4px;
  font-size: 19px;
}

.delivery-activity header p {
  margin-top: 5px;
  color: var(--muted);
  font-size: 11px;
}

.activity-table {
  margin-top: 24px;
  border-top: 1px solid var(--line);
}

.activity-table > div {
  min-width: 0;
  display: grid;
  grid-template-columns: 160px minmax(180px, 1fr) minmax(180px, 1fr) 150px;
  align-items: center;
  gap: 12px;
  padding: 15px 4px;
  border-bottom: 1px solid var(--line);
  font-size: 10px;
}

.activity-region {
  display: flex;
  align-items: center;
  gap: 8px;
}

.activity-region svg {
  width: 15px;
  color: var(--green);
}

.read-contract {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin-top: 18px;
  border: 1px solid var(--line);
}

.read-contract > div {
  min-width: 0;
  padding: 14px;
  border-right: 1px solid var(--line);
}

.read-contract > div:last-child {
  border-right: 0;
}

.read-contract dt {
  color: var(--muted);
  font-size: 9px;
}

.read-contract dd {
  margin: 5px 0 0;
  font-size: 10px;
  font-weight: 650;
  overflow-wrap: anywhere;
}

.delivery-activity > footer {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 18px;
}

@media (max-width: 960px) {
  .version-source {
    grid-template-columns: 38px minmax(0, 1fr);
  }

  .version-source dl {
    grid-column: 1 / -1;
    border-top: 1px solid var(--line);
    border-left: 0;
  }

  .delivery-layout {
    grid-template-columns: 1fr;
  }

  .delivery-aside {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 650px) {
  .delivery-steps button {
    min-height: 64px;
    grid-template-columns: 24px minmax(0, 1fr);
    padding: 10px;
  }

  .delivery-steps button > span {
    width: 23px;
    height: 23px;
  }

  .delivery-steps small {
    display: none;
  }

  .version-source dl,
  .form-grid,
  .read-contract {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .version-source dl > div:nth-child(2),
  .read-contract > div:nth-child(2) {
    border-right: 0;
  }

  .version-source dl > div:nth-child(-n + 2),
  .read-contract > div:nth-child(-n + 2) {
    border-bottom: 1px solid var(--line);
  }

  .delivery-section,
  .delivery-aside > section,
  .delivery-activity {
    padding: 16px;
  }

  .delivery-aside {
    grid-template-columns: 1fr;
  }

  .delivery-actions {
    flex-wrap: wrap;
  }

  .delivery-actions > span {
    width: 100%;
  }

  .placement-list article {
    grid-template-columns: 24px 32px minmax(0, 1fr);
  }

  .placement-estimate {
    grid-column: 2 / -1;
    text-align: left;
  }

  .placement-copy dl {
    grid-template-columns: 1fr;
  }

  .placement-summary {
    flex-wrap: wrap;
    justify-content: flex-start;
  }

  .placement-summary span:first-child {
    width: 100%;
  }

  .delivery-activity > header {
    grid-template-columns: 42px minmax(0, 1fr);
  }

  .delivery-activity > header .el-button {
    grid-column: 1 / -1;
    justify-self: start;
  }

  .activity-table > div {
    grid-template-columns: 1fr auto;
  }

  .activity-table > div > code,
  .activity-table > div > span:nth-child(3) {
    grid-column: 1 / -1;
  }

  .delivery-activity > footer {
    flex-wrap: wrap;
  }
}
</style>
