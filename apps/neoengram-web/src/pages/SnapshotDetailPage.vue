<script setup lang="ts">
import { Back, CircleCheck, Lock, RefreshRight, WarningFilled } from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { querySnapshot } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import {
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

const snapshotQuery = useQuery({
  queryKey: computed(() => ['snapshot', tenantId.value, snapshotId.value]),
  queryFn: async () => {
    const result = await querySnapshot(tenantId.value, snapshotId.value);
    const snapshot = result.data.snapshot;
    if (snapshot.project_id !== projectId.value || snapshot.artifact_id !== artifactId.value) {
      throw new Error('Snapshot 不属于当前 Artifact');
    }
    return result;
  },
  refetchInterval: (query) => snapshotPollInterval(query.state.data?.data.snapshot.state),
});
const snapshot = computed(() => snapshotQuery.data.value?.data.snapshot);
const tags = computed(() => commitTagNames(snapshot.value?.tag_names ?? []));

async function backToArtifact(): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: projectId.value, artifactId: artifactId.value },
    query: { tab: 'snapshots' },
  });
}
</script>

<template>
  <div class="page snapshot-detail-page">
    <PageHeading
      :title="snapshot?.message ?? snapshotId"
      :description="`${projectId} / ${artifactId}`"
    >
      <template #actions>
        <el-button :icon="Back" @click="backToArtifact">返回 Artifact</el-button>
        <el-button
          :icon="RefreshRight"
          :loading="snapshotQuery.isFetching.value"
          @click="snapshotQuery.refetch"
          >刷新</el-button
        >
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="snapshotQuery.error.value"
      :error="snapshotQuery.error.value"
      :retrying="snapshotQuery.isFetching.value"
      @retry="snapshotQuery.refetch"
    />
    <el-skeleton v-if="snapshotQuery.isPending.value" :rows="8" animated />

    <template v-else-if="snapshot">
      <section class="snapshot-state-band">
        <span :class="['snapshot-state-icon', `snapshot-state-icon--${snapshot.state}`]">
          <CircleCheck v-if="snapshot.state === 'ready'" />
          <WarningFilled v-else-if="snapshot.state === 'abnormal'" />
          <RefreshRight v-else />
        </span>
        <div>
          <small>{{ snapshotStateLabel(snapshot.state) }}</small>
          <h2>{{ snapshotPhaseLabel(snapshot.phase) }}</h2>
          <p v-if="snapshot.state === 'creating'">目标 Volume 正在建立只读 FUSE 视图。</p>
          <p v-else-if="snapshot.state === 'ready'">固定 Commit 已通过只读 FUSE 视图交付。</p>
          <p v-else>只读视图未能完成交付，请检查 Volume 状态。</p>
        </div>
        <el-tag :type="snapshotStateTagType(snapshot.state)" effect="plain">
          {{ snapshotStateLabel(snapshot.state) }}
        </el-tag>
      </section>

      <el-alert
        v-if="snapshot.issue"
        :title="snapshot.issue.message"
        :description="snapshot.issue.code"
        type="error"
        :closable="false"
      />

      <section class="content-section snapshot-detail-section">
        <header class="section-heading">
          <div>
            <span>READ-ONLY PLACEMENT</span>
            <h2>FUSE 挂载</h2>
          </div>
          <Lock />
        </header>
        <dl class="snapshot-facts">
          <div>
            <dt>Snapshot ID</dt>
            <dd>
              <code>{{ snapshot.snapshot_id }}</code>
            </dd>
          </div>
          <div>
            <dt>StorageVolume</dt>
            <dd>
              <code>{{ snapshot.storage_volume_id }}</code>
            </dd>
          </div>
          <div>
            <dt>Region</dt>
            <dd>{{ snapshot.region }}</dd>
          </div>
          <div>
            <dt>访问模式</dt>
            <dd><el-tag type="success" effect="plain">只读</el-tag></dd>
          </div>
          <div class="snapshot-facts__wide">
            <dt>Artifact Commit</dt>
            <dd>
              <code>{{ snapshot.commit_id }}</code>
            </dd>
          </div>
          <div>
            <dt>完整性</dt>
            <dd>
              <el-tag :type="snapshotIntegrityTagType(snapshot.integrity.state)" effect="plain">{{
                snapshotIntegrityLabel(snapshot.integrity.state)
              }}</el-tag>
            </dd>
          </div>
          <div>
            <dt>文件</dt>
            <dd>{{ formatCount(snapshot.logical_file_count) }}</dd>
          </div>
          <div>
            <dt>逻辑大小</dt>
            <dd>{{ formatBytes(snapshot.logical_size_bytes) }}</dd>
          </div>
          <div>
            <dt>已校验</dt>
            <dd>{{ formatBytes(snapshot.integrity.bytes_verified) }}</dd>
          </div>
          <div>
            <dt>创建时间</dt>
            <dd>{{ formatTime(snapshot.created_at_unix_ms) }}</dd>
          </div>
          <div>
            <dt>更新时间</dt>
            <dd>{{ formatTime(snapshot.updated_at_unix_ms) }}</dd>
          </div>
          <div class="snapshot-facts__wide">
            <dt>Commit Tags</dt>
            <dd class="tag-list">
              <el-tag v-for="tag in tags" :key="tag" size="small" effect="plain">{{ tag }}</el-tag>
              <span v-if="tags.length === 0">暂无 Tag</span>
            </dd>
          </div>
        </dl>
      </section>
    </template>
  </div>
</template>

<style scoped>
.snapshot-detail-page {
  max-width: 1120px;
}

.snapshot-state-band {
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  padding: 18px;
  border: 1px solid var(--border);
  background: #fff;
}

.snapshot-state-band h2 {
  margin: 2px 0 4px;
  font-size: 22px;
}
.snapshot-state-band p {
  margin: 0;
  color: var(--muted);
}
.snapshot-state-icon {
  display: grid;
  width: 54px;
  height: 54px;
  place-items: center;
  background: #eef3f1;
  color: #7a8581;
}
.snapshot-state-icon svg {
  width: 28px;
}
.snapshot-state-icon--ready {
  background: #eaf6f0;
  color: #167450;
}
.snapshot-state-icon--abnormal {
  background: #fff0ef;
  color: #c33f35;
}
.snapshot-state-icon--creating svg {
  animation: spin 1.2s linear infinite;
}

.snapshot-detail-section {
  margin-top: 16px;
}
.section-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.section-heading span {
  color: var(--muted);
  font-size: 11px;
}
.section-heading h2 {
  margin: 3px 0 0;
  font-size: 18px;
}
.section-heading svg {
  width: 24px;
  color: #167450;
}

.snapshot-facts {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1px;
  padding: 1px;
  background: var(--border);
}
.snapshot-facts > div {
  min-width: 0;
  padding: 14px;
  background: #fff;
}
.snapshot-facts__wide {
  grid-column: 1 / -1;
}
.snapshot-facts dt {
  margin-bottom: 6px;
  color: var(--muted);
  font-size: 11px;
}
.snapshot-facts dd {
  min-width: 0;
  margin: 0;
  overflow-wrap: anywhere;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 640px) {
  .snapshot-state-band {
    grid-template-columns: 48px minmax(0, 1fr);
  }
  .snapshot-state-band > .el-tag {
    grid-column: 1 / -1;
    justify-self: start;
  }
  .snapshot-state-icon {
    width: 44px;
    height: 44px;
  }
  .snapshot-facts {
    grid-template-columns: 1fr;
  }
  .snapshot-facts__wide {
    grid-column: auto;
  }
}
</style>
