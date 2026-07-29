<script setup lang="ts">
import { Back, Box, RefreshRight } from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { querySnapshot } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { commitTagNames } from '@/utils/commit';
import { formatBytes, formatCount, formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const commitId = computed(() => String(route.params.commitId ?? ''));
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
const snapshot = computed(() => snapshotQuery.data.value?.data.snapshot);
const snapshotTags = computed(() => commitTagNames(snapshot.value?.ref_names ?? []));
</script>

<template>
  <div class="page">
    <PageHeading :title="snapshot?.message ?? commitId" :description="`Snapshot · ${commitId}`">
      <template #actions>
        <el-button
          :icon="Back"
          @click="router.push({ name: 'snapshot-list', params: { tenantId } })"
        >
          返回列表
        </el-button>
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
    <template v-if="snapshot">
      <section class="resource-summary">
        <div>
          <span>文件数</span><strong>{{ formatCount(snapshot.logical_file_count) }}</strong>
        </div>
        <div>
          <span>逻辑大小</span><strong>{{ formatBytes(snapshot.logical_size_bytes) }}</strong>
        </div>
        <div>
          <span>Region</span><strong>{{ snapshot.region }}</strong>
        </div>
        <div>
          <span>创建时间</span><strong>{{ formatTime(snapshot.created_at_unix_ms) }}</strong>
        </div>
      </section>
      <section class="content-section">
        <div class="section-heading section-heading--inline">
          <div>
            <h2>复合身份</h2>
            <p>Snapshot 没有独立 snapshot_id</p>
          </div>
          <el-button
            text
            type="primary"
            :icon="Box"
            @click="
              router.push({
                name: 'artifact-detail',
                params: { tenantId, projectId, artifactId },
                query: { tab: 'commits' },
              })
            "
            >查看 Artifact Commit</el-button
          >
        </div>
        <dl class="definition-grid definition-grid--scope">
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
          <div>
            <dt>StorageVolume</dt>
            <dd>
              <code>{{ snapshot.storage_volume_id }}</code>
            </dd>
          </div>
          <div class="definition-grid__wide">
            <dt>Tags</dt>
            <dd class="tag-list">
              <el-tag v-for="tagName in snapshotTags" :key="tagName" effect="plain">
                {{ tagName }}
              </el-tag>
              <span v-if="snapshotTags.length === 0">暂无 Tag</span>
            </dd>
          </div>
        </dl>
      </section>
    </template>
    <div v-else-if="snapshotQuery.isPending.value" class="page-loading">
      <el-skeleton :rows="8" animated />
    </div>
  </div>
</template>
