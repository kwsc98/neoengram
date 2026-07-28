<script setup lang="ts">
import { Back, Box, RefreshRight } from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { queryPlayground } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
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
});
const playground = computed(() => playgroundQuery.data.value?.data.playground);
</script>

<template>
  <div class="page">
    <PageHeading
      :title="playground?.display_name ?? playgroundId"
      :description="`${projectId} / ${artifactId} / ${playgroundId}`"
    >
      <template #actions>
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
      <section class="resource-summary">
        <div>
          <span>状态</span><el-tag effect="plain">{{ playground.state }}</el-tag>
        </div>
        <div>
          <span>Index revision</span><strong>{{ playground.index_version.revision }}</strong>
        </div>
        <div>
          <span>更新时间</span><strong>{{ formatTime(playground.updated_at_unix_ms) }}</strong>
        </div>
      </section>
      <section class="content-section">
        <div class="section-heading section-heading--inline">
          <div>
            <h2>资源信息</h2>
            <p>公开 PlaygroundView</p>
          </div>
          <el-button
            text
            type="primary"
            :icon="Box"
            @click="
              router.push({ name: 'artifact-detail', params: { tenantId, projectId, artifactId } })
            "
            >查看 Artifact</el-button
          >
        </div>
        <dl class="definition-grid definition-grid--scope">
          <div>
            <dt>Tenant</dt>
            <dd>{{ playground.tenant_id }}</dd>
          </div>
          <div>
            <dt>Project</dt>
            <dd>{{ playground.project_id }}</dd>
          </div>
          <div>
            <dt>Artifact</dt>
            <dd>{{ playground.artifact_id }}</dd>
          </div>
          <div>
            <dt>Playground</dt>
            <dd>{{ playground.playground_id }}</dd>
          </div>
          <div>
            <dt>Base commit</dt>
            <dd>
              <code>{{ playground.base_commit_id ?? '—' }}</code>
            </dd>
          </div>
          <div>
            <dt>Head commit</dt>
            <dd>
              <code>{{ playground.head_commit_id ?? '—' }}</code>
            </dd>
          </div>
          <div>
            <dt>Index digest</dt>
            <dd>
              <code>{{ playground.index_version.digest }}</code>
            </dd>
          </div>
          <div>
            <dt>创建时间</dt>
            <dd>{{ formatTime(playground.created_at_unix_ms) }}</dd>
          </div>
        </dl>
      </section>
    </template>
    <div v-else-if="playgroundQuery.isPending.value" class="page-loading">
      <el-skeleton :rows="8" animated />
    </div>
  </div>
</template>
