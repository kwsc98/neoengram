<script setup lang="ts">
import { ArrowRight, Search } from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { queryArtifactList } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import ProjectFilter from '@/components/ProjectFilter.vue';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = ref(String(route.query.project_id ?? ''));
const searchInput = ref(String(route.query.q ?? ''));
const search = ref(searchInput.value);
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);

watch(
  () => route.query,
  (query) => {
    projectId.value = String(query.project_id ?? '');
    searchInput.value = String(query.q ?? '');
    search.value = searchInput.value;
    cursor.value = undefined;
    cursorHistory.value = [];
  },
);

const artifactsQuery = useQuery({
  queryKey: computed(() => [
    'artifacts',
    tenantId.value,
    projectId.value,
    search.value,
    cursor.value ?? '',
  ]),
  queryFn: () =>
    queryArtifactList({
      tenant_id: tenantId.value,
      page_size: 50,
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(search.value ? { query: search.value } : {}),
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
});

async function applyFilters(): Promise<void> {
  cursor.value = undefined;
  cursorHistory.value = [];
  await router.replace({
    query: {
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(searchInput.value.trim() ? { q: searchInput.value.trim() } : {}),
    },
  });
}

function nextPage(): void {
  const next = artifactsQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  const previous = cursorHistory.value.pop();
  cursor.value = previous || undefined;
}

async function openArtifact(project: string, artifact: string): Promise<void> {
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: project, artifactId: artifact },
  });
}
</script>

<template>
  <div class="page">
    <PageHeading title="Artifacts" :description="`${tenantId} 内的受管 Artifact`" />

    <form class="resource-toolbar" @submit.prevent="applyFilters">
      <ProjectFilter v-model="projectId" :tenant-id="tenantId" />
      <el-input v-model="searchInput" clearable placeholder="搜索名称或 Artifact ID" />
      <el-button type="primary" native-type="submit" :icon="Search">查询</el-button>
    </form>

    <ApiProblemAlert
      v-if="artifactsQuery.error.value"
      :error="artifactsQuery.error.value"
      :retrying="artifactsQuery.isFetching.value"
      @retry="artifactsQuery.refetch"
    />

    <section class="content-section resource-section">
      <el-skeleton v-if="artifactsQuery.isPending.value" :rows="7" animated />
      <el-empty
        v-else-if="!artifactsQuery.data.value?.data.items.length"
        description="当前筛选下没有 Artifact"
        :image-size="78"
      />
      <template v-else>
        <el-table
          :data="artifactsQuery.data.value?.data.items"
          class="resource-table desktop-table"
        >
          <el-table-column label="Artifact" min-width="250">
            <template #default="scope">
              <button
                class="resource-link"
                type="button"
                @click="openArtifact(scope.row.project_id, scope.row.artifact_id)"
              >
                <strong>{{ scope.row.display_name }}</strong>
                <code>{{ scope.row.artifact_id }}</code>
              </button>
            </template>
          </el-table-column>
          <el-table-column prop="project_id" label="Project" min-width="170" />
          <el-table-column prop="default_ref" label="Default ref" min-width="180" />
          <el-table-column label="更新时间" min-width="160">
            <template #default="scope">{{ formatTime(scope.row.updated_at_unix_ms) }}</template>
          </el-table-column>
          <el-table-column width="54" align="right">
            <template #default="scope">
              <el-button
                text
                :icon="ArrowRight"
                title="查看 Artifact"
                @click="openArtifact(scope.row.project_id, scope.row.artifact_id)"
              />
            </template>
          </el-table-column>
        </el-table>
        <div class="mobile-resource-list">
          <button
            v-for="artifact in artifactsQuery.data.value?.data.items"
            :key="`${artifact.project_id}/${artifact.artifact_id}`"
            type="button"
            class="mobile-resource-item"
            @click="openArtifact(artifact.project_id, artifact.artifact_id)"
          >
            <span
              ><strong>{{ artifact.display_name }}</strong
              ><code>{{ artifact.artifact_id }}</code></span
            >
            <span
              ><small>{{ artifact.project_id }}</small
              ><ArrowRight
            /></span>
          </button>
        </div>
        <PageCursor
          :has-previous="cursorHistory.length > 0"
          :has-next="Boolean(artifactsQuery.data.value?.data.next_cursor)"
          :loading="artifactsQuery.isFetching.value"
          @previous="previousPage"
          @next="nextPage"
        />
      </template>
    </section>
  </div>
</template>
