<script setup lang="ts">
import { ArrowRight, Search } from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { queryArtifactList, querySnapshotList } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import ProjectFilter from '@/components/ProjectFilter.vue';
import { commitTagNames } from '@/utils/commit';
import { formatBytes, formatCount, formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = ref(String(route.query.project_id ?? ''));
const artifactId = ref(String(route.query.artifact_id ?? ''));
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);

const artifactOptionsQuery = useQuery({
  queryKey: computed(() => ['artifacts', tenantId.value, projectId.value, 'snapshot-filter']),
  queryFn: () =>
    queryArtifactList({ tenant_id: tenantId.value, project_id: projectId.value, page_size: 100 }),
  enabled: computed(() => Boolean(projectId.value)),
});
const snapshotQuery = useQuery({
  queryKey: computed(() => [
    'snapshots',
    tenantId.value,
    projectId.value,
    artifactId.value,
    cursor.value ?? '',
  ]),
  queryFn: () =>
    querySnapshotList({
      tenant_id: tenantId.value,
      page_size: 50,
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(artifactId.value ? { artifact_id: artifactId.value } : {}),
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
});

watch(projectId, (value, previous) => {
  if (value !== previous && artifactId.value) artifactId.value = '';
});

async function applyFilters(): Promise<void> {
  cursor.value = undefined;
  cursorHistory.value = [];
  await router.replace({
    query: {
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(artifactId.value ? { artifact_id: artifactId.value } : {}),
    },
  });
}

function nextPage(): void {
  const next = snapshotQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  cursor.value = cursorHistory.value.pop() || undefined;
}

async function openSnapshot(project: string, artifact: string, commit: string): Promise<void> {
  await router.push({
    name: 'snapshot-detail',
    params: {
      tenantId: tenantId.value,
      projectId: project,
      artifactId: artifact,
      commitId: commit,
    },
  });
}
</script>

<template>
  <div class="page">
    <PageHeading
      title="快照与交付"
      :description="`${tenantId} 内固定版本的只读视图及区域可用状态`"
    />
    <form class="resource-toolbar" @submit.prevent="applyFilters">
      <ProjectFilter v-model="projectId" :tenant-id="tenantId" />
      <el-select
        v-model="artifactId"
        aria-label="Artifact 筛选"
        clearable
        filterable
        :disabled="!projectId"
        placeholder="全部 Artifact"
      >
        <el-option
          v-for="artifact in artifactOptionsQuery.data.value?.data.items ?? []"
          :key="artifact.artifact_id"
          :label="artifact.display_name"
          :value="artifact.artifact_id"
        />
      </el-select>
      <el-button type="primary" native-type="submit" :icon="Search">查询</el-button>
    </form>

    <ApiProblemAlert
      v-if="snapshotQuery.error.value"
      :error="snapshotQuery.error.value"
      :retrying="snapshotQuery.isFetching.value"
      @retry="snapshotQuery.refetch"
    />
    <section class="content-section resource-section">
      <el-skeleton v-if="snapshotQuery.isPending.value" :rows="7" animated />
      <el-empty
        v-else-if="!snapshotQuery.data.value?.data.items.length"
        description="当前筛选下没有 Snapshot"
        :image-size="78"
      />
      <template v-else>
        <el-table :data="snapshotQuery.data.value?.data.items" class="resource-table desktop-table">
          <el-table-column label="Commit" min-width="260">
            <template #default="scope">
              <button
                class="resource-link"
                type="button"
                @click="
                  openSnapshot(scope.row.project_id, scope.row.artifact_id, scope.row.commit_id)
                "
              >
                <strong>{{ scope.row.message }}</strong
                ><code>{{ scope.row.commit_id }}</code>
              </button>
            </template>
          </el-table-column>
          <el-table-column prop="artifact_id" label="Artifact" min-width="160" />
          <el-table-column label="Tags" min-width="170">
            <template #default="scope">
              <div class="tag-list">
                <el-tag
                  v-for="tagName in commitTagNames(scope.row.ref_names)"
                  :key="tagName"
                  size="small"
                  effect="plain"
                >
                  {{ tagName }}
                </el-tag>
                <span v-if="commitTagNames(scope.row.ref_names).length === 0">—</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="放置" min-width="190">
            <template #default="scope">
              <div class="table-placement">
                <strong>{{ scope.row.region }}</strong>
                <code>{{ scope.row.storage_volume_id }}</code>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="文件" width="110">
            <template #default="scope">{{ formatCount(scope.row.logical_file_count) }}</template>
          </el-table-column>
          <el-table-column label="逻辑大小" width="130">
            <template #default="scope">{{ formatBytes(scope.row.logical_size_bytes) }}</template>
          </el-table-column>
          <el-table-column label="创建时间" min-width="160">
            <template #default="scope">{{ formatTime(scope.row.created_at_unix_ms) }}</template>
          </el-table-column>
          <el-table-column width="54" align="right">
            <template #default="scope">
              <el-button
                text
                :icon="ArrowRight"
                title="查看 Snapshot"
                @click="
                  openSnapshot(scope.row.project_id, scope.row.artifact_id, scope.row.commit_id)
                "
              />
            </template>
          </el-table-column>
        </el-table>
        <div class="mobile-resource-list">
          <button
            v-for="snapshot in snapshotQuery.data.value?.data.items"
            :key="`${snapshot.project_id}/${snapshot.artifact_id}/${snapshot.commit_id}`"
            class="mobile-resource-item"
            type="button"
            @click="openSnapshot(snapshot.project_id, snapshot.artifact_id, snapshot.commit_id)"
          >
            <span
              ><strong>{{ snapshot.message }}</strong
              ><code>{{ snapshot.commit_id }}</code
              ><small v-if="commitTagNames(snapshot.ref_names).length" class="mobile-resource-tags">
                Tags: {{ commitTagNames(snapshot.ref_names).join(', ') }}
              </small></span
            >
            <span
              ><small>{{ snapshot.region }} · {{ formatBytes(snapshot.logical_size_bytes) }}</small
              ><ArrowRight
            /></span>
          </button>
        </div>
        <PageCursor
          :has-previous="cursorHistory.length > 0"
          :has-next="Boolean(snapshotQuery.data.value?.data.next_cursor)"
          :loading="snapshotQuery.isFetching.value"
          @previous="previousPage"
          @next="nextPage"
        />
      </template>
    </section>
  </div>
</template>
