<script setup lang="ts">
import { ArrowRight, Plus, Search } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { createPlayground, queryPlaygroundList } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import StorageVolumeFilter from '@/components/StorageVolumeFilter.vue';
import {
  playgroundAvailabilityLabel,
  playgroundAvailabilityTagType,
} from '@/features/precommit/status';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const canCreatePlayground = computed(
  () => tenants.byId(tenantId.value)?.permissions.includes('playground.create') ?? false,
);
const projectId = ref(String(route.query.project_id ?? ''));
const artifactId = ref(String(route.query.artifact_id ?? ''));
const searchInput = ref(String(route.query.q ?? ''));
const search = ref(searchInput.value);
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);
const createOpen = ref(false);
const createError = ref('');
const createForm = reactive({
  projectId: '',
  artifactId: '',
  playgroundId: '',
  displayName: '',
  storageVolumeId: '',
});
const createMutation = useMutation({ mutationFn: createPlayground });

const playgroundQuery = useQuery({
  queryKey: computed(() => [
    'playgrounds',
    tenantId.value,
    projectId.value,
    artifactId.value,
    search.value,
    cursor.value ?? '',
  ]),
  queryFn: () =>
    queryPlaygroundList({
      tenant_id: tenantId.value,
      page_size: 50,
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(artifactId.value ? { artifact_id: artifactId.value } : {}),
      ...(search.value ? { query: search.value } : {}),
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
});

watch(projectId, (value, previous) => {
  if (value !== previous && artifactId.value && String(route.query.project_id ?? '') !== value) {
    artifactId.value = '';
  }
  cursor.value = undefined;
  cursorHistory.value = [];
});

watch(
  [tenantId, () => route.query],
  ([, query]) => {
    projectId.value = String(query.project_id ?? '');
    artifactId.value = String(query.artifact_id ?? '');
    searchInput.value = String(query.q ?? '');
    search.value = searchInput.value;
    cursor.value = undefined;
    cursorHistory.value = [];
    createOpen.value = false;
    createError.value = '';
    createMutation.reset();
  },
  { deep: true },
);

watch(artifactId, () => {
  cursor.value = undefined;
  cursorHistory.value = [];
});

async function applyFilters(): Promise<void> {
  cursor.value = undefined;
  cursorHistory.value = [];
  await router.replace({
    query: {
      ...(projectId.value ? { project_id: projectId.value } : {}),
      ...(artifactId.value ? { artifact_id: artifactId.value } : {}),
      ...(searchInput.value.trim() ? { q: searchInput.value.trim() } : {}),
    },
  });
  search.value = searchInput.value.trim();
}

function nextPage(): void {
  const next = playgroundQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  cursor.value = cursorHistory.value.pop() || undefined;
}

function openCreate(): void {
  Object.assign(createForm, {
    projectId: projectId.value,
    artifactId: artifactId.value,
    playgroundId: '',
    displayName: '',
    storageVolumeId: '',
  });
  createError.value = '';
  createMutation.reset();
  createOpen.value = true;
}

async function submitCreate(): Promise<void> {
  createError.value = '';
  const resourceId = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
  if (
    !resourceId.test(createForm.projectId) ||
    !resourceId.test(createForm.artifactId) ||
    !resourceId.test(createForm.playgroundId) ||
    !createForm.displayName.trim() ||
    !resourceId.test(createForm.storageVolumeId)
  ) {
    createError.value = '请填写合法的 Project、Artifact、Playground、名称和 StorageVolume';
    return;
  }

  let result;
  try {
    result = await createMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: createForm.projectId,
      artifact_id: createForm.artifactId,
      playground_id: createForm.playgroundId,
      display_name: createForm.displayName.trim(),
      storage_volume_id: createForm.storageVolumeId,
    });
  } catch {
    return;
  }

  await queryClient.invalidateQueries({ queryKey: ['playgrounds', tenantId.value] });
  createOpen.value = false;
  ElMessage.success(result.data.replayed ? '已返回现有 Playground' : 'Playground 已创建');
  await openPlayground(
    result.data.playground.project_id,
    result.data.playground.artifact_id,
    result.data.playground.playground_id,
  );
}

async function openPlayground(
  project: string,
  artifact: string,
  playground: string,
): Promise<void> {
  await router.push({
    name: 'playground-detail',
    params: {
      tenantId: tenantId.value,
      projectId: project,
      artifactId: artifact,
      playgroundId: playground,
    },
  });
}
</script>

<template>
  <div class="page">
    <PageHeading title="工作区" :description="`${tenantId} 内可以产生数据变化的 Playground`">
      <template v-if="canCreatePlayground" #actions>
        <el-button type="primary" :icon="Plus" @click="openCreate">创建 Playground</el-button>
      </template>
    </PageHeading>
    <form class="resource-toolbar resource-toolbar--wide" @submit.prevent="applyFilters">
      <el-input
        v-model="projectId"
        aria-label="Project 筛选"
        clearable
        placeholder="全部 Project"
      />
      <el-input
        v-model="artifactId"
        aria-label="Artifact 筛选"
        clearable
        placeholder="全部 Artifact"
      />
      <el-input v-model="searchInput" clearable placeholder="搜索 Playground" />
      <el-button type="primary" native-type="submit" :icon="Search">查询</el-button>
    </form>

    <ApiProblemAlert
      v-if="playgroundQuery.error.value"
      :error="playgroundQuery.error.value"
      :retrying="playgroundQuery.isFetching.value"
      @retry="playgroundQuery.refetch"
    />
    <section class="content-section resource-section">
      <el-skeleton v-if="playgroundQuery.isPending.value" :rows="7" animated />
      <el-empty
        v-else-if="!playgroundQuery.data.value?.data.items.length"
        description="当前筛选下没有 Playground"
        :image-size="78"
      />
      <template v-else>
        <el-table
          :data="playgroundQuery.data.value?.data.items"
          class="resource-table desktop-table"
        >
          <el-table-column label="Playground" min-width="230">
            <template #default="scope">
              <button
                class="resource-link"
                type="button"
                @click="
                  openPlayground(
                    scope.row.project_id,
                    scope.row.artifact_id,
                    scope.row.playground_id,
                  )
                "
              >
                <strong>{{ scope.row.display_name }}</strong
                ><code>{{ scope.row.playground_id }}</code>
              </button>
            </template>
          </el-table-column>
          <el-table-column prop="project_id" label="Project" min-width="150" />
          <el-table-column prop="artifact_id" label="Artifact" min-width="160" />
          <el-table-column label="放置" min-width="190">
            <template #default="scope">
              <div class="table-placement">
                <strong>{{ scope.row.region }}</strong>
                <code>{{ scope.row.storage_volume_id }}</code>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="可用性 / 当前操作" min-width="190">
            <template #default="scope">
              <div class="state-stack">
                <el-tag :type="playgroundAvailabilityTagType(scope.row.state)" effect="plain">
                  {{ playgroundAvailabilityLabel(scope.row.state) }}
                </el-tag>
                <el-tag v-if="scope.row.active_precommit_id" type="warning" effect="plain">
                  存在活动 Pre-commit
                </el-tag>
                <span v-else>空闲</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="更新时间" min-width="160">
            <template #default="scope">{{ formatTime(scope.row.updated_at_unix_ms) }}</template>
          </el-table-column>
          <el-table-column width="54" align="right">
            <template #default="scope">
              <el-button
                text
                :icon="ArrowRight"
                title="查看 Playground"
                @click="
                  openPlayground(
                    scope.row.project_id,
                    scope.row.artifact_id,
                    scope.row.playground_id,
                  )
                "
              />
            </template>
          </el-table-column>
        </el-table>
        <div class="mobile-resource-list">
          <button
            v-for="playground in playgroundQuery.data.value?.data.items"
            :key="`${playground.project_id}/${playground.artifact_id}/${playground.playground_id}`"
            class="mobile-resource-item"
            type="button"
            @click="
              openPlayground(
                playground.project_id,
                playground.artifact_id,
                playground.playground_id,
              )
            "
          >
            <span
              ><strong>{{ playground.display_name }}</strong
              ><code>{{ playground.playground_id }}</code></span
            >
            <span
              ><small>{{ playground.region }}</small
              ><el-tag
                :type="playgroundAvailabilityTagType(playground.state)"
                size="small"
                effect="plain"
                >{{ playgroundAvailabilityLabel(playground.state) }}</el-tag
              ><el-tag
                v-if="playground.active_precommit_id"
                type="warning"
                size="small"
                effect="plain"
                >活动 Pre-commit</el-tag
              ><ArrowRight
            /></span>
          </button>
        </div>
        <PageCursor
          :has-previous="cursorHistory.length > 0"
          :has-next="Boolean(playgroundQuery.data.value?.data.next_cursor)"
          :loading="playgroundQuery.isFetching.value"
          @previous="previousPage"
          @next="nextPage"
        />
      </template>
    </section>

    <el-dialog v-model="createOpen" title="创建 Playground" width="min(580px, calc(100vw - 32px))">
      <ApiProblemAlert v-if="createMutation.error.value" :error="createMutation.error.value" />
      <el-alert v-if="createError" :title="createError" type="error" :closable="false" />
      <el-form label-position="top" class="dialog-form">
        <div class="dialog-form-grid">
          <el-form-item label="Project ID" required>
            <el-input v-model="createForm.projectId" placeholder="project-lab" />
          </el-form-item>
          <el-form-item label="Artifact ID" required>
            <el-input v-model="createForm.artifactId" placeholder="dataset-a" />
          </el-form-item>
          <el-form-item label="Playground ID" required>
            <el-input v-model="createForm.playgroundId" placeholder="review-august" />
          </el-form-item>
          <el-form-item label="名称" required>
            <el-input v-model="createForm.displayName" placeholder="八月复核" />
          </el-form-item>
        </div>
        <el-form-item label="StorageVolume" required>
          <StorageVolumeFilter v-model="createForm.storageVolumeId" :tenant-id="tenantId" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createOpen = false">取消</el-button>
        <el-button type="primary" :loading="createMutation.isPending.value" @click="submitCreate">
          创建 Playground
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.state-stack {
  display: flex;
  align-items: center;
  gap: 6px;
}

.state-stack > span {
  color: var(--muted);
  font-size: 10px;
}
</style>
