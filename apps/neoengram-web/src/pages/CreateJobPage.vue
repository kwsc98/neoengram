<script setup lang="ts">
import { Back, CirclePlus, RefreshLeft } from '@element-plus/icons-vue';
import { useMutation, useQuery } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { createAddJob, queryPlayground } from '@/api/operations';
import type { CreateAddJobRequest, PlaygroundView } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import PlaygroundSelect from '@/components/PlaygroundSelect.vue';
import { createJobFormSchema, parsePathLines } from '@/features/jobs/create-form';
import { useRecentJobsStore } from '@/stores/recent-jobs';

const route = useRoute();
const router = useRouter();
const recentJobs = useRecentJobsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const sourceScope = computed(() => ({
  project_id: String(route.query.project_id ?? ''),
  artifact_id: String(route.query.artifact_id ?? ''),
  playground_id: String(route.query.playground_id ?? ''),
}));
const hasSourceScope = computed(() =>
  Object.values(sourceScope.value).every((value) => Boolean(value)),
);
const selectedPlayground = ref<PlaygroundView>();
const form = reactive({
  jobId: `job-${globalThis.crypto.randomUUID()}`,
  deadline: new Date(Date.now() + 60 * 60 * 1000),
  all: true,
  pathsText: '',
});
const errors = reactive<Record<string, string>>({});

const mutation = useMutation({ mutationFn: createAddJob });
const sourcePlaygroundQuery = useQuery({
  queryKey: computed(() => [
    'playground',
    tenantId.value,
    sourceScope.value.project_id,
    sourceScope.value.artifact_id,
    sourceScope.value.playground_id,
    'job-create-source',
  ]),
  enabled: hasSourceScope,
  queryFn: () =>
    queryPlayground(
      tenantId.value,
      sourceScope.value.project_id,
      sourceScope.value.artifact_id,
      sourceScope.value.playground_id,
    ),
});
const selectedScope = computed(() => {
  const selected = selectedPlayground.value;
  if (!selected || selected.tenant_id !== tenantId.value) return undefined;
  return {
    project_id: selected.project_id,
    artifact_id: selected.artifact_id,
    playground_id: selected.playground_id,
  };
});
const playgroundQuery = useQuery({
  queryKey: computed(() => [
    'playground',
    tenantId.value,
    selectedScope.value?.project_id ?? '',
    selectedScope.value?.artifact_id ?? '',
    selectedScope.value?.playground_id ?? '',
    'job-create',
  ]),
  enabled: computed(() => Boolean(selectedScope.value)),
  queryFn: () =>
    queryPlayground(
      tenantId.value,
      selectedScope.value!.project_id,
      selectedScope.value!.artifact_id,
      selectedScope.value!.playground_id,
    ),
  refetchInterval: 5_000,
});
const playground = computed(() => {
  const candidate = playgroundQuery.data.value?.data.playground;
  const scope = selectedScope.value;
  if (
    !candidate ||
    !scope ||
    candidate.tenant_id !== tenantId.value ||
    candidate.project_id !== scope.project_id ||
    candidate.artifact_id !== scope.artifact_id ||
    candidate.playground_id !== scope.playground_id
  ) {
    return undefined;
  }
  return candidate;
});
const playgroundIndexVersionKey = computed(() => {
  const indexVersion = playground.value?.index_version;
  return indexVersion ? `${indexVersion.revision}:${indexVersion.digest}` : '';
});
let observedIndexVersionKey: string | undefined;

function clearErrors(): void {
  for (const key of Object.keys(errors)) delete errors[key];
}

function resetJobId(): void {
  form.jobId = `job-${globalThis.crypto.randomUUID()}`;
  mutation.reset();
}

watch(
  () => [
    tenantId.value,
    selectedScope.value?.project_id ?? '',
    selectedScope.value?.artifact_id ?? '',
    selectedScope.value?.playground_id ?? '',
    form.deadline instanceof Date ? form.deadline.getTime() : '',
    form.all,
    form.pathsText,
  ],
  () => {
    resetJobId();
    clearErrors();
  },
);

watch(
  () => sourcePlaygroundQuery.data.value?.data.playground,
  (candidate) => {
    const source = sourceScope.value;
    if (
      !candidate ||
      selectedPlayground.value ||
      candidate.tenant_id !== tenantId.value ||
      candidate.project_id !== source.project_id ||
      candidate.artifact_id !== source.artifact_id ||
      candidate.playground_id !== source.playground_id
    ) {
      return;
    }
    selectedPlayground.value = candidate;
  },
  { immediate: true },
);

watch(
  () => [tenantId.value, sourceScope.value] as const,
  () => {
    if (route.name !== 'job-create') return;
    selectedPlayground.value = undefined;
  },
  { deep: true },
);

watch(playgroundIndexVersionKey, (value) => {
  if (!value) return;
  if (observedIndexVersionKey === undefined) {
    observedIndexVersionKey = value;
    return;
  }
  if (value === observedIndexVersionKey) return;
  observedIndexVersionKey = value;
  resetJobId();
  clearErrors();
});

watch(
  () => form.jobId,
  () => mutation.reset(),
);

async function backToPlayground(): Promise<void> {
  const currentPlayground = playground.value;
  if (!currentPlayground) return;
  await router.push({
    name: 'playground-detail',
    params: {
      tenantId: tenantId.value,
      projectId: currentPlayground.project_id,
      artifactId: currentPlayground.artifact_id,
      playgroundId: currentPlayground.playground_id,
    },
  });
}

async function submit(): Promise<void> {
  if (mutation.isPending.value) return;
  clearErrors();
  const currentPlayground = playground.value;
  if (!currentPlayground) {
    errors.playground = playgroundQuery.isFetching.value
      ? '正在读取 Playground 权威状态'
      : '请选择可查询的 Playground';
    return;
  }
  const paths = parsePathLines(form.pathsText);
  const parsed = createJobFormSchema.safeParse({
    tenantId: tenantId.value,
    projectId: currentPlayground.project_id,
    artifactId: currentPlayground.artifact_id,
    playgroundId: currentPlayground.playground_id,
    ...form,
    revision: currentPlayground.index_version.revision,
    digest: currentPlayground.index_version.digest,
    paths,
  });
  if (!parsed.success) {
    for (const issue of parsed.error.issues)
      errors[String(issue.path[0] ?? 'form')] ??= issue.message;
    return;
  }

  const request: CreateAddJobRequest = {
    tenant_id: tenantId.value,
    project_id: currentPlayground.project_id,
    artifact_id: currentPlayground.artifact_id,
    playground_id: currentPlayground.playground_id,
    job_id: parsed.data.jobId,
    expected_index_version: currentPlayground.index_version,
    deadline_unix_ms: String(parsed.data.deadline.getTime()),
    paths: parsed.data.all ? [] : parsed.data.paths,
    all: parsed.data.all,
  };

  let result;
  try {
    result = await mutation.mutateAsync(request);
  } catch {
    return;
  }
  resetJobId();
  recentJobs.remember(request.tenant_id, request.job_id);
  ElMessage.success(result.data.replayed ? '已返回同一请求的幂等结果' : 'Add Job 已创建');
  await router.push({
    name: 'job-detail',
    params: { tenantId: request.tenant_id, jobId: request.job_id },
  });
}
</script>

<template>
  <div class="page page--narrow">
    <PageHeading title="扫描 Playground 变更" :description="`当前租户：${tenantId}`">
      <template v-if="playground" #actions>
        <el-button :icon="Back" @click="backToPlayground">返回 Playground</el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="mutation.error.value"
      :error="mutation.error.value"
      :retrying="mutation.isPending.value"
      @retry="submit"
    />
    <ApiProblemAlert
      v-if="sourcePlaygroundQuery.error.value"
      :error="sourcePlaygroundQuery.error.value"
      :retrying="sourcePlaygroundQuery.isFetching.value"
      @retry="sourcePlaygroundQuery.refetch"
    />
    <ApiProblemAlert
      v-if="playgroundQuery.error.value"
      :error="playgroundQuery.error.value"
      :retrying="playgroundQuery.isFetching.value"
      @retry="playgroundQuery.refetch"
    />

    <form class="job-form" @submit.prevent="submit">
      <section class="form-section">
        <div class="section-heading">
          <div>
            <h2>扫描范围</h2>
            <p>系统将读取该 Playground 的当前文件状态并生成新的 IndexVersion</p>
          </div>
        </div>
        <el-form-item label="Playground" :error="errors.playground" required>
          <PlaygroundSelect v-model="selectedPlayground" :tenant-id="tenantId" />
        </el-form-item>
        <dl v-if="playground" class="scope-summary">
          <div>
            <dt>Project</dt>
            <dd>
              <code>{{ playground.project_id }}</code>
            </dd>
          </div>
          <div>
            <dt>Artifact</dt>
            <dd>
              <code>{{ playground.artifact_id }}</code>
            </dd>
          </div>
          <div>
            <dt>Playground</dt>
            <dd>
              <code>{{ playground.playground_id }}</code>
            </dd>
          </div>
        </dl>
      </section>

      <section class="form-section">
        <div class="section-heading">
          <div>
            <h2>一致性条件</h2>
            <p>扫描只会在当前 IndexVersion 未发生变化时发布结果</p>
          </div>
        </div>
        <div class="form-grid form-grid--two">
          <el-form-item label="Job ID" :error="errors.jobId">
            <el-input v-model="form.jobId">
              <template #append>
                <el-button :icon="RefreshLeft" title="生成新的 Job ID" @click="resetJobId" />
              </template>
            </el-input>
          </el-form-item>
          <el-form-item label="Deadline" :error="errors.deadline">
            <el-date-picker v-model="form.deadline" type="datetime" class="full-width" />
          </el-form-item>
          <div class="index-version-field">
            <span>Expected revision</span>
            <code>{{ playground?.index_version.revision ?? '—' }}</code>
          </div>
          <div class="index-version-field">
            <span>Expected digest</span>
            <code>{{ playground?.index_version.digest ?? '—' }}</code>
          </div>
        </div>
      </section>

      <section class="form-section">
        <div class="section-heading section-heading--inline">
          <div>
            <h2>路径范围</h2>
            <p>每行一个 repository-relative path</p>
          </div>
          <el-switch v-model="form.all" inline-prompt active-text="全部" inactive-text="指定" />
        </div>
        <el-form-item :error="errors.paths">
          <el-input
            v-model="form.pathsText"
            type="textarea"
            :rows="6"
            :disabled="form.all"
            placeholder="dataset/images"
            resize="vertical"
          />
        </el-form-item>
      </section>

      <div class="form-actions">
        <el-button
          type="primary"
          native-type="submit"
          :icon="CirclePlus"
          :loading="mutation.isPending.value"
          :disabled="!playground || playgroundQuery.isFetching.value"
        >
          开始扫描
        </el-button>
      </div>
    </form>
  </div>
</template>

<style scoped>
.index-version-field {
  min-width: 0;
  display: grid;
  gap: 8px;
  align-content: start;
}

.scope-summary {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  margin: 0;
}

.scope-summary > div {
  min-width: 0;
  display: grid;
  gap: 6px;
}

.scope-summary dt {
  color: var(--muted);
  font-size: 12px;
}

.scope-summary dd {
  min-width: 0;
  margin: 0;
}

.scope-summary code {
  display: block;
  overflow-wrap: anywhere;
}

.index-version-field > span {
  color: var(--muted);
  font-size: 12px;
}

.index-version-field code {
  min-height: 32px;
  padding: 8px 10px;
  overflow-wrap: anywhere;
  border: 1px solid var(--line);
  background: #f7f9f8;
}

@media (max-width: 720px) {
  .scope-summary {
    grid-template-columns: 1fr;
  }
}
</style>
