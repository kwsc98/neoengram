<script setup lang="ts">
import { Back, CirclePlus, RefreshLeft } from '@element-plus/icons-vue';
import { useMutation, useQuery } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { createAddJob, queryPlayground } from '@/api/operations';
import type { CreateAddJobRequest } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { createJobFormSchema, parsePathLines } from '@/features/jobs/create-form';
import { useRecentJobsStore } from '@/stores/recent-jobs';

const route = useRoute();
const router = useRouter();
const recentJobs = useRecentJobsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const sourcePlayground = computed(() => ({
  projectId: String(route.query.project_id ?? ''),
  artifactId: String(route.query.artifact_id ?? ''),
  playgroundId: String(route.query.playground_id ?? ''),
}));
const hasSourcePlayground = computed(() =>
  Object.values(sourcePlayground.value).every((value) => Boolean(value)),
);
const form = reactive({
  projectId: sourcePlayground.value.projectId,
  artifactId: sourcePlayground.value.artifactId,
  playgroundId: sourcePlayground.value.playgroundId,
  jobId: `job-${globalThis.crypto.randomUUID()}`,
  deadline: new Date(Date.now() + 60 * 60 * 1000),
  all: true,
  pathsText: '',
});
const errors = reactive<Record<string, string>>({});

const mutation = useMutation({ mutationFn: createAddJob });
const hasPlaygroundIdentity = computed(() =>
  [form.projectId, form.artifactId, form.playgroundId].every((value) => Boolean(value.trim())),
);
const playgroundQuery = useQuery({
  queryKey: computed(() => [
    'playground',
    tenantId.value,
    form.projectId,
    form.artifactId,
    form.playgroundId,
    'job-create',
  ]),
  enabled: hasPlaygroundIdentity,
  queryFn: () =>
    queryPlayground(tenantId.value, form.projectId, form.artifactId, form.playgroundId),
});
const playground = computed(() => playgroundQuery.data.value?.data.playground);
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
    form.projectId,
    form.artifactId,
    form.playgroundId,
    form.deadline instanceof Date ? form.deadline.getTime() : '',
    form.all,
    form.pathsText,
  ],
  () => {
    resetJobId();
    clearErrors();
  },
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
  if (!hasSourcePlayground.value) return;
  await router.push({
    name: 'playground-detail',
    params: {
      tenantId: tenantId.value,
      projectId: sourcePlayground.value.projectId,
      artifactId: sourcePlayground.value.artifactId,
      playgroundId: sourcePlayground.value.playgroundId,
    },
  });
}

async function submit(): Promise<void> {
  if (mutation.isPending.value) return;
  clearErrors();
  const currentPlayground = playground.value;
  if (!currentPlayground) {
    errors.playgroundId = playgroundQuery.isFetching.value
      ? '正在读取 Playground 权威状态'
      : '请先选择可查询的 Playground';
    return;
  }
  const paths = parsePathLines(form.pathsText);
  const parsed = createJobFormSchema.safeParse({
    ...form,
    tenantId: tenantId.value,
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
    project_id: parsed.data.projectId,
    artifact_id: parsed.data.artifactId,
    playground_id: parsed.data.playgroundId,
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
      <template v-if="hasSourcePlayground" #actions>
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
        <div class="form-grid form-grid--two">
          <el-form-item label="Project ID" :error="errors.projectId">
            <el-input v-model="form.projectId" />
          </el-form-item>
          <el-form-item label="Artifact ID" :error="errors.artifactId">
            <el-input v-model="form.artifactId" />
          </el-form-item>
          <el-form-item label="Playground ID" :error="errors.playgroundId">
            <el-input v-model="form.playgroundId" />
          </el-form-item>
        </div>
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
</style>
