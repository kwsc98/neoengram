<script setup lang="ts">
import { CirclePlus, RefreshLeft } from '@element-plus/icons-vue';
import { useMutation } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { createAddJob } from '@/api/operations';
import type { CreateAddJobRequest } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { createJobFormSchema, parsePathLines } from '@/features/jobs/create-form';
import { useRecentJobsStore } from '@/stores/recent-jobs';

const route = useRoute();
const router = useRouter();
const recentJobs = useRecentJobsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const form = reactive({
  projectId: 'project-vision',
  artifactId: 'road-scenes',
  playgroundId: 'labeling',
  jobId: `job-${globalThis.crypto.randomUUID()}`,
  revision: '0',
  digest: 'a'.repeat(64),
  deadline: new Date(Date.now() + 60 * 60 * 1000),
  all: false,
  pathsText: 'dataset/images\ndataset/labels.csv',
});
const errors = reactive<Record<string, string>>({});

const mutation = useMutation({ mutationFn: createAddJob });

function clearErrors(): void {
  for (const key of Object.keys(errors)) delete errors[key];
}

function resetJobId(): void {
  form.jobId = `job-${globalThis.crypto.randomUUID()}`;
}

async function submit(): Promise<void> {
  clearErrors();
  const paths = parsePathLines(form.pathsText);
  const parsed = createJobFormSchema.safeParse({ ...form, tenantId: tenantId.value, paths });
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
    expected_index_version: { revision: parsed.data.revision, digest: parsed.data.digest },
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
    <PageHeading title="创建 Add Job" :description="`当前租户：${tenantId}`" />

    <ApiProblemAlert
      v-if="mutation.error.value"
      :error="mutation.error.value"
      :retrying="mutation.isPending.value"
      @retry="submit"
    />

    <form class="job-form" @submit.prevent="submit">
      <section class="form-section">
        <div class="section-heading">
          <div>
            <h2>资源范围</h2>
            <p>Tenant 由当前路由固定，只声明其下的 Project、Artifact 与 Playground</p>
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
            <h2>Job 与版本条件</h2>
            <p>稳定 Job identity、expected IndexVersion 和 deadline</p>
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
          <el-form-item label="Expected revision" :error="errors.revision">
            <el-input v-model="form.revision" />
          </el-form-item>
          <el-form-item label="Expected digest" :error="errors.digest">
            <el-input v-model="form.digest" class="monospace-input" />
          </el-form-item>
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
        >
          创建 Job
        </el-button>
      </div>
    </form>
  </div>
</template>
