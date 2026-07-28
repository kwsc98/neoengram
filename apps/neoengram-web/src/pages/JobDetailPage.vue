<script setup lang="ts">
import { Check, RefreshRight } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, watch } from 'vue';
import { useRoute } from 'vue-router';

import { finalizeAddJob, queryJob, type ApiResult } from '@/api/operations';
import type { QueryJobResponse } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import JobStateTag from '@/components/JobStateTag.vue';
import PageHeading from '@/components/PageHeading.vue';
import { jobPollInterval } from '@/features/jobs/polling';
import { useRecentJobsStore } from '@/stores/recent-jobs';

const route = useRoute();
const queryClient = useQueryClient();
const recent = useRecentJobsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const jobId = computed(() => String(route.params.jobId ?? ''));
const queryKey = computed(() => ['job', tenantId.value, jobId.value] as const);

const jobQuery = useQuery({
  queryKey,
  queryFn: () => queryJob(tenantId.value, jobId.value),
  refetchInterval: (query) => jobPollInterval(query.state.data?.data.job.state),
});
const job = computed(() => jobQuery.data.value?.data.job);
const finalizeMutation = useMutation({
  mutationFn: () => finalizeAddJob(tenantId.value, jobId.value),
});

watch(
  [tenantId, jobId],
  ([tenant, id]) => {
    if (tenant && id) recent.remember(tenant, id);
  },
  { immediate: true },
);

async function finalize(): Promise<void> {
  try {
    await ElMessageBox.confirm('将校验 Prepared metadata 并执行最终 Index CAS。', '确认 Finalize', {
      confirmButtonText: 'Finalize',
      cancelButtonText: '取消',
      type: 'warning',
    });
  } catch {
    return;
  }
  let result;
  try {
    result = await finalizeMutation.mutateAsync();
  } catch {
    return;
  }
  const queryResult: ApiResult<QueryJobResponse> = {
    data: { job: result.data.job },
    requestId: result.requestId,
  };
  queryClient.setQueryData(queryKey.value, queryResult);
  ElMessage.success(result.data.replayed ? '已重放稳定终态' : 'Job 发布成功');
}

function formatTime(value?: string): string {
  if (!value) return '—';
  return new Intl.DateTimeFormat('zh-CN', { dateStyle: 'medium', timeStyle: 'medium' }).format(
    new Date(Number(value)),
  );
}

function formatCount(value?: string): string {
  if (!value) return '0';
  return BigInt(value).toLocaleString('zh-CN');
}

function formatBytes(value?: string): string {
  if (!value) return '0 B';
  const bytes = BigInt(value);
  const gib = 1024n ** 3n;
  const mib = 1024n ** 2n;
  if (bytes >= gib) return `${Number((bytes * 10n) / gib) / 10} GiB`;
  if (bytes >= mib) return `${Number((bytes * 10n) / mib) / 10} MiB`;
  return `${bytes.toLocaleString('zh-CN')} B`;
}
</script>

<template>
  <div class="page">
    <PageHeading :title="jobId || 'Job 详情'" :description="tenantId">
      <template #actions>
        <JobStateTag v-if="job" :state="job.state" />
        <el-button
          :icon="RefreshRight"
          :loading="jobQuery.isFetching.value"
          @click="jobQuery.refetch"
        >
          刷新
        </el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="jobQuery.error.value"
      :error="jobQuery.error.value"
      :retrying="jobQuery.isFetching.value"
      @retry="jobQuery.refetch"
    />
    <ApiProblemAlert
      v-if="finalizeMutation.error.value"
      :error="finalizeMutation.error.value"
      :retrying="finalizeMutation.isPending.value"
      @retry="finalize"
    />

    <div v-if="job" class="job-detail">
      <section class="job-summary">
        <div><span>状态</span><JobStateTag :state="job.state" /></div>
        <div>
          <span>Resource version</span><strong>{{ job.resource_version }}</strong>
        </div>
        <div>
          <span>Deadline</span><strong>{{ formatTime(job.deadline_unix_ms) }}</strong>
        </div>
        <div>
          <span>Request ID</span><code>{{ jobQuery.data.value?.requestId }}</code>
        </div>
      </section>

      <section class="content-section">
        <div class="section-heading">
          <div>
            <h2>资源范围</h2>
            <p>公开 JobView 中的 tenant-scoped identity</p>
          </div>
        </div>
        <dl class="definition-grid definition-grid--scope">
          <div>
            <dt>Tenant</dt>
            <dd>{{ job.tenant_id }}</dd>
          </div>
          <div>
            <dt>Project</dt>
            <dd>{{ job.project_id }}</dd>
          </div>
          <div>
            <dt>Artifact</dt>
            <dd>{{ job.artifact_id }}</dd>
          </div>
          <div>
            <dt>Playground</dt>
            <dd>{{ job.playground_id }}</dd>
          </div>
        </dl>
      </section>

      <section v-if="job.progress" class="content-section">
        <div class="section-heading section-heading--inline">
          <div>
            <h2>执行进度</h2>
            <p>{{ job.progress.phase }}</p>
          </div>
          <span class="live-indicator"><span /> 权威观测</span>
        </div>
        <div class="progress-metrics">
          <div>
            <span>完成文件</span><strong>{{ formatCount(job.progress.files_completed) }}</strong>
          </div>
          <div>
            <span>处理字节</span><strong>{{ formatBytes(job.progress.bytes_completed) }}</strong>
          </div>
          <div>
            <span>阶段状态</span><strong>{{ job.progress.state }}</strong>
          </div>
        </div>
      </section>

      <section v-if="job.state === 'prepared'" class="action-band">
        <div>
          <h2>Prepared Add 已可发布</h2>
          <p>Finalize 将执行 durability 校验与 expected IndexVersion CAS。</p>
        </div>
        <el-button
          type="primary"
          :icon="Check"
          :loading="finalizeMutation.isPending.value"
          @click="finalize"
        >
          Finalize
        </el-button>
      </section>

      <section v-if="job.decision" class="content-section decision-section">
        <div class="section-heading">
          <div>
            <h2>发布决定</h2>
            <p>稳定、可重放的终态结果</p>
          </div>
        </div>
        <dl class="definition-grid">
          <div>
            <dt>Outcome</dt>
            <dd>{{ job.decision.outcome }}</dd>
          </div>
          <div>
            <dt>Final state</dt>
            <dd>{{ job.decision.final_state }}</dd>
          </div>
          <div v-if="job.decision.outcome === 'publish'">
            <dt>Revision</dt>
            <dd>{{ job.decision.published_index_version.revision }}</dd>
          </div>
          <div class="definition-grid__wide">
            <dt>Finalized at</dt>
            <dd>{{ formatTime(job.finalized_at_unix_ms) }}</dd>
          </div>
        </dl>
      </section>

      <section v-if="job.failure" class="content-section failure-section">
        <div class="section-heading">
          <div>
            <h2>失败信息</h2>
            <p>{{ job.failure.stage }}</p>
          </div>
        </div>
        <p>
          <code>{{ job.failure.error.code }}</code> {{ job.failure.error.message }}
        </p>
      </section>
    </div>

    <div v-else-if="jobQuery.isPending.value" class="page-loading">
      <el-skeleton :rows="7" animated />
    </div>
  </div>
</template>
