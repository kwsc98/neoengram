<script setup lang="ts">
import { ArrowRight, Delete, Search } from '@element-plus/icons-vue';
import { computed, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import PageHeading from '@/components/PageHeading.vue';
import { useRecentJobsStore } from '@/stores/recent-jobs';

const route = useRoute();
const router = useRouter();
const recent = useRecentJobsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const jobId = ref('');
const tenantJobs = computed(() => recent.jobs.filter((item) => item.tenantId === tenantId.value));

async function query(): Promise<void> {
  const id = jobId.value.trim();
  if (!id) return;
  recent.remember(tenantId.value, id);
  await open(id);
}

async function open(id: string): Promise<void> {
  await router.push({
    name: 'job-detail',
    params: { tenantId: tenantId.value, jobId: id },
  });
}

function formatSeen(value: string): string {
  return new Intl.DateTimeFormat('zh-CN', { dateStyle: 'medium', timeStyle: 'short' }).format(
    new Date(value),
  );
}
</script>

<template>
  <div class="page">
    <PageHeading title="活动" :description="`查看 ${tenantId} 中的数据扫描、物化和发布任务`">
    </PageHeading>

    <form class="query-bar query-bar--tenant" @submit.prevent="query">
      <div class="query-tenant">
        <span>当前租户</span><code>{{ tenantId }}</code>
      </div>
      <el-input v-model="jobId" aria-label="Job ID" placeholder="Job ID" />
      <el-button type="primary" native-type="submit" :icon="Search" :disabled="!jobId">
        查询
      </el-button>
    </form>

    <section class="content-section">
      <div class="section-heading section-heading--inline">
        <div>
          <h2>最近活动</h2>
          <p>{{ tenantJobs.length }} 条浏览器本地记录</p>
        </div>
        <el-button
          v-if="tenantJobs.length"
          text
          type="danger"
          :icon="Delete"
          @click="recent.clearTenant(tenantId)"
          >清空</el-button
        >
      </div>

      <el-empty v-if="tenantJobs.length === 0" description="暂无最近访问的 Job" :image-size="72" />
      <el-table v-else :data="tenantJobs" class="recent-table">
        <el-table-column prop="jobId" label="Job ID" min-width="240" />
        <el-table-column label="最近访问" min-width="180">
          <template #default="scope">{{ formatSeen(scope.row.lastSeen) }}</template>
        </el-table-column>
        <el-table-column width="64" align="right">
          <template #default="scope">
            <el-button text :icon="ArrowRight" title="打开 Job" @click="open(scope.row.jobId)" />
          </template>
        </el-table-column>
      </el-table>
    </section>
  </div>
</template>
