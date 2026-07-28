<script setup lang="ts">
import { Key, RefreshRight } from '@element-plus/icons-vue';
import { computed } from 'vue';

import { isApiProblem } from '@/api/problem';
import { useAuthStore } from '@/stores/auth';

const props = defineProps<{
  error: unknown;
  retrying?: boolean;
}>();

const emit = defineEmits<{ retry: [] }>();
const auth = useAuthStore();
const problem = computed(() => (isApiProblem(props.error) ? props.error : null));
const title = computed(() => {
  switch (problem.value?.status) {
    case 401:
      return '登录状态已失效';
    case 403:
      return '当前身份无权执行此操作';
    case 404:
      return '未找到可见的 Job';
    case 408:
      return 'Job 已超过 deadline';
    case 409:
      return '请求与当前状态冲突';
    case 422:
      return '请求未通过校验';
    case 503:
      return '中心 authority 暂不可用';
    default:
      return '请求未完成';
  }
});
</script>

<template>
  <el-alert :title="title" type="error" :closable="false" show-icon class="problem-alert">
    <p>{{ problem?.message ?? '发生了未分类错误' }}</p>
    <ul v-if="problem?.violations?.length" class="problem-alert__violations">
      <li v-for="violation in problem.violations" :key="`${violation.field}:${violation.reason}`">
        <code>{{ violation.field }}</code> {{ violation.reason }}
      </li>
    </ul>
    <div v-if="problem" class="problem-alert__meta">
      <el-tag effect="plain" size="small">{{ problem.code }}</el-tag>
      <span
        >Request ID: <code>{{ problem.requestId }}</code></span
      >
    </div>
    <div class="problem-alert__actions">
      <el-button v-if="problem?.status === 401" type="primary" :icon="Key" @click="auth.login()">
        重新登录
      </el-button>
      <el-button
        v-if="problem?.retryable"
        :icon="RefreshRight"
        :loading="retrying"
        @click="emit('retry')"
      >
        重试
      </el-button>
    </div>
  </el-alert>
</template>
