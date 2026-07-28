<script setup lang="ts">
import { computed } from 'vue';

import type { JobState } from '@/api/types';

const props = defineProps<{ state: JobState }>();

const labels: Record<JobState, string> = {
  queued: '排队中',
  assigned: '已分配',
  accepted: '已接收',
  running: '运行中',
  prepared: '待发布',
  publishing: '发布中',
  cancel_requested: '取消中',
  succeeded: '已成功',
  conflicted: '有冲突',
  rejected: '已拒绝',
  failed: '失败',
  cancelled: '已取消',
  timed_out: '已超时',
  recovery_required: '需要恢复',
  unknown: '未知',
};

const tagType = computed(() => {
  if (props.state === 'succeeded') return 'success';
  if (props.state === 'prepared') return 'warning';
  if (
    ['conflicted', 'rejected', 'failed', 'timed_out', 'recovery_required'].includes(props.state)
  ) {
    return 'danger';
  }
  if (['queued', 'assigned', 'accepted', 'running', 'publishing'].includes(props.state))
    return 'primary';
  return 'info';
});
</script>

<template>
  <el-tag :type="tagType" effect="light" round>{{ labels[state] }}</el-tag>
</template>
