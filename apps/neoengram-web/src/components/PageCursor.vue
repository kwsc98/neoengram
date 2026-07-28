<script setup lang="ts">
import { ArrowLeft, ArrowRight } from '@element-plus/icons-vue';

defineProps<{
  hasPrevious: boolean;
  hasNext: boolean;
  loading?: boolean;
}>();
defineEmits<{ previous: []; next: [] }>();
</script>

<template>
  <div class="page-cursor" aria-label="分页">
    <el-button :icon="ArrowLeft" :disabled="!hasPrevious || loading" @click="$emit('previous')">
      上一页
    </el-button>
    <el-button :icon="ArrowRight" :disabled="!hasNext || loading" @click="$emit('next')">
      下一页
    </el-button>
  </div>
</template>
