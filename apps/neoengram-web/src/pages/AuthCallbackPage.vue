<script setup lang="ts">
import { Loading } from '@element-plus/icons-vue';
import { onMounted, ref } from 'vue';
import { useRouter } from 'vue-router';

import { useAuthStore } from '@/stores/auth';

const auth = useAuthStore();
const router = useRouter();
const error = ref('');

onMounted(async () => {
  try {
    const returnTo = await auth.handleCallback();
    await router.replace(returnTo);
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : 'OIDC callback failed';
  }
});
</script>

<template>
  <div class="auth-callback">
    <el-icon v-if="!error" class="is-loading"><Loading /></el-icon>
    <h1>{{ error ? '登录未完成' : '正在完成登录' }}</h1>
    <p v-if="error">{{ error }}</p>
  </div>
</template>
