import 'element-plus/dist/index.css';
import './styles/base.css';

import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import {
  ElAlert,
  ElButton,
  ElConfigProvider,
  ElDatePicker,
  ElDialog,
  ElDrawer,
  ElEmpty,
  ElForm,
  ElIcon,
  ElInput,
  ElOption,
  ElProgress,
  ElRadio,
  ElSelect,
  ElSegmented,
  ElSkeleton,
  ElSwitch,
  ElTabPane,
  ElTable,
  ElTableColumn,
  ElTag,
  ElTabs,
  ElTimeline,
  ElTooltip,
} from 'element-plus';
import { createPinia } from 'pinia';
import { createApp } from 'vue';

import App from './App.vue';
import { isApiProblem } from './api/problem';
import { runtimeConfig } from './config';
import { router } from './router';
import { useAuthStore } from './stores/auth';

const elementComponents = [
  ElAlert,
  ElButton,
  ElConfigProvider,
  ElDatePicker,
  ElDialog,
  ElDrawer,
  ElEmpty,
  ElForm,
  ElIcon,
  ElInput,
  ElOption,
  ElProgress,
  ElRadio,
  ElSelect,
  ElSegmented,
  ElSkeleton,
  ElSwitch,
  ElTabPane,
  ElTable,
  ElTableColumn,
  ElTag,
  ElTabs,
  ElTimeline,
  ElTooltip,
] as const;

async function enableMocks(): Promise<void> {
  if (import.meta.env.PROD) return;
  if (runtimeConfig.apiMode !== 'mock') return;
  const { worker } = await import('./mocks/browser');
  await worker.start({ onUnhandledRequest: 'bypass', quiet: true });
}

async function bootstrap(): Promise<void> {
  await enableMocks();
  const pinia = createPinia();
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: (failureCount, error) => isApiProblem(error) && error.retryable && failureCount < 2,
        retryDelay: (_attempt, error) =>
          isApiProblem(error) ? (error.retryAfterMs ?? 1000) : 1000,
        refetchOnWindowFocus: false,
      },
      mutations: { retry: false },
    },
  });

  const app = createApp(App);
  app.use(pinia);
  app.use(VueQueryPlugin, { queryClient });
  for (const component of elementComponents) app.use(component);
  app.use(router);
  await useAuthStore(pinia).initialize();
  await router.isReady();
  app.mount('#app');
}

void bootstrap();
