import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { TaskView } from '@/api/types';
import TaskListPage from '@/pages/TaskListPage.vue';

const api = vi.hoisted(() => ({
  queryTaskList: vi.fn(),
  queryTaskSummary: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const task: TaskView = {
  task_id: 'task-materialize-1',
  task_kind: 'commit.materialize',
  state: 'running',
  phase: 'materializing',
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'artifact-a',
  object_namespace_id: 'artifact-a',
  commit_id: 'commit-a',
  storage_volume_id: 'volume-a',
  request_id: 'request-a',
  request_digest: 'd'.repeat(64),
  actor: 'test-user',
  attempt: '1',
  progress: {
    completed: '1',
    total: '2',
    completed_bytes: '10',
    total_bytes: '20',
  },
  deadline_unix_ms: '9999999999999',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
  resource_version: '1',
  origin: 'user',
  executable: true,
};

const summary = {
  total: '1',
  queued: '0',
  running: '1',
  waiting: '0',
  verifying: '0',
  succeeded: '0',
  stalled: '0',
  failed: '0',
  cancelled: '0',
};

async function mountPage() {
  const listData = { items: [task] };
  const summaryData = { summary };
  api.queryTaskList.mockResolvedValue({ data: listData, requestId: 'list-request' });
  api.queryTaskSummary.mockResolvedValue({ data: summaryData, requestId: 'summary-request' });

  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: '/tenants/:tenantId/tasks', name: 'task-list', component: TaskListPage }],
  });
  await router.push('/tenants/tenant-a/tasks');
  await router.isReady();

  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(TaskListPage, {
    global: {
      plugins: [ElementPlus, [VueQueryPlugin, { queryClient }], router],
    },
  });
  await flushPromises();
  return { wrapper, queryClient };
}

afterEach(() => vi.clearAllMocks());

describe('Task list', () => {
  it('mounts and renders a running task while configuring polling', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryTaskList).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      page_size: 50,
    });
    expect(wrapper.text()).toContain('task-materialize-1');
    expect(wrapper.text()).toContain('运行中');

    wrapper.unmount();
    queryClient.clear();
  });
});
