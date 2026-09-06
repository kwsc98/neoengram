import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { QueryTaskResponse, TaskView } from '@/api/types';
import TaskDetailPage from '@/pages/TaskDetailPage.vue';

const api = vi.hoisted(() => ({
  cancelTask: vi.fn(),
  queryTask: vi.fn(),
  retryTask: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const task: TaskView = {
  task_id: 'task-running',
  intent_kind: 'commit.materialize',
  purpose: 'copy',
  state: 'running',
  tenant_id: 'tenant-a',
  primary_resource: { resource_kind: 'materialization', resource_id: 'task-running' },
  resource_links: [
    { resource_kind: 'project', resource_id: 'project-a', role: 'related' },
    { resource_kind: 'artifact', resource_id: 'artifact-a', role: 'related' },
    { resource_kind: 'object_namespace', resource_id: 'artifact-a', role: 'related' },
    { resource_kind: 'commit', resource_id: 'commit-a', role: 'source' },
    { resource_kind: 'storage_volume', resource_id: 'volume-a', role: 'target' },
  ],
  execution_id: 'execution-task-running',
  execution_key_digest: 'e'.repeat(64),
  execution_reused: false,
  current_stage: {
    stage_key: 'transfer',
    stage_kind: 'transfer',
    ordinal: '3',
    dependencies: ['plan'],
    state: 'running',
    stage_attempt: '1',
    progress: { completed: '1', total: '2', completed_bytes: '10', total_bytes: '20' },
    created_at_unix_ms: '1',
    updated_at_unix_ms: '2',
    resource_version: '1',
  },
  stages: [],
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

const response: { data: QueryTaskResponse; requestId: string } = {
  data: {
    task,
    attempts: [],
    events: [],
  },
  requestId: 'task-request',
};

afterEach(() => vi.clearAllMocks());

describe('Task detail', () => {
  it('mounts with a cached active task while configuring polling', async () => {
    api.queryTask.mockResolvedValue(response);

    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        {
          path: '/tenants/:tenantId/tasks/:taskId',
          name: 'task-detail',
          component: TaskDetailPage,
        },
      ],
    });
    await router.push('/tenants/tenant-a/tasks/task-running');
    await router.isReady();

    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
    });
    queryClient.setQueryData(['task', 'tenant-a', 'task-running'], response);

    const wrapper = mount(TaskDetailPage, {
      global: {
        plugins: [ElementPlus, [VueQueryPlugin, { queryClient }], router],
      },
    });
    await flushPromises();

    expect(wrapper.text()).toContain('task-running');
    expect(wrapper.text()).toContain('运行中');
    expect(api.queryTask).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      task_id: 'task-running',
    });

    wrapper.unmount();
    queryClient.clear();
  });
});
