import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { DeletionOperationView, TenantView } from '@/api/types';
import RecycleBinPage from '@/pages/RecycleBinPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  createRetentionHold: vi.fn(),
  queryDeletion: vi.fn(),
  queryDeletionList: vi.fn(),
  releaseRetentionHold: vi.fn(),
  restoreDeletion: vi.fn(),
  retryDeletion: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const deletion: DeletionOperationView = {
  deletion_id: 'deletion-a',
  tenant_id: 'tenant-a',
  root: {
    type: 'playground',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    playground_id: 'review-a',
  },
  state: 'recoverable',
  resource_version: '2',
  targets: [
    {
      resource: {
        type: 'playground',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        playground_id: 'review-a',
      },
      resource_version: '4',
      lifecycle_generation: '2',
      requires_agent_cleanup: true,
    },
  ],
  request_id: 'request-a',
  request_digest: 'a'.repeat(64),
  impact_digest: 'b'.repeat(64),
  cascade: false,
  confirm_managed_data_erase: false,
  purge_after_unix_ms: (Date.now() + 3 * 24 * 60 * 60 * 1_000).toString(),
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
  retry_count: '0',
};

async function mountPage() {
  api.queryDeletionList.mockResolvedValue({
    data: { items: [deletion] },
    requestId: 'request-list',
  });
  api.queryDeletion.mockResolvedValue({
    data: { deletion, retention_holds: [] },
    requestId: 'request-detail',
  });
  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      permissions: [
        'tenant.read',
        'resource.lifecycle.read',
        'resource.lifecycle.manage',
        'retention.manage',
      ] as TenantView['permissions'],
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '1',
    },
  ];
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/recycle-bin',
        name: 'recycle-bin',
        component: RecycleBinPage,
      },
    ],
  });
  await router.push('/tenants/tenant-a/recycle-bin');
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(RecycleBinPage, {
    global: { plugins: [ElementPlus, pinia, [VueQueryPlugin, { queryClient }], router] },
  });
  await flushPromises();
  return { wrapper, queryClient };
}

afterEach(() => vi.clearAllMocks());

describe('Recycle bin', () => {
  it('shows recoverable resources and opens the operation details', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(wrapper.text()).toContain('review-a');
    expect(wrapper.text()).toContain('可恢复');
    expect(wrapper.text()).toContain('3 天');

    await wrapper.get('button.mobile-resource-item').trigger('click');
    await flushPromises();

    expect(api.queryDeletion).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      deletion_id: 'deletion-a',
    });
    expect(wrapper.text()).toContain('删除任务详情');
    expect(wrapper.text()).toContain('没有活动保留锁');

    wrapper.unmount();
    queryClient.clear();
  });
});
