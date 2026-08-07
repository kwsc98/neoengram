import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import SnapshotDetailPage from '@/pages/SnapshotDetailPage.vue';

const api = vi.hoisted(() => ({ querySnapshot: vi.fn() }));
vi.mock('@/api/operations', () => api);

const commitId = 'a'.repeat(64);

function snapshot(state: 'creating' | 'ready' | 'abnormal' = 'ready') {
  return {
    snapshot_id: 'snapshot-a',
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    commit_id: commitId,
    storage_volume_id: 'volume-a',
    region: 'cn-shanghai',
    message: 'Freeze training data',
    tag_names: ['dataset/v1'],
    state,
    phase: state === 'creating' ? ('materializing' as const) : ('idle' as const),
    ...(state === 'abnormal'
      ? { issue: { code: 'DELIVERY_FAILED', message: 'Delivery failed', retryable: true } }
      : {}),
    integrity: {
      state:
        state === 'ready'
          ? ('verified' as const)
          : state === 'abnormal'
            ? ('failed' as const)
            : ('pending' as const),
      files_verified: state === 'ready' ? '3' : '0',
      bytes_verified: state === 'ready' ? '30' : '0',
    },
    logical_file_count: '3',
    logical_size_bytes: '30',
    created_at_unix_ms: '1',
    updated_at_unix_ms: '2',
  };
}

async function mountPage(state: 'creating' | 'ready' | 'abnormal' = 'ready') {
  api.querySnapshot.mockResolvedValue({
    data: { snapshot: snapshot(state) },
    requestId: 'request-snapshot',
  });
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/:snapshotId',
        name: 'snapshot-detail',
        component: SnapshotDetailPage,
      },
      { path: '/artifact', name: 'artifact-detail', component: { template: '<div />' } },
    ],
  });
  await router.push(
    '/tenants/tenant-a/projects/project-a/artifacts/artifact-a/snapshots/snapshot-a',
  );
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = shallowMount(SnapshotDetailPage, {
    global: { plugins: [ElementPlus, [VueQueryPlugin, { queryClient }], router] },
  });
  await flushPromises();
  return { wrapper, queryClient };
}

afterEach(() => vi.clearAllMocks());

describe('Snapshot detail page', () => {
  it('queries the real Snapshot and presents its immutable read-only placement', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.querySnapshot).toHaveBeenCalledWith('tenant-a', 'snapshot-a');
    expect(wrapper.text()).toContain('固定 Commit 已通过只读 FUSE 视图交付');
    expect(wrapper.text()).toContain('volume-a');
    expect(wrapper.text()).toContain(commitId);
    expect(wrapper.text()).toContain('只读');
    expect(wrapper.text()).not.toContain('重试交付');

    wrapper.unmount();
    queryClient.clear();
  });

  it('shows the materialization phase while delivery is creating', async () => {
    const { wrapper, queryClient } = await mountPage('creating');

    expect(wrapper.text()).toContain('创建中');
    expect(wrapper.text()).toContain('物化数据');
    expect(wrapper.text()).toContain('目标 Volume 正在建立只读 FUSE 视图');

    wrapper.unmount();
    queryClient.clear();
  });
});
