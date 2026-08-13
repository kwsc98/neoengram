import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import PlaygroundDetailPage from '@/pages/PlaygroundDetailPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  queryApiVersion: vi.fn(),
  queryPlayground: vi.fn(),
  queryPlaygroundChangeList: vi.fn(),
  queryPlaygroundDatasetProfile: vi.fn(),
  queryPlaygroundFileList: vi.fn(),
  queryPlaygroundFileMetadata: vi.fn(),
  startPlaygroundPreCommit: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const indexDigest = 'a'.repeat(64);

interface MountPageOptions {
  state?: 'ready' | 'creating' | 'abnormal';
  storageAvailability?: 'ready' | 'degraded' | 'unavailable' | 'unknown';
  includeStorageAvailability?: boolean;
  capabilities?: string[];
}

async function mountPage({
  state = 'ready',
  storageAvailability = 'ready',
  includeStorageAvailability = true,
  capabilities = ['artifact_catalog'],
}: MountPageOptions = {}) {
  api.queryApiVersion.mockResolvedValue({
    data: {
      api_versions: [1],
      agent_protocol_versions: [1],
      capabilities,
    },
    requestId: 'request-version',
  });
  api.queryPlayground.mockResolvedValue({
    data: {
      playground: {
        tenant_id: 'tenant-a',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        playground_id: 'playground-a',
        storage_volume_id: 'volume-a',
        region: 'cn-shanghai',
        display_name: 'Catalog Playground',
        index_version: { revision: '7', digest: indexDigest },
        state,
        ...(includeStorageAvailability ? { storage_availability: storageAvailability } : {}),
        created_at_unix_ms: '1785167000000',
        updated_at_unix_ms: '1785167600000',
      },
    },
    requestId: 'request-playground',
  });

  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      permissions: ['playground.read', 'playground.create', 'job.create'],
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '2',
    },
  ];
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/playgrounds/:playgroundId',
        name: 'playground-detail',
        component: { template: '<div />' },
      },
      {
        path: '/tenants/:tenantId/jobs/new',
        name: 'job-create',
        component: { template: '<div />' },
      },
      {
        path: '/tenants/:tenantId/playgrounds',
        name: 'playground-list',
        component: { template: '<div />' },
      },
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId',
        name: 'artifact-detail',
        component: { template: '<div />' },
      },
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/playgrounds/:playgroundId/commit',
        name: 'playground-commit',
        component: { template: '<div />' },
      },
    ],
  });
  await router.push(
    '/tenants/tenant-a/projects/project-a/artifacts/artifact-a/playgrounds/playground-a',
  );
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(PlaygroundDetailPage, {
    global: {
      plugins: [ElementPlus, pinia, [VueQueryPlugin, { queryClient }], router],
    },
  });
  await flushPromises();
  return { queryClient, router, wrapper };
}

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
});

describe('artifact_catalog-only Playground detail', () => {
  it('shows authoritative metadata without exposing a direct Add/scan Job entry', async () => {
    const { queryClient, wrapper } = await mountPage();

    expect(api.queryPlayground).toHaveBeenCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
      'playground-a',
    );
    expect(api.queryPlaygroundChangeList).not.toHaveBeenCalled();
    expect(api.queryPlaygroundFileList).not.toHaveBeenCalled();
    expect(api.queryPlaygroundDatasetProfile).not.toHaveBeenCalled();
    expect(api.queryPlaygroundFileMetadata).not.toHaveBeenCalled();
    expect(api.startPlaygroundPreCommit).not.toHaveBeenCalled();

    expect(wrapper.text()).toContain('Playground 元数据');
    expect(wrapper.text()).toContain('artifact-a');
    expect(wrapper.text()).toContain('7');
    expect(wrapper.text()).toContain(indexDigest);
    expect(wrapper.text()).not.toContain('工作区数据');
    expect(wrapper.text()).not.toContain('Pre-commit');

    expect(wrapper.findAll('button').some((button) => button.text() === '创建扫描 Job')).toBe(
      false,
    );

    wrapper.unmount();
    queryClient.clear();
  });

  it('does not offer a scan Job for a non-ready Playground', async () => {
    const { queryClient, wrapper } = await mountPage({ state: 'abnormal' });

    expect(wrapper.findAll('button').some((button) => button.text() === '创建扫描 Job')).toBe(
      false,
    );
    expect(api.queryPlaygroundChangeList).not.toHaveBeenCalled();
    expect(api.queryPlaygroundFileList).not.toHaveBeenCalled();
    expect(api.queryPlaygroundDatasetProfile).not.toHaveBeenCalled();
    expect(api.queryPlaygroundFileMetadata).not.toHaveBeenCalled();
    expect(api.startPlaygroundPreCommit).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain('工作区物化异常');

    wrapper.unmount();
    queryClient.clear();
  });

  it('renders the materializing state as a read-only wait screen', async () => {
    const { queryClient, wrapper } = await mountPage({ state: 'creating' });

    expect(wrapper.text()).toContain('工作区正在创建');
    expect(wrapper.text()).toContain('创建中');
    expect(wrapper.text()).not.toContain('发起 Pre-commit');
    expect(api.queryPlaygroundChangeList).not.toHaveBeenCalled();
    expect(api.queryPlaygroundFileList).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('shows an unavailable StorageVolume, keeps central metadata readable, and gates mutations', async () => {
    const { queryClient, wrapper } = await mountPage({
      storageAvailability: 'unavailable',
      capabilities: ['artifact_catalog', 'playground_browser', 'playground_precommit'],
    });

    expect(wrapper.text()).toContain('已物化');
    expect(wrapper.text()).toContain('存储不可达');
    expect(wrapper.text()).toContain('可以查看中心索引');
    expect(wrapper.text()).toContain('工作区数据');
    expect(wrapper.findAll('button').some((button) => button.text() === '发起 Pre-commit')).toBe(
      false,
    );
    expect(api.queryPlaygroundChangeList).toHaveBeenCalled();
    expect(api.queryPlaygroundFileList).toHaveBeenCalled();
    expect(api.queryPlaygroundDatasetProfile).toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('fails closed for a legacy response that omits storage availability', async () => {
    const { queryClient, wrapper } = await mountPage({
      includeStorageAvailability: false,
      capabilities: ['artifact_catalog', 'playground_browser', 'playground_precommit'],
    });

    expect(wrapper.text()).toContain('存储状态未知');
    expect(wrapper.findAll('button').some((button) => button.text() === '发起 Pre-commit')).toBe(
      false,
    );
    expect(api.queryPlaygroundChangeList).toHaveBeenCalled();
    expect(api.startPlaygroundPreCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('automatically follows Agent storage loss and recovery after the Playground is ready', async () => {
    vi.useFakeTimers();
    const { queryClient, wrapper } = await mountPage({
      capabilities: ['artifact_catalog', 'playground_browser', 'playground_precommit'],
    });

    api.queryPlayground.mockResolvedValueOnce({
      data: {
        playground: {
          tenant_id: 'tenant-a',
          project_id: 'project-a',
          artifact_id: 'artifact-a',
          playground_id: 'playground-a',
          storage_volume_id: 'volume-a',
          region: 'cn-shanghai',
          display_name: 'Catalog Playground',
          index_version: { revision: '7', digest: indexDigest },
          state: 'ready',
          storage_availability: 'unavailable',
          created_at_unix_ms: '1785167000000',
          updated_at_unix_ms: '1785167600000',
        },
      },
      requestId: 'request-playground-offline',
    });
    await vi.advanceTimersByTimeAsync(5_000);
    await flushPromises();

    expect(api.queryPlayground).toHaveBeenCalledTimes(2);
    expect(wrapper.text()).toContain('存储不可达');

    api.queryPlayground.mockResolvedValueOnce({
      data: {
        playground: {
          tenant_id: 'tenant-a',
          project_id: 'project-a',
          artifact_id: 'artifact-a',
          playground_id: 'playground-a',
          storage_volume_id: 'volume-a',
          region: 'cn-shanghai',
          display_name: 'Catalog Playground',
          index_version: { revision: '7', digest: indexDigest },
          state: 'ready',
          storage_availability: 'ready',
          created_at_unix_ms: '1785167000000',
          updated_at_unix_ms: '1785167600000',
        },
      },
      requestId: 'request-playground-recovered',
    });
    await vi.advanceTimersByTimeAsync(5_000);
    await flushPromises();

    expect(api.queryPlayground).toHaveBeenCalledTimes(3);
    expect(wrapper.text()).toContain('存储可达');
    expect(wrapper.text()).not.toContain('依赖 Agent 的实时操作已暂停');

    wrapper.unmount();
    queryClient.clear();
  });
});
