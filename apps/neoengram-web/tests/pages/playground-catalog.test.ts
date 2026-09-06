import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import WorkspaceDetailPage from '@/pages/WorkspaceDetailPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  queryApiVersion: vi.fn(),
  queryWorkspace: vi.fn(),
  queryWorkspaceChangeList: vi.fn(),
  queryWorkspaceDatasetProfile: vi.fn(),
  queryWorkspaceFileList: vi.fn(),
  queryWorkspaceFileMetadata: vi.fn(),
  startWorkspacePreCommit: vi.fn(),
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
      api_version: 1,
      agent_wire_version: 1,
      capabilities,
    },
    requestId: 'request-version',
  });
  api.queryWorkspace.mockResolvedValue({
    data: {
      workspace: {
        tenant_id: 'tenant-a',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        workspace_id: 'workspace-a',
        storage_volume_id: 'volume-a',
        region: 'cn-shanghai',
        display_name: 'Catalog Workspace',
        index_version: { revision: '7', digest: indexDigest },
        state,
        ...(includeStorageAvailability ? { storage_availability: storageAvailability } : {}),
        created_at_unix_ms: '1785167000000',
        updated_at_unix_ms: '1785167600000',
      },
    },
    requestId: 'request-workspace',
  });

  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      permissions: ['workspace.read', 'workspace.create', 'task.manage'],
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '2',
    },
  ];
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/workspaces/:workspaceId',
        name: 'workspace-detail',
        component: { template: '<div />' },
      },
      {
        path: '/tenants/:tenantId/workspaces',
        name: 'workspace-list',
        component: { template: '<div />' },
      },
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId',
        name: 'artifact-detail',
        component: { template: '<div />' },
      },
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/workspaces/:workspaceId/commit',
        name: 'workspace-commit',
        component: { template: '<div />' },
      },
    ],
  });
  await router.push(
    '/tenants/tenant-a/projects/project-a/artifacts/artifact-a/workspaces/workspace-a',
  );
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(WorkspaceDetailPage, {
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

describe('artifact_catalog-only Workspace detail', () => {
  it('shows authoritative metadata without exposing a direct Add/scan Job entry', async () => {
    const { queryClient, wrapper } = await mountPage();

    expect(api.queryWorkspace).toHaveBeenCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
      'workspace-a',
    );
    expect(api.queryWorkspaceChangeList).not.toHaveBeenCalled();
    expect(api.queryWorkspaceFileList).not.toHaveBeenCalled();
    expect(api.queryWorkspaceDatasetProfile).not.toHaveBeenCalled();
    expect(api.queryWorkspaceFileMetadata).not.toHaveBeenCalled();
    expect(api.startWorkspacePreCommit).not.toHaveBeenCalled();

    expect(wrapper.text()).toContain('Workspace 元数据');
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

  it('does not offer a scan Job for a non-ready Workspace', async () => {
    const { queryClient, wrapper } = await mountPage({ state: 'abnormal' });

    expect(wrapper.findAll('button').some((button) => button.text() === '创建扫描 Job')).toBe(
      false,
    );
    expect(api.queryWorkspaceChangeList).not.toHaveBeenCalled();
    expect(api.queryWorkspaceFileList).not.toHaveBeenCalled();
    expect(api.queryWorkspaceDatasetProfile).not.toHaveBeenCalled();
    expect(api.queryWorkspaceFileMetadata).not.toHaveBeenCalled();
    expect(api.startWorkspacePreCommit).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain('工作区物化异常');

    wrapper.unmount();
    queryClient.clear();
  });

  it('renders the materializing state as a read-only wait screen', async () => {
    const { queryClient, wrapper } = await mountPage({ state: 'creating' });

    expect(wrapper.text()).toContain('工作区正在创建');
    expect(wrapper.text()).toContain('创建中');
    expect(wrapper.text()).not.toContain('发起 Pre-commit');
    expect(api.queryWorkspaceChangeList).not.toHaveBeenCalled();
    expect(api.queryWorkspaceFileList).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('shows an unavailable StorageVolume, keeps central metadata readable, and gates mutations', async () => {
    const { queryClient, wrapper } = await mountPage({
      storageAvailability: 'unavailable',
      capabilities: ['artifact_catalog', 'workspace_browser', 'workspace_precommit'],
    });

    expect(wrapper.text()).toContain('已物化');
    expect(wrapper.text()).toContain('存储不可达');
    expect(wrapper.text()).toContain('可以查看中心索引');
    expect(wrapper.text()).toContain('工作区数据');
    expect(wrapper.findAll('button').some((button) => button.text() === '发起 Pre-commit')).toBe(
      false,
    );
    expect(api.queryWorkspaceChangeList).toHaveBeenCalled();
    expect(api.queryWorkspaceFileList).toHaveBeenCalled();
    expect(api.queryWorkspaceDatasetProfile).toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('fails closed for a legacy response that omits storage availability', async () => {
    const { queryClient, wrapper } = await mountPage({
      includeStorageAvailability: false,
      capabilities: ['artifact_catalog', 'workspace_browser', 'workspace_precommit'],
    });

    expect(wrapper.text()).toContain('存储状态未知');
    expect(wrapper.findAll('button').some((button) => button.text() === '发起 Pre-commit')).toBe(
      false,
    );
    expect(api.queryWorkspaceChangeList).toHaveBeenCalled();
    expect(api.startWorkspacePreCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('automatically follows Agent storage loss and recovery after the Workspace is ready', async () => {
    vi.useFakeTimers();
    const { queryClient, wrapper } = await mountPage({
      capabilities: ['artifact_catalog', 'workspace_browser', 'workspace_precommit'],
    });

    api.queryWorkspace.mockResolvedValueOnce({
      data: {
        workspace: {
          tenant_id: 'tenant-a',
          project_id: 'project-a',
          artifact_id: 'artifact-a',
          workspace_id: 'workspace-a',
          storage_volume_id: 'volume-a',
          region: 'cn-shanghai',
          display_name: 'Catalog Workspace',
          index_version: { revision: '7', digest: indexDigest },
          state: 'ready',
          storage_availability: 'unavailable',
          created_at_unix_ms: '1785167000000',
          updated_at_unix_ms: '1785167600000',
        },
      },
      requestId: 'request-workspace-offline',
    });
    await vi.advanceTimersByTimeAsync(5_000);
    await flushPromises();

    expect(api.queryWorkspace).toHaveBeenCalledTimes(2);
    expect(wrapper.text()).toContain('存储不可达');

    api.queryWorkspace.mockResolvedValueOnce({
      data: {
        workspace: {
          tenant_id: 'tenant-a',
          project_id: 'project-a',
          artifact_id: 'artifact-a',
          workspace_id: 'workspace-a',
          storage_volume_id: 'volume-a',
          region: 'cn-shanghai',
          display_name: 'Catalog Workspace',
          index_version: { revision: '7', digest: indexDigest },
          state: 'ready',
          storage_availability: 'ready',
          created_at_unix_ms: '1785167000000',
          updated_at_unix_ms: '1785167600000',
        },
      },
      requestId: 'request-workspace-recovered',
    });
    await vi.advanceTimersByTimeAsync(5_000);
    await flushPromises();

    expect(api.queryWorkspace).toHaveBeenCalledTimes(3);
    expect(wrapper.text()).toContain('存储可达');
    expect(wrapper.text()).not.toContain('依赖 Agent 的实时操作已暂停');

    wrapper.unmount();
    queryClient.clear();
  });
});
