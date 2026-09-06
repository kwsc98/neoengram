import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { ArtifactView } from '@/api/types';
import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import ArtifactSelect from '@/components/ArtifactSelect.vue';
import StorageVolumeFilter from '@/components/StorageVolumeFilter.vue';
import WorkspaceListPage from '@/pages/WorkspaceListPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  createWorkspace: vi.fn(),
  queryApiVersion: vi.fn(),
  queryArtifactCommitGraph: vi.fn(),
  queryArtifactList: vi.fn(),
  queryWorkspaceList: vi.fn(),
  queryStorageVolumeList: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const headCommitId = 'a'.repeat(64);
const historicalCommitId = 'b'.repeat(64);
const artifact: ArtifactView = {
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'artifact-a',
  display_name: 'Artifact A',
  initialization: { mode: 'empty' },
  head_commit_id: headCommitId,
  resource_version: '1',
  lifecycle: { state: 'active', generation: '1' },
  created_at_unix_ms: '1',
  updated_at_unix_ms: '1',
};

async function mountPage(workspaceItems: Array<Record<string, unknown>> = []) {
  api.queryApiVersion.mockResolvedValue({
    data: {
      service: 'neoengram-central',
      version: '0.2.0',
      git_commit: 'test',
      api_version: 1,
      agent_wire_version: 1,
      capabilities: ['artifact_catalog', 'artifact_commit_graph', 'workspace_materialize'],
    },
    requestId: 'request-version',
  });
  api.queryWorkspaceList.mockResolvedValue({
    data: { items: workspaceItems },
    requestId: 'request-workspaces',
  });
  api.queryArtifactList.mockResolvedValue({
    data: { items: [artifact] },
    requestId: 'request-artifacts',
  });
  api.queryArtifactCommitGraph.mockResolvedValue({
    data: {
      graph: {
        graph_version: '2',
        head_commit_id: headCommitId,
        nodes: [
          {
            commit_id: headCommitId,
            parent_commit_id: historicalCommitId,
            message: 'Current head',
            tag_names: [],
            data_layout: 'fast_cdc',
            created_at_unix_ms: '2',
          },
          {
            commit_id: historicalCommitId,
            message: 'Historical baseline',
            tag_names: [],
            data_layout: 'fast_cdc',
            created_at_unix_ms: '1',
          },
        ],
      },
    },
    requestId: 'request-commits',
  });
  api.queryStorageVolumeList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-volumes',
  });
  api.createWorkspace.mockResolvedValue({
    data: {
      workspace: {
        tenant_id: 'tenant-a',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        workspace_id: 'historical-review',
        storage_volume_id: 'volume-a',
        region: 'cn-shanghai',
        display_name: 'Historical review',
        base_commit_id: historicalCommitId,
        head_commit_id: historicalCommitId,
        index_version: { revision: '1', digest: historicalCommitId },
        state: 'creating',
        storage_availability: 'ready',
        created_at_unix_ms: '1',
        updated_at_unix_ms: '1',
      },
      request_replayed: false,
      execution_reused: false,
    },
    requestId: 'request-create-workspace',
  });

  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      permissions: ['workspace.read', 'workspace.create'],
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '1',
    },
  ];
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/workspaces',
        name: 'workspace-list',
        component: WorkspaceListPage,
      },
      { path: '/workspace', name: 'workspace-detail', component: { template: '<div />' } },
    ],
  });
  await router.push('/tenants/tenant-a/workspaces');
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(WorkspaceListPage, {
    global: { plugins: [ElementPlus, pinia, [VueQueryPlugin, { queryClient }], router] },
  });
  await flushPromises();
  return { queryClient, wrapper };
}

afterEach(() => vi.clearAllMocks());

describe('Workspace list creation', () => {
  it('defaults to Artifact Head and submits a selected historical Commit', async () => {
    const { queryClient, wrapper } = await mountPage();

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '创建 Workspace')!
      .trigger('click');
    await flushPromises();
    wrapper.findComponent(ArtifactSelect).vm.$emit('update:modelValue', artifact);
    await flushPromises();

    const commitSelect = wrapper.findComponent(ArtifactCommitSelect);
    expect(commitSelect.props('modelValue')).toBe(headCommitId);
    expect(api.queryArtifactCommitGraph).toHaveBeenCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
    );
    commitSelect.vm.$emit('update:modelValue', historicalCommitId);
    await wrapper.find('input[placeholder="review-august"]').setValue('historical-review');
    await wrapper.find('input[placeholder="八月复核"]').setValue('Historical review');
    wrapper.findComponent(StorageVolumeFilter).vm.$emit('update:modelValue', 'volume-a');
    await flushPromises();
    await wrapper
      .findAll('button')
      .filter((button) => button.text() === '创建 Workspace')
      .at(-1)!
      .trigger('click');
    await flushPromises();

    expect(api.createWorkspace.mock.calls[0]?.[0]).toEqual({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      workspace_id: 'historical-review',
      display_name: 'Historical review',
      storage_volume_id: 'volume-a',
      base_commit_id: historicalCommitId,
    });

    wrapper.unmount();
    queryClient.clear();
  });

  it('renders lifecycle and live storage availability as separate statuses', async () => {
    const { queryClient, wrapper } = await mountPage([
      {
        tenant_id: 'tenant-a',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        workspace_id: 'offline-review',
        storage_volume_id: 'volume-a',
        region: 'cn-shanghai',
        display_name: 'Offline review',
        index_version: { revision: '1', digest: historicalCommitId },
        state: 'ready',
        storage_availability: 'unavailable',
        created_at_unix_ms: '1',
        updated_at_unix_ms: '2',
      },
    ]);

    expect(wrapper.text()).toContain('已物化');
    expect(wrapper.text()).toContain('存储不可达');
    expect(wrapper.text()).not.toContain('可用');

    wrapper.unmount();
    queryClient.clear();
  });
});
