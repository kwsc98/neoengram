import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { ArtifactView, CommitGraphView, TenantView } from '@/api/types';
import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import PageHeading from '@/components/PageHeading.vue';
import StorageVolumeFilter from '@/components/StorageVolumeFilter.vue';
import ArtifactDetailPage from '@/pages/ArtifactDetailPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  createWorkspace: vi.fn(),
  queryApiVersion: vi.fn(),
  queryArtifact: vi.fn(),
  queryArtifactCommitGraph: vi.fn(),
  queryWorkspaceList: vi.fn(),
  querySnapshotList: vi.fn(),
  queryStorageVolumeList: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const headCommitId = 'a'.repeat(64);
const historicalCommitId = 'b'.repeat(64);

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const artifact = {
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'artifact-a',
  display_name: 'Authoritative data',
  initialization: { mode: 'empty' as const },
  head_commit_id: headCommitId,
  resource_version: '3',
  lifecycle: { state: 'active' as const, generation: '1' },
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
};

async function mountPage(
  location = '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
  artifactView: ArtifactView = artifact,
  capabilities = ['artifact_catalog'],
  commitGraph?: CommitGraphView,
  permissions: TenantView['permissions'] = ['artifact.read', 'workspace.create', 'snapshot.create'],
) {
  api.queryApiVersion.mockResolvedValue({
    data: {
      service: 'neoengram-central',
      version: '0.2.0',
      git_commit: 'test',
      api_version: 1,
      agent_wire_version: 1,
      capabilities,
    },
    requestId: 'request-version',
  });
  api.queryArtifact.mockResolvedValue({
    data: { artifact: artifactView },
    requestId: 'request-artifact',
  });
  api.queryWorkspaceList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-workspaces',
  });
  api.queryArtifactCommitGraph.mockResolvedValue({
    data: {
      graph: commitGraph ?? {
        graph_version: '1',
        head_commit_id: artifactView.head_commit_id,
        nodes: artifactView.head_commit_id
          ? [
              {
                commit_id: artifactView.head_commit_id,
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
            ]
          : [],
      },
    },
    requestId: 'request-commits',
  });
  api.queryStorageVolumeList.mockResolvedValue({
    data: {
      items: [
        {
          tenant_id: 'tenant-a',
          storage_volume_id: 'volume-a',
          display_name: 'Volume A',
          edge_cluster_id: 'edge-a',
          backend_type: 'pvc',
          access_mode: 'read_write_once',
          region: 'cn-shanghai',
          allowed_delivery_modes: ['copy'],
          hardlink_policy: 'disabled',
          max_whole_file_bytes: '1024',
          copy_reserve_bytes: '0',
          state: 'ready',
          resource_version: '1',
          lifecycle: { state: 'active', generation: '1', resource_version: '1' },
          created_at_unix_ms: '1',
          updated_at_unix_ms: '1',
        },
      ],
    },
    requestId: 'request-volumes',
  });
  api.querySnapshotList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-snapshots',
  });

  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      permissions,
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '2',
    },
  ];
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId',
        name: 'artifact-detail',
        component: ArtifactDetailPage,
      },
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/commits/:commitId',
        name: 'commit-detail',
        component: { template: '<div />' },
      },
      { path: '/workspace', name: 'workspace-detail', component: { template: '<div />' } },
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/:snapshotId',
        name: 'snapshot-detail',
        component: { template: '<div />' },
      },
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/new',
        name: 'snapshot-create',
        component: { template: '<div />' },
      },
    ],
  });
  await router.push(location);
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(ArtifactDetailPage, {
    global: {
      plugins: [ElementPlus, pinia, [VueQueryPlugin, { queryClient }], router],
    },
  });
  await flushPromises();
  return { queryClient, router, wrapper };
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('Artifact catalog detail', () => {
  it('loads the authoritative Artifact and Workspace relation without advanced APIs', async () => {
    const { queryClient, wrapper } = await mountPage();

    expect(api.queryArtifact).toHaveBeenCalledWith('tenant-a', 'project-a', 'artifact-a');
    expect(api.queryWorkspaceList).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      page_size: 100,
    });
    expect(api.queryArtifactCommitGraph).not.toHaveBeenCalled();
    expect(api.querySnapshotList).not.toHaveBeenCalled();
    expect(wrapper.findComponent(PageHeading).props('title')).toBe('Authoritative data');
    expect(wrapper.findAll('button').some((button) => button.text() === '创建 Workspace')).toBe(
      false,
    );

    wrapper.unmount();
    queryClient.clear();
  });

  it('allows Workspace creation when the server advertises materialization', async () => {
    const emptyArtifact: ArtifactView = { ...artifact };
    delete emptyArtifact.head_commit_id;
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      emptyArtifact,
      ['artifact_catalog', 'workspace_materialize'],
    );

    expect(wrapper.findAll('button').some((button) => button.text() === '创建 Workspace')).toBe(
      true,
    );

    wrapper.unmount();
    queryClient.clear();
  });

  it('keeps non-empty Workspace derivation available with explicit capabilities', async () => {
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      artifact,
      ['artifact_catalog', 'artifact_commit_graph', 'workspace_materialize'],
    );

    expect(wrapper.findAll('button').some((button) => button.text() === '创建 Workspace')).toBe(
      true,
    );

    wrapper.unmount();
    queryClient.clear();
  });

  it('exposes Snapshot creation with the precise materialization capability', async () => {
    const { queryClient, router, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      artifact,
      ['artifact_catalog', 'commit_materialization_v2'],
    );

    const createButton = wrapper
      .findAll('button')
      .find((button) => button.text() === '创建 Snapshot');
    expect(createButton).toBeDefined();
    expect(api.querySnapshotList).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      page_size: 100,
    });

    await createButton!.trigger('click');
    await flushPromises();
    expect(router.currentRoute.value.name).toBe('snapshot-create');
    expect(router.currentRoute.value.query.commit_id).toBe(headCommitId);

    wrapper.unmount();
    queryClient.clear();
  });

  it('submits the historical Commit selected while creating a Workspace', async () => {
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
          created_at_unix_ms: '1',
          updated_at_unix_ms: '1',
        },
        request_replayed: false,
        execution_reused: false,
      },
      requestId: 'request-create-workspace',
    });
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      artifact,
      ['artifact_catalog', 'workspace_materialize', 'artifact_commit_graph'],
    );

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '创建 Workspace')!
      .trigger('click');
    await flushPromises();

    const commitSelect = wrapper.findComponent(ArtifactCommitSelect);
    expect(commitSelect.props('modelValue')).toBe(headCommitId);
    expect(commitSelect.props('allowHistory')).toBe(true);
    commitSelect.vm.$emit('update:modelValue', historicalCommitId);
    await flushPromises();

    await wrapper.find('input[placeholder="review-july"]').setValue('historical-review');
    await wrapper.find('input[placeholder="七月复核"]').setValue('Historical review');
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
      storage_volume_id: 'volume-a',
      display_name: 'Historical review',
      base_commit_id: historicalCommitId,
    });

    wrapper.unmount();
    queryClient.clear();
  });

  it('does not treat a Commit query parameter as a detail fallback', async () => {
    const { queryClient, wrapper } = await mountPage(
      `/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits&commit_id=${headCommitId}`,
    );

    expect(api.queryArtifactCommitGraph).not.toHaveBeenCalled();
    expect(wrapper.findComponent({ name: 'ElDrawer' }).exists()).toBe(false);
    expect(wrapper.findComponent({ name: 'ElTabs' }).props('modelValue')).toBe('overview');

    wrapper.unmount();
    queryClient.clear();
  });

  it('renders commits sharing a parent as sibling branch tips with an explicit count', async () => {
    const siblingCommitId = 'c'.repeat(64);
    const rootCommitId = 'd'.repeat(64);
    const graph: CommitGraphView = {
      graph_version: '4',
      head_commit_id: headCommitId,
      nodes: [
        {
          commit_id: headCommitId,
          parent_commit_id: historicalCommitId,
          message: 'Commit 2',
          tag_names: ['default-line'],
          data_layout: 'fast_cdc',
          created_at_unix_ms: '4',
        },
        {
          commit_id: siblingCommitId,
          parent_commit_id: historicalCommitId,
          message: 'Commit 2.2',
          tag_names: ['experiment'],
          data_layout: 'whole_file',
          created_at_unix_ms: '3',
        },
        {
          commit_id: historicalCommitId,
          parent_commit_id: rootCommitId,
          message: 'Commit 1',
          tag_names: [],
          data_layout: 'fast_cdc',
          created_at_unix_ms: '2',
        },
        {
          commit_id: rootCommitId,
          message: 'Root commit',
          tag_names: [],
          data_layout: 'fast_cdc',
          created_at_unix_ms: '1',
        },
      ],
    };
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits',
      artifact,
      ['artifact_catalog', 'artifact_commit_graph'],
      graph,
    );

    const tree = wrapper.find('[aria-label="Commit 分支树"]');
    expect(tree.exists()).toBe(true);
    expect(tree.findAll('.commit-node')).toHaveLength(4);
    expect(wrapper.find('.commit-summary__metrics').text()).toContain('4已加载 Commit');
    expect(wrapper.find('.commit-summary__metrics').text()).toContain('2分支末端');

    const headNode = tree.find(`[data-commit-id="${headCommitId}"]`);
    const siblingNode = tree.find(`[data-commit-id="${siblingCommitId}"]`);
    expect(headNode.attributes('data-parent-commit-id')).toBe(historicalCommitId);
    expect(siblingNode.attributes('data-parent-commit-id')).toBe(historicalCommitId);
    expect(headNode.attributes('data-depth')).toBe('2');
    expect(siblingNode.attributes('data-depth')).toBe('2');
    expect(headNode.classes()).toContain('commit-node--head');
    expect(headNode.text()).toContain('默认 HEAD');
    expect(headNode.text()).toContain('归档：分块');
    expect(siblingNode.text()).not.toContain('默认 HEAD');
    expect(siblingNode.text()).toContain('归档：全文件');
    expect(tree.findAll('.commit-node--tip')).toHaveLength(2);

    wrapper.unmount();
    queryClient.clear();
  });

  it('navigates from a selected Commit node to the dedicated detail route', async () => {
    const { queryClient, router, wrapper } = await mountPage(
      `/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits`,
      artifact,
      ['artifact_catalog', 'artifact_commit_graph', 'artifact_commit_diff'],
    );

    await wrapper.find(`[data-commit-id="${headCommitId}"]`).find('button').trigger('click');
    await flushPromises();

    expect(router.currentRoute.value.name).toBe('commit-detail');
    expect(router.currentRoute.value.path).toBe(
      `/tenants/tenant-a/projects/project-a/artifacts/artifact-a/commits/${headCommitId}`,
    );
    expect(router.currentRoute.value.params).toMatchObject({
      tenantId: 'tenant-a',
      projectId: 'project-a',
      artifactId: 'artifact-a',
      commitId: headCommitId,
    });

    wrapper.unmount();
    queryClient.clear();
  });

  it('refreshes the Artifact and every enabled relation from one action', async () => {
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      artifact,
      [
        'artifact_catalog',
        'artifact_commit_graph',
        'workspace_materialize',
        'commit_materialization_v2',
      ],
    );

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '刷新')!
      .trigger('click');
    await flushPromises();

    expect(api.queryArtifact).toHaveBeenCalledTimes(2);
    expect(api.queryArtifactCommitGraph).toHaveBeenCalledTimes(2);
    expect(api.queryWorkspaceList).toHaveBeenCalledTimes(2);
    expect(api.querySnapshotList).toHaveBeenCalledTimes(2);

    wrapper.unmount();
    queryClient.clear();
  });

  it('ignores a stale Commit page after navigating to another Artifact', async () => {
    const stalePage = deferred<{
      data: { graph: CommitGraphView };
      requestId: string;
    }>();
    const firstGraph: CommitGraphView = {
      graph_version: '2',
      head_commit_id: headCommitId,
      nodes: [
        {
          commit_id: headCommitId,
          message: 'Artifact A head',
          tag_names: [],
          data_layout: 'fast_cdc',
          created_at_unix_ms: '2',
        },
      ],
      next_cursor: 'artifact-a-page-2',
    };
    const { queryClient, router, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits',
      artifact,
      ['artifact_catalog', 'artifact_commit_graph'],
      firstGraph,
    );
    const artifactBCommitId = 'e'.repeat(64);
    const staleCommitId = 'f'.repeat(64);
    api.queryArtifactCommitGraph.mockImplementation(
      (_tenantId: string, _projectId: string, requestedArtifactId: string, cursor?: string) => {
        if (cursor === 'artifact-a-page-2') return stalePage.promise;
        if (requestedArtifactId === 'artifact-b') {
          return Promise.resolve({
            data: {
              graph: {
                graph_version: '1',
                head_commit_id: artifactBCommitId,
                nodes: [
                  {
                    commit_id: artifactBCommitId,
                    message: 'Artifact B head',
                    tag_names: [],
                    data_layout: 'whole_file',
                    created_at_unix_ms: '3',
                  },
                ],
              },
            },
            requestId: 'request-artifact-b-commits',
          });
        }
        throw new Error('unexpected Commit graph request');
      },
    );

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '加载更多历史')!
      .trigger('click');
    await router.push('/tenants/tenant-a/projects/project-a/artifacts/artifact-b?tab=commits');
    await flushPromises();
    stalePage.resolve({
      data: {
        graph: {
          graph_version: '2',
          head_commit_id: headCommitId,
          nodes: [
            {
              commit_id: staleCommitId,
              message: 'Stale Artifact A history',
              tag_names: [],
              data_layout: 'fast_cdc',
              created_at_unix_ms: '1',
            },
          ],
        },
      },
      requestId: 'request-stale-page',
    });
    await flushPromises();

    expect(wrapper.find(`[data-commit-id="${artifactBCommitId}"]`).exists()).toBe(true);
    expect(wrapper.find(`[data-commit-id="${staleCommitId}"]`).exists()).toBe(false);

    wrapper.unmount();
    queryClient.clear();
  });
});
