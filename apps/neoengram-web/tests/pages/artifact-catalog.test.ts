import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type {
  ArtifactView,
  CommitGraphView,
  CreateCommitReplicationRequest,
  TenantView,
} from '@/api/types';
import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import PageHeading from '@/components/PageHeading.vue';
import StorageVolumeFilter from '@/components/StorageVolumeFilter.vue';
import ArtifactDetailPage from '@/pages/ArtifactDetailPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  cancelCommitReplication: vi.fn(),
  createPlayground: vi.fn(),
  queryApiVersion: vi.fn(),
  queryArtifact: vi.fn(),
  queryArtifactCommitDiff: vi.fn(),
  queryArtifactCommitGraph: vi.fn(),
  queryCommitPlacementList: vi.fn(),
  queryCommitReplicationList: vi.fn(),
  queryGatewayPoolList: vi.fn(),
  queryPlaygroundList: vi.fn(),
  querySnapshotList: vi.fn(),
  queryStorageVolumeList: vi.fn(),
  replicateCommit: vi.fn(),
  retryCommitReplication: vi.fn(),
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
  permissions: TenantView['permissions'] = [
    'artifact.read',
    'playground.create',
    'snapshot.create',
  ],
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
  api.queryPlaygroundList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-playgrounds',
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
  api.queryGatewayPoolList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-gateway-pools',
  });
  api.queryCommitReplicationList.mockResolvedValue({
    data: { replications: [] },
    requestId: 'request-replications',
  });
  api.queryCommitPlacementList.mockResolvedValue({
    data: { placements: [] },
    requestId: 'request-placements',
  });
  api.replicateCommit.mockResolvedValue({
    data: {
      replication: {
        replication_id: 'replication-a',
        tenant_id: 'tenant-a',
        artifact_id: 'artifact-a',
        commit_id: headCommitId,
        target_storage_volume_id: 'volume-a',
        attempt: '1',
        state: 'queued',
        object_set_digest: 'd'.repeat(64),
        completed_objects: '0',
        total_objects: '3',
        completed_bytes: '0',
        total_bytes: '30',
      },
      replayed: false,
    },
    requestId: 'request-replicate',
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
      { path: '/playground', name: 'playground-detail', component: { template: '<div />' } },
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
  it('loads the authoritative Artifact and Playground relation without advanced APIs', async () => {
    const { queryClient, wrapper } = await mountPage();

    expect(api.queryArtifact).toHaveBeenCalledWith('tenant-a', 'project-a', 'artifact-a');
    expect(api.queryPlaygroundList).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      page_size: 100,
    });
    expect(api.queryArtifactCommitGraph).not.toHaveBeenCalled();
    expect(api.queryArtifactCommitDiff).not.toHaveBeenCalled();
    expect(api.querySnapshotList).not.toHaveBeenCalled();
    expect(wrapper.findComponent(PageHeading).props('title')).toBe('Authoritative data');
    expect(wrapper.findAll('button').some((button) => button.text() === '创建 Playground')).toBe(
      false,
    );

    wrapper.unmount();
    queryClient.clear();
  });

  it('allows Playground creation when the server advertises materialization', async () => {
    const emptyArtifact: ArtifactView = { ...artifact };
    delete emptyArtifact.head_commit_id;
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      emptyArtifact,
      ['artifact_catalog', 'playground_materialize'],
    );

    expect(wrapper.findAll('button').some((button) => button.text() === '创建 Playground')).toBe(
      true,
    );

    wrapper.unmount();
    queryClient.clear();
  });

  it('keeps non-empty Playground derivation available with explicit capabilities', async () => {
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      artifact,
      ['artifact_catalog', 'artifact_commit_graph', 'playground_materialize'],
    );

    expect(wrapper.findAll('button').some((button) => button.text() === '创建 Playground')).toBe(
      true,
    );

    wrapper.unmount();
    queryClient.clear();
  });

  it('exposes Snapshot creation with the precise materialization capability', async () => {
    const { queryClient, router, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      artifact,
      ['artifact_catalog', 'snapshot_materialize'],
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

  it('submits the historical Commit selected while creating a Playground', async () => {
    api.createPlayground.mockResolvedValue({
      data: {
        playground: {
          tenant_id: 'tenant-a',
          project_id: 'project-a',
          artifact_id: 'artifact-a',
          playground_id: 'historical-review',
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
        replayed: false,
      },
      requestId: 'request-create-playground',
    });
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      artifact,
      ['artifact_catalog', 'playground_materialize', 'artifact_commit_graph'],
    );

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '创建 Playground')!
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
      .filter((button) => button.text() === '创建 Playground')
      .at(-1)!
      .trigger('click');
    await flushPromises();

    expect(api.createPlayground.mock.calls[0]?.[0]).toEqual({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      playground_id: 'historical-review',
      storage_volume_id: 'volume-a',
      display_name: 'Historical review',
      base_commit_id: historicalCommitId,
    });

    wrapper.unmount();
    queryClient.clear();
  });

  it('ignores Commit deep links when the resource browser is unavailable', async () => {
    const { queryClient, wrapper } = await mountPage(
      `/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits&commit_id=${headCommitId}`,
    );

    expect(api.queryArtifactCommitGraph).not.toHaveBeenCalled();
    expect(api.queryArtifactCommitDiff).not.toHaveBeenCalled();
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

  it('loads Commit details from the selected node and follows its parent', async () => {
    const target = {
      commit_id: headCommitId,
      parent_commit_id: historicalCommitId,
      message: 'Current head',
      description: 'Current head description',
      tag_names: ['release-candidate'],
      data_layout: 'fast_cdc' as const,
      created_at_unix_ms: '2',
    };
    const parent = {
      commit_id: historicalCommitId,
      message: 'Historical baseline',
      description: 'Historical baseline description',
      tag_names: ['v1.0'],
      data_layout: 'fast_cdc' as const,
      created_at_unix_ms: '1',
    };
    api.queryArtifactCommitDiff.mockImplementation(
      (_tenantId: string, _projectId: string, _artifactId: string, commitId: string) => ({
        data: {
          diff: {
            ...(commitId === target.commit_id ? { base_commit: parent } : {}),
            target_commit: commitId === target.commit_id ? target : parent,
            summary: {
              files_added: '0',
              files_modified: '0',
              files_deleted: '0',
              files_renamed: '0',
              bytes_added: '0',
              bytes_removed: '0',
            },
            changes: [],
          },
        },
        requestId: `request-diff-${commitId}`,
      }),
    );
    const { queryClient, router, wrapper } = await mountPage(
      `/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits`,
      artifact,
      ['artifact_catalog', 'artifact_commit_graph', 'artifact_commit_diff'],
    );

    await wrapper.find(`[data-commit-id="${headCommitId}"]`).find('button').trigger('click');
    await flushPromises();

    expect(api.queryArtifactCommitDiff).toHaveBeenLastCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
      headCommitId,
    );
    expect(wrapper.text()).toContain('Current head description');
    expect(wrapper.text()).toContain('与基线没有文件变化');

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '查看父 Commit')!
      .trigger('click');
    await flushPromises();
    expect(router.currentRoute.value.query.commit_id).toBe(historicalCommitId);
    expect(api.queryArtifactCommitDiff).toHaveBeenLastCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
      historicalCommitId,
    );
    expect(wrapper.text()).toContain('Historical baseline description');

    wrapper.unmount();
    queryClient.clear();
  });

  it('opens the Commit replication drawer without Diff capability and submits a scoped request', async () => {
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits',
      artifact,
      ['artifact_catalog', 'artifact_commit_graph', 'artifact_commit_replication'],
      undefined,
      ['artifact.read', 'artifact.commit.replicate'],
    );

    await wrapper.find(`[data-commit-id="${headCommitId}"]`).find('button').trigger('click');
    await flushPromises();

    expect(api.queryArtifactCommitDiff).not.toHaveBeenCalled();
    expect(api.queryCommitReplicationList).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      commit_id: headCommitId,
      object_namespace_id: 'artifact-a',
    });
    expect(api.queryCommitPlacementList).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      commit_id: headCommitId,
      object_namespace_id: 'artifact-a',
    });
    expect(api.queryGatewayPoolList).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain('路由由 Central 校验');

    await wrapper
      .findAll('button')
      .find((button) => button.text().trim() === '复制 Commit')!
      .trigger('click');
    await flushPromises();

    const request = api.replicateCommit.mock.calls[0]?.[0] as
      CreateCommitReplicationRequest | undefined;
    expect(request).toMatchObject({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      commit_id: headCommitId,
      target_storage_volume_id: 'volume-a',
    });
    expect(request?.request_id).toMatch(/^commit-replicate-[a-f0-9]{32}$/);
    expect(request?.request_id.length).toBeLessThanOrEqual(128);

    wrapper.unmount();
    queryClient.clear();
  });

  it('refreshes live StorageVolume availability while the replication drawer is open', async () => {
    vi.useFakeTimers();
    const mounted = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits',
      artifact,
      ['artifact_catalog', 'artifact_commit_graph', 'artifact_commit_replication'],
      undefined,
      ['artifact.read', 'artifact.commit.replicate'],
    );
    try {
      const { wrapper } = mounted;

      await wrapper.find(`[data-commit-id="${headCommitId}"]`).find('button').trigger('click');
      await flushPromises();
      expect(api.queryStorageVolumeList).toHaveBeenCalledTimes(1);

      api.queryStorageVolumeList.mockResolvedValueOnce({
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
              state: 'unavailable',
              resource_version: '2',
              lifecycle: { state: 'active', generation: '1', resource_version: '2' },
              created_at_unix_ms: '1',
              updated_at_unix_ms: '2',
            },
          ],
        },
        requestId: 'request-volumes-unavailable',
      });
      await vi.advanceTimersByTimeAsync(5_000);
      await flushPromises();

      expect(api.queryStorageVolumeList).toHaveBeenCalledTimes(2);
      expect(
        wrapper
          .findAll('button')
          .find((button) => button.text().trim() === '复制 Commit')
          ?.attributes('disabled'),
      ).toBeDefined();
    } finally {
      mounted.wrapper.unmount();
      mounted.queryClient.clear();
      vi.useRealTimers();
    }
  });

  it('restores an active task for the target and does not create a duplicate', async () => {
    api.queryCommitReplicationList.mockResolvedValueOnce({
      data: {
        replications: [
          {
            replication_id: 'replication-existing',
            tenant_id: 'tenant-a',
            artifact_id: 'artifact-a',
            commit_id: headCommitId,
            target_storage_volume_id: 'volume-a',
            attempt: '1',
            state: 'transferring',
            object_set_digest: 'd'.repeat(64),
            completed_objects: '1',
            total_objects: '3',
            completed_bytes: '10',
            total_bytes: '30',
          },
        ],
      },
      requestId: 'request-existing-replication',
    });
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits',
      artifact,
      ['artifact_catalog', 'artifact_commit_graph', 'artifact_commit_replication'],
      undefined,
      ['artifact.read', 'artifact.commit.replicate'],
    );

    await wrapper.find(`[data-commit-id="${headCommitId}"]`).find('button').trigger('click');
    await flushPromises();

    expect(wrapper.text()).toContain('replication-existing');
    const action = wrapper
      .findAll('button')
      .find((button) => button.text().trim() === '复制进行中');
    expect(action?.attributes('disabled')).toBeDefined();
    await action!.trigger('click');
    expect(api.replicateCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('disables a target below a known unhealthy GatewayPool', async () => {
    api.queryGatewayPoolList.mockResolvedValueOnce({
      data: {
        items: [
          {
            gateway_pool_id: 'pool-a',
            edge_cluster_id: 'edge-a',
            display_name: 'Gateway A',
            agent_endpoint: 'https://gateway.example.test',
            desired_replicas: 1,
            minimum_ready_replicas: 1,
            state: 'draining',
            config_generation: '1',
            resource_version: '1',
            created_at_unix_ms: '1',
            updated_at_unix_ms: '1',
          },
        ],
      },
      requestId: 'request-gateway-pools',
    });
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a?tab=commits',
      artifact,
      ['artifact_catalog', 'artifact_commit_graph', 'artifact_commit_replication'],
      undefined,
      ['artifact.read', 'artifact.commit.replicate', 'gateway.read'],
    );

    await wrapper.find(`[data-commit-id="${headCommitId}"]`).find('button').trigger('click');
    await flushPromises();

    expect(api.queryGatewayPoolList).toHaveBeenCalledWith({});
    expect(wrapper.text()).toContain('路由不可用');
    expect(
      wrapper
        .findAll('button')
        .find((button) => button.text().trim() === '复制 Commit')
        ?.attributes('disabled'),
    ).toBeDefined();

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
        'playground_materialize',
        'snapshot_materialize',
      ],
    );

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '刷新')!
      .trigger('click');
    await flushPromises();

    expect(api.queryArtifact).toHaveBeenCalledTimes(2);
    expect(api.queryArtifactCommitGraph).toHaveBeenCalledTimes(2);
    expect(api.queryPlaygroundList).toHaveBeenCalledTimes(2);
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
