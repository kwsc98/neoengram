import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { ArtifactView } from '@/api/types';
import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import PageHeading from '@/components/PageHeading.vue';
import StorageVolumeFilter from '@/components/StorageVolumeFilter.vue';
import ArtifactDetailPage from '@/pages/ArtifactDetailPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  createPlayground: vi.fn(),
  queryApiVersion: vi.fn(),
  queryArtifact: vi.fn(),
  queryArtifactCommitDiff: vi.fn(),
  queryArtifactCommitGraph: vi.fn(),
  queryPlaygroundList: vi.fn(),
  querySnapshotList: vi.fn(),
  queryStorageVolumeList: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const headCommitId = 'a'.repeat(64);
const historicalCommitId = 'b'.repeat(64);

const artifact = {
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'artifact-a',
  display_name: 'Authoritative data',
  initialization: { mode: 'empty' as const },
  head_commit_id: headCommitId,
  resource_version: '3',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
};

async function mountPage(
  location = '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
  artifactView: ArtifactView = artifact,
  capabilities = ['artifact_catalog'],
) {
  api.queryApiVersion.mockResolvedValue({
    data: {
      service: 'neoengram-server',
      version: '0.2.0',
      git_commit: 'test',
      api_versions: [1],
      agent_protocol_versions: [1],
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
      graph: {
        graph_version: '1',
        head_commit_id: artifactView.head_commit_id,
        nodes: artifactView.head_commit_id
          ? [
              {
                commit_id: artifactView.head_commit_id,
                parent_commit_id: historicalCommitId,
                message: 'Current head',
                tag_names: [],
                created_at_unix_ms: '2',
              },
              {
                commit_id: historicalCommitId,
                message: 'Historical baseline',
                tag_names: [],
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
          backend_type: 'pvc',
          access_mode: 'read_write_once',
          region: 'cn-shanghai',
          state: 'ready',
          resource_version: '1',
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
      permissions: ['artifact.read', 'playground.create', 'snapshot.create'],
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

  it('keeps non-empty Playground derivation available to the full resource browser', async () => {
    const { queryClient, wrapper } = await mountPage(
      '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
      artifact,
      ['resource_browser'],
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
});
