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
import PlaygroundListPage from '@/pages/PlaygroundListPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  createPlayground: vi.fn(),
  queryApiVersion: vi.fn(),
  queryArtifactCommitGraph: vi.fn(),
  queryArtifactList: vi.fn(),
  queryPlaygroundList: vi.fn(),
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
  created_at_unix_ms: '1',
  updated_at_unix_ms: '1',
};

async function mountPage() {
  api.queryApiVersion.mockResolvedValue({
    data: {
      service: 'neoengram-server',
      version: '0.2.0',
      git_commit: 'test',
      api_versions: [1],
      agent_protocol_versions: [1],
      capabilities: ['artifact_catalog', 'artifact_commit_graph', 'playground_materialize'],
    },
    requestId: 'request-version',
  });
  api.queryPlaygroundList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-playgrounds',
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
            created_at_unix_ms: '2',
          },
          {
            commit_id: historicalCommitId,
            message: 'Historical baseline',
            tag_names: [],
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

  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      permissions: ['playground.read', 'playground.create'],
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '1',
    },
  ];
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/playgrounds',
        name: 'playground-list',
        component: PlaygroundListPage,
      },
      { path: '/playground', name: 'playground-detail', component: { template: '<div />' } },
    ],
  });
  await router.push('/tenants/tenant-a/playgrounds');
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(PlaygroundListPage, {
    global: { plugins: [ElementPlus, pinia, [VueQueryPlugin, { queryClient }], router] },
  });
  await flushPromises();
  return { queryClient, wrapper };
}

afterEach(() => vi.clearAllMocks());

describe('Playground list creation', () => {
  it('defaults to Artifact Head and submits a selected historical Commit', async () => {
    const { queryClient, wrapper } = await mountPage();

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '创建 Playground')!
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
      .filter((button) => button.text() === '创建 Playground')
      .at(-1)!
      .trigger('click');
    await flushPromises();

    expect(api.createPlayground.mock.calls[0]?.[0]).toEqual({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      playground_id: 'historical-review',
      display_name: 'Historical review',
      storage_volume_id: 'volume-a',
      base_commit_id: historicalCommitId,
    });

    wrapper.unmount();
    queryClient.clear();
  });
});
