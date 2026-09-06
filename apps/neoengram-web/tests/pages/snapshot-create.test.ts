import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount, type VueWrapper } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import SnapshotCreatePage from '@/pages/SnapshotCreatePage.vue';

const api = vi.hoisted(() => ({
  createSnapshot: vi.fn(),
  queryApiVersion: vi.fn(),
  queryArtifact: vi.fn(),
  queryArtifactCommitGraph: vi.fn(),
  querySnapshot: vi.fn(),
  queryStorageVolumeList: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const ElButtonStub = {
  emits: ['click'],
  template: '<button type="button" @click="$emit(\'click\')"><slot /></button>',
};
const ElSelectStub = {
  inheritAttrs: false,
  props: ['modelValue', 'id'],
  emits: ['update:modelValue'],
  template:
    '<select :id="id" :value="modelValue" v-bind="$attrs" @change="$emit(\'update:modelValue\', $event.target.value)"><slot /></select>',
};
type SelectStubVm = { $emit: (event: 'update:modelValue', value: string) => void };
const headCommitId = 'a'.repeat(64);
const historicalCommitId = 'b'.repeat(64);
const artifact = {
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'artifact-a',
  display_name: 'Authoritative artifact',
  initialization: { mode: 'empty' as const },
  head_commit_id: headCommitId,
  resource_version: '4',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
};
const snapshot = {
  snapshot_id: 'snapshot-a',
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'artifact-a',
  commit_id: historicalCommitId,
  delivery_id: 'delivery-a',
  edge_cluster_id: 'edge-a',
  storage_volume_id: 'volume-a',
  delivery_mode: 'copy' as const,
  data_layout: 'fast_cdc' as const,
  message: 'Historical baseline',
  tag_names: [],
  state: 'ready' as const,
  data_health: 'available' as const,
  integrity: { state: 'verified' as const, files_verified: '3', bytes_verified: '30' },
  logical_file_count: '3',
  logical_size_bytes: '30',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
};

function mockBaseQueries(): void {
  api.queryApiVersion.mockResolvedValue({
    data: {
      api_version: 1,
      agent_wire_version: 1,
      capabilities: ['artifact_commit_graph', 'snapshot_delivery_copy_v2'],
    },
    requestId: 'request-version',
  });
  api.queryArtifact.mockResolvedValue({ data: { artifact }, requestId: 'request-artifact' });
  api.queryArtifactCommitGraph.mockResolvedValue({
    data: {
      graph: {
        graph_version: '1',
        head_commit_id: headCommitId,
        nodes: [
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
    requestId: 'request-commit-graph',
  });
  api.createSnapshot.mockResolvedValue({
    data: { snapshot, request_replayed: true },
    requestId: 'request-create',
  });
  api.querySnapshot.mockResolvedValue({ data: { snapshot }, requestId: 'request-snapshot' });
  api.queryStorageVolumeList.mockResolvedValue({
    data: {
      items: [
        {
          tenant_id: 'tenant-a',
          storage_volume_id: 'volume-a',
          display_name: 'Volume A',
          edge_cluster_id: 'edge-a',
          region: 'cn-shanghai',
          backend_type: 'nfs',
          access_mode: 'read_write_many',
          allowed_delivery_modes: ['copy'],
          hardlink_policy: 'disabled',
          max_whole_file_bytes: '1073741824',
          copy_reserve_bytes: '1024',
          state: 'ready',
          resource_version: '1',
          lifecycle: { state: 'active', generation: '1', resource_version: '1' },
          created_at_unix_ms: '1',
          updated_at_unix_ms: '2',
        },
      ],
    },
    requestId: 'request-volume-list',
  });
}

async function mountPage(
  volumeItems?: unknown[],
  volumeResponses?: Array<{
    data: { items: unknown[]; next_cursor?: string };
    requestId: string;
  }>,
) {
  mockBaseQueries();
  if (volumeResponses) {
    api.queryStorageVolumeList.mockReset();
    for (const response of volumeResponses) {
      api.queryStorageVolumeList.mockResolvedValueOnce(response);
    }
  } else if (volumeItems) {
    api.queryStorageVolumeList.mockResolvedValue({
      data: { items: volumeItems },
      requestId: 'request-volume-list',
    });
  }
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/new',
        name: 'snapshot-create',
        component: SnapshotCreatePage,
      },
      { path: '/artifact', name: 'artifact-detail', component: { template: '<div />' } },
      {
        path: '/snapshot/:snapshotId',
        name: 'snapshot-detail',
        component: { template: '<div />' },
      },
    ],
  });
  await router.push(
    `/tenants/tenant-a/projects/project-a/artifacts/artifact-a/snapshots/new?commit_id=${historicalCommitId}`,
  );
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = shallowMount(SnapshotCreatePage, {
    global: {
      plugins: [ElementPlus, [VueQueryPlugin, { queryClient }], router],
      stubs: { ElButton: ElButtonStub, ElSelect: ElSelectStub },
    },
  });
  await flushPromises();
  return { wrapper, queryClient };
}

function elementButton(wrapper: VueWrapper, label: string) {
  const button = wrapper.findAll('button').find((item) => item.text().trim() === label);
  if (!button) throw new Error(`Missing button: ${label}`);
  return button;
}

afterEach(() => vi.clearAllMocks());

describe('Snapshot create page', () => {
  it('creates a Snapshot with its immutable delivery target and mode', async () => {
    const { wrapper, queryClient } = await mountPage();
    const commitSelect = wrapper.findComponent(ArtifactCommitSelect);
    expect(commitSelect.props()).toMatchObject({
      tenantId: 'tenant-a',
      projectId: 'project-a',
      artifactId: 'artifact-a',
      headCommitId,
      modelValue: historicalCommitId,
      allowHistory: true,
    });
    api.createSnapshot
      .mockRejectedValueOnce(new TypeError('transport interrupted'))
      .mockResolvedValueOnce({ data: { snapshot, request_replayed: true }, requestId: 'retry' });
    await elementButton(wrapper, '创建 Snapshot').trigger('click');
    await flushPromises();
    await elementButton(wrapper, '创建 Snapshot').trigger('click');
    await flushPromises();
    expect(api.createSnapshot).toHaveBeenCalledTimes(2);
    const first = api.createSnapshot.mock.calls[0]?.[0] as Record<string, unknown>;
    const second = api.createSnapshot.mock.calls[1]?.[0] as Record<string, unknown>;
    expect(first).toMatchObject({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      commit_id: historicalCommitId,
      target_edge_cluster_id: 'edge-a',
      target_storage_volume_id: 'volume-a',
      delivery_mode: 'copy',
    });
    expect(first.request_id).toMatch(/^snapshot-request-/);
    expect(second).toEqual(first);
    expect(wrapper.text()).toContain('Snapshot 已绑定一个目标 Volume 和唯一只读交付');
    expect(wrapper.text()).toContain('delivery-a');
    wrapper.unmount();
    queryClient.clear();
  });

  it('ignores duplicate clicks while Snapshot creation is pending', async () => {
    const { wrapper, queryClient } = await mountPage();
    let resolveCreate!: (value: unknown) => void;
    api.createSnapshot.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveCreate = resolve;
        }),
    );
    const button = elementButton(wrapper, '创建 Snapshot');
    await button.trigger('click');
    await button.trigger('click');
    expect(api.createSnapshot).toHaveBeenCalledTimes(1);
    resolveCreate({ data: { snapshot, request_replayed: false }, requestId: 'pending' });
    await flushPromises();
    wrapper.unmount();
    queryClient.clear();
  });

  it('filters the disk choices after explicitly changing the target cluster', async () => {
    const volumeA = {
      tenant_id: 'tenant-a',
      storage_volume_id: 'volume-a',
      display_name: 'Volume A',
      edge_cluster_id: 'edge-a',
      region: 'cn-shanghai',
      backend_type: 'nfs',
      access_mode: 'read_write_many',
      allowed_delivery_modes: ['copy'],
      hardlink_policy: 'disabled',
      max_whole_file_bytes: '1073741824',
      copy_reserve_bytes: '1024',
      state: 'ready',
      resource_version: '1',
      lifecycle: { state: 'active', generation: '1', resource_version: '1' },
      created_at_unix_ms: '1',
      updated_at_unix_ms: '2',
    };
    const volumeB = {
      ...volumeA,
      storage_volume_id: 'volume-b',
      display_name: 'Volume B',
      edge_cluster_id: 'edge-b',
    };
    const { wrapper, queryClient } = await mountPage([volumeA, volumeB]);
    const clusterSelect = wrapper.findComponent(ElSelectStub);
    expect(clusterSelect.exists()).toBe(true);
    (clusterSelect.vm as unknown as SelectStubVm).$emit('update:modelValue', 'edge-b');
    await flushPromises();
    expect(wrapper.text()).toContain('edge-b');
    expect(wrapper.text()).toContain('volume-b');
    expect(wrapper.text()).not.toContain('volume-a');
    wrapper.unmount();
    queryClient.clear();
  });

  it('loads every target volume page before deriving the cluster choices', async () => {
    const firstVolume = {
      tenant_id: 'tenant-a',
      storage_volume_id: 'volume-a',
      display_name: 'Volume A',
      edge_cluster_id: 'edge-a',
      region: 'cn-shanghai',
      backend_type: 'nfs',
      access_mode: 'read_write_many',
      allowed_delivery_modes: ['copy'],
      hardlink_policy: 'disabled',
      max_whole_file_bytes: '1073741824',
      copy_reserve_bytes: '1024',
      state: 'ready',
      resource_version: '1',
      lifecycle: { state: 'active', generation: '1', resource_version: '1' },
      created_at_unix_ms: '1',
      updated_at_unix_ms: '2',
    };
    const secondVolume = {
      ...firstVolume,
      storage_volume_id: 'volume-b',
      display_name: 'Volume B',
      edge_cluster_id: 'edge-b',
    };
    const { wrapper, queryClient } = await mountPage(undefined, [
      {
        data: { items: [firstVolume], next_cursor: 'volume-page-2' },
        requestId: 'request-volume-page-1',
      },
      {
        data: { items: [secondVolume] },
        requestId: 'request-volume-page-2',
      },
    ]);
    expect(api.queryStorageVolumeList).toHaveBeenNthCalledWith(1, {
      tenant_id: 'tenant-a',
      page_size: 100,
    });
    expect(api.queryStorageVolumeList).toHaveBeenNthCalledWith(2, {
      tenant_id: 'tenant-a',
      page_size: 100,
      cursor: 'volume-page-2',
    });
    expect(wrapper.text()).toContain('edge-a');
    expect(wrapper.text()).toContain('volume-a');
    (wrapper.findComponent(ElSelectStub).vm as unknown as SelectStubVm).$emit(
      'update:modelValue',
      'edge-b',
    );
    await flushPromises();
    expect(wrapper.text()).toContain('volume-b');
    wrapper.unmount();
    queryClient.clear();
  });

  it('fails closed when the volume API repeats a pagination cursor', async () => {
    const { wrapper, queryClient } = await mountPage(undefined, [
      {
        data: { items: [], next_cursor: 'same-page' },
        requestId: 'request-volume-page-1',
      },
      {
        data: { items: [], next_cursor: 'same-page' },
        requestId: 'request-volume-page-2',
      },
    ]);
    await flushPromises();
    expect(wrapper.findComponent(ApiProblemAlert).props('error')).toMatchObject({
      message: 'StorageVolume 分页游标重复，无法安全加载目标列表',
    });
    expect(api.queryStorageVolumeList).toHaveBeenCalledTimes(2);
    wrapper.unmount();
    queryClient.clear();
  });
});
