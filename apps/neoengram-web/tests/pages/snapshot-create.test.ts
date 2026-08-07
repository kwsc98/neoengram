import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount, type VueWrapper } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import PageCursor from '@/components/PageCursor.vue';
import SnapshotCreatePage from '@/pages/SnapshotCreatePage.vue';

const api = vi.hoisted(() => ({
  createSnapshot: vi.fn(),
  queryApiVersion: vi.fn(),
  queryArtifact: vi.fn(),
  querySnapshot: vi.fn(),
  queryStorageVolumeList: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const ElButtonStub = {
  emits: ['click'],
  template: '<button type="button" @click="$emit(\'click\')"><slot /></button>',
};
const ElTagStub = { template: '<span><slot /></span>' };
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

const readyVolume = {
  tenant_id: 'tenant-a',
  storage_volume_id: 'volume-ready',
  display_name: 'Ready volume',
  edge_cluster_id: 'cluster-a',
  region: 'cn-shanghai',
  backend_type: 'pvc' as const,
  access_mode: 'read_only_many' as const,
  state: 'ready' as const,
  resource_version: '1',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
};

const degradedVolume = {
  ...readyVolume,
  storage_volume_id: 'volume-degraded',
  display_name: 'Degraded volume',
  state: 'degraded' as const,
};

const snapshot = {
  snapshot_id: 'snapshot-a',
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'artifact-a',
  commit_id: historicalCommitId,
  storage_volume_id: 'volume-ready',
  region: 'cn-shanghai',
  message: 'Historical baseline',
  tag_names: [],
  state: 'ready' as const,
  phase: 'idle' as const,
  integrity: { state: 'verified' as const, files_verified: '3', bytes_verified: '30' },
  logical_file_count: '3',
  logical_size_bytes: '30',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
};

function mockBaseQueries(): void {
  api.queryApiVersion.mockResolvedValue({
    data: {
      api_versions: [1],
      agent_protocol_versions: [1],
      capabilities: ['artifact_catalog', 'artifact_commit_graph', 'snapshot_materialize'],
    },
    requestId: 'request-version',
  });
  api.queryArtifact.mockResolvedValue({ data: { artifact }, requestId: 'request-artifact' });
  api.queryStorageVolumeList.mockResolvedValue({
    data: { items: [degradedVolume, readyVolume] },
    requestId: 'request-volumes',
  });
  api.createSnapshot.mockResolvedValue({
    data: { snapshot, replayed: true, placement_reused: true },
    requestId: 'request-create',
  });
  api.querySnapshot.mockResolvedValue({ data: { snapshot }, requestId: 'request-snapshot' });
}

async function mountPage() {
  mockBaseQueries();
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
      stubs: { ElButton: ElButtonStub, ElTag: ElTagStub },
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
  it('uses ArtifactCommitSelect and submits the exact historical Commit with a Ready Volume', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryArtifact).toHaveBeenCalledWith('tenant-a', 'project-a', 'artifact-a');
    const commitSelect = wrapper.findComponent(ArtifactCommitSelect);
    expect(commitSelect.props()).toMatchObject({
      tenantId: 'tenant-a',
      projectId: 'project-a',
      artifactId: 'artifact-a',
      headCommitId,
      modelValue: historicalCommitId,
      allowHistory: true,
    });

    await elementButton(wrapper, '选择 StorageVolume').trigger('click');
    await flushPromises();
    expect(wrapper.text()).toContain('Ready volume');
    expect(wrapper.text()).toContain('Degraded volume');
    expect(
      wrapper
        .findAll('.snapshot-volume-list > button')
        .find((item) => item.text().includes('Degraded volume'))
        ?.attributes('disabled'),
    ).toBeDefined();

    await wrapper.find('.snapshot-volume-list > button:not([disabled])').trigger('click');
    api.createSnapshot
      .mockRejectedValueOnce(new TypeError('transport interrupted'))
      .mockResolvedValueOnce({
        data: { snapshot, replayed: true, placement_reused: true },
        requestId: 'request-create-retry',
      });
    await elementButton(wrapper, '创建 Snapshot').trigger('click');
    await flushPromises();
    await elementButton(wrapper, '创建 Snapshot').trigger('click');
    await flushPromises();

    expect(api.createSnapshot).toHaveBeenCalledTimes(2);
    const requests = (api.createSnapshot.mock.calls as unknown[][]).map(
      ([request]) => request as Record<string, unknown>,
    );
    expect(requests[0]).toMatchObject({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      commit_id: historicalCommitId,
      storage_volume_id: 'volume-ready',
    });
    expect(requests[0]?.snapshot_request_id).toMatch(/^snapshot-request-/);
    expect(requests[1]).toEqual(requests[0]);
    expect(api.querySnapshot).toHaveBeenCalledWith('tenant-a', 'snapshot-a');
    expect(wrapper.text()).toContain('幂等重放');
    expect(wrapper.text()).toContain('复用现有放置');

    wrapper.unmount();
    queryClient.clear();
  });

  it('forwards the StorageVolume cursor and keeps unavailable placements disabled', async () => {
    const { wrapper, queryClient } = await mountPage();
    api.queryStorageVolumeList
      .mockResolvedValueOnce({
        data: { items: [degradedVolume], next_cursor: 'volume-page-2' },
        requestId: 'request-volume-page-1',
      })
      .mockResolvedValueOnce({
        data: { items: [readyVolume] },
        requestId: 'request-volume-page-2',
      });

    await elementButton(wrapper, '选择 StorageVolume').trigger('click');
    await flushPromises();
    expect(wrapper.find('.snapshot-volume-list > button').attributes('disabled')).toBeDefined();
    wrapper.findComponent(PageCursor).vm.$emit('next');
    await flushPromises();

    expect(api.queryStorageVolumeList).toHaveBeenLastCalledWith({
      tenant_id: 'tenant-a',
      page_size: 20,
      cursor: 'volume-page-2',
    });
    expect(wrapper.text()).toContain('Ready volume');

    wrapper.unmount();
    queryClient.clear();
  });

  it('ignores duplicate create clicks while the first request is pending', async () => {
    const { wrapper, queryClient } = await mountPage();
    await elementButton(wrapper, '选择 StorageVolume').trigger('click');
    await flushPromises();
    await wrapper.find('.snapshot-volume-list > button:not([disabled])').trigger('click');

    const pendingResult = {
      data: { snapshot, replayed: false, placement_reused: false },
      requestId: 'request-create-pending',
    };
    let resolveCreate!: (value: typeof pendingResult) => void;
    api.createSnapshot.mockImplementationOnce(
      () =>
        new Promise<typeof pendingResult>((resolve) => {
          resolveCreate = resolve;
        }),
    );

    const createButton = elementButton(wrapper, '创建 Snapshot');
    await createButton.trigger('click');
    await createButton.trigger('click');
    expect(api.createSnapshot).toHaveBeenCalledTimes(1);

    resolveCreate(pendingResult);
    await flushPromises();
    wrapper.unmount();
    queryClient.clear();
  });
});
