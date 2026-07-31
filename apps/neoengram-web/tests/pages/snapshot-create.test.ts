import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount, type VueWrapper } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import PageCursor from '@/components/PageCursor.vue';
import SnapshotCreatePage from '@/pages/SnapshotCreatePage.vue';

const api = vi.hoisted(() => ({
  createSnapshot: vi.fn(),
  queryArtifactCommitDiff: vi.fn(),
  queryArtifactCommitGraph: vi.fn(),
  querySnapshot: vi.fn(),
  queryStorageVolumeList: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const ElButtonStub = {
  emits: ['click'],
  template: '<button type="button" @click="$emit(\'click\')"><slot /></button>',
};
const ElTagStub = { template: '<span><slot /></span>' };

const commit = {
  commit_id: 'commit-a',
  parent_commit_id: 'commit-parent',
  message: 'Freeze training data',
  tag_names: ['dataset/v1'],
  created_at_unix_ms: '1785167600000',
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
  commit_id: 'commit-a',
  storage_volume_id: 'volume-ready',
  region: 'cn-shanghai',
  message: 'Freeze training data',
  tag_names: ['dataset/v1'],
  state: 'ready' as const,
  phase: 'idle' as const,
  integrity: {
    state: 'verified' as const,
    files_verified: '3',
    bytes_verified: '30',
  },
  logical_file_count: '3',
  logical_size_bytes: '30',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
};

function mockBaseQueries(): void {
  api.queryArtifactCommitGraph.mockResolvedValue({
    data: {
      graph: {
        graph_version: '4',
        head_commit_id: commit.commit_id,
        nodes: [commit],
      },
    },
    requestId: 'request-graph',
  });
  api.queryArtifactCommitDiff.mockResolvedValue({
    data: {
      diff: {
        target_commit: commit,
        summary: {
          files_added: '1',
          files_modified: '0',
          files_deleted: '0',
          files_renamed: '0',
          bytes_added: '30',
          bytes_removed: '0',
        },
        changes: [{ change_type: 'added', path: 'dataset/a.parquet', new_size_bytes: '30' }],
      },
    },
    requestId: 'request-diff',
  });
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
      { path: '/snapshots', name: 'snapshot-list', component: { template: '<div />' } },
      { path: '/artifact', name: 'artifact-detail', component: { template: '<div />' } },
      {
        path: '/snapshot/:snapshotId',
        name: 'snapshot-detail',
        component: { template: '<div />' },
      },
    ],
  });
  await router.push(
    '/tenants/tenant-a/projects/project-a/artifacts/artifact-a/snapshots/new?commit_id=commit-a',
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

afterEach(() => {
  vi.clearAllMocks();
});

describe('Snapshot create page', () => {
  it('uses Commit graph/diff, permits only Ready volumes and preserves identity across retry', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryArtifactCommitGraph).toHaveBeenCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
    );
    expect(api.queryArtifactCommitDiff).toHaveBeenCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
      'commit-a',
    );
    expect(wrapper.text()).toContain('commit-parent');

    await elementButton(wrapper, '选择存储位置').trigger('click');
    await flushPromises();
    expect(wrapper.text()).toContain('Ready volume');
    expect(wrapper.text()).toContain('Degraded volume');
    expect(
      wrapper
        .findAll('.volume-list > button')
        .find((item) => item.text().includes('Degraded volume'))
        ?.attributes('disabled'),
    ).toBeDefined();

    await wrapper.find('.volume-list > button:not([disabled])').trigger('click');
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
    const createRequests = (api.createSnapshot.mock.calls as unknown[][]).map(
      ([request]) => request as Record<string, unknown>,
    );
    expect(createRequests[0]).toMatchObject({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      commit_id: 'commit-a',
      storage_volume_id: 'volume-ready',
    });
    expect(createRequests[0]?.snapshot_request_id).toMatch(/^snapshot-request-/);
    expect(createRequests[1]).toEqual(createRequests[0]);
    expect(api.querySnapshot).toHaveBeenCalledWith('tenant-a', 'snapshot-a');
    expect(wrapper.text()).toContain('幂等重放');
    expect(wrapper.text()).toContain('复用现有交付位置');

    wrapper.unmount();
    queryClient.clear();
  });

  it('forwards the StorageVolume cursor while keeping non-ready resources visible but disabled', async () => {
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

    await elementButton(wrapper, '选择存储位置').trigger('click');
    await flushPromises();
    const degradedButton = wrapper
      .findAll('.volume-list > button')
      .find((item) => item.text().includes('Degraded volume'));
    expect(degradedButton?.exists()).toBe(true);
    expect(degradedButton?.attributes('disabled')).toBeDefined();

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

  it('creates a new request identity after the user leaves a failed placement attempt', async () => {
    const { wrapper, queryClient } = await mountPage();

    await elementButton(wrapper, '选择存储位置').trigger('click');
    await flushPromises();
    await wrapper.find('.volume-list > button:not([disabled])').trigger('click');
    api.createSnapshot.mockRejectedValueOnce(new TypeError('transport interrupted'));
    await elementButton(wrapper, '创建 Snapshot').trigger('click');
    await flushPromises();

    await elementButton(wrapper, '返回 Commit').trigger('click');
    await elementButton(wrapper, '选择存储位置').trigger('click');
    await elementButton(wrapper, '创建 Snapshot').trigger('click');
    await flushPromises();

    const requests = (api.createSnapshot.mock.calls as unknown[][]).map(
      ([request]) => request as Record<string, unknown>,
    );
    expect(requests).toHaveLength(2);
    expect(requests[0]?.snapshot_request_id).not.toBe(requests[1]?.snapshot_request_id);
    expect(requests[1]).toMatchObject({
      commit_id: 'commit-a',
      storage_volume_id: 'volume-ready',
    });

    wrapper.unmount();
    queryClient.clear();
  });

  it('ignores duplicate create clicks while the first mutation is pending', async () => {
    const { wrapper, queryClient } = await mountPage();
    await elementButton(wrapper, '选择存储位置').trigger('click');
    await flushPromises();
    await wrapper.find('.volume-list > button:not([disabled])').trigger('click');

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
