import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount, type VueWrapper } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';
import SnapshotCreatePage from '@/pages/SnapshotCreatePage.vue';

const api = vi.hoisted(() => ({
  createSnapshot: vi.fn(),
  queryApiVersion: vi.fn(),
  queryArtifact: vi.fn(),
  queryArtifactCommitGraph: vi.fn(),
  querySnapshot: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const ElButtonStub = {
  emits: ['click'],
  template: '<button type="button" @click="$emit(\'click\')"><slot /></button>',
};
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
    data: { api_version: 1, agent_wire_version: 1, capabilities: ['artifact_commit_graph'] },
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
    data: { snapshot, replayed: true },
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
      stubs: { ElButton: ElButtonStub },
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
  it('creates a logical Snapshot without selecting a Volume', async () => {
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
      .mockResolvedValueOnce({ data: { snapshot, replayed: true }, requestId: 'retry' });
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
    });
    expect(first).not.toHaveProperty('storage_volume_id');
    expect(first).not.toHaveProperty('region');
    expect(first.request_id).toMatch(/^snapshot-request-/);
    expect(second).toEqual(first);
    expect(wrapper.text()).toContain('请在详情页先复制到目标 Volume');
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
    resolveCreate({ data: { snapshot, replayed: false }, requestId: 'pending' });
    await flushPromises();
    wrapper.unmount();
    queryClient.clear();
  });
});
