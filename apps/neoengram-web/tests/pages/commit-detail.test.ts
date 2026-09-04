import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type {
  ArtifactView,
  CommitGraphView,
  MaterializationView,
  QueryCommitAvailabilityV2Response,
  StorageVolumeView,
  TaskMutationResponse,
  TaskView,
} from '@/api/types';
import CommitDetailPage from '@/pages/CommitDetailPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  queryApiVersion: vi.fn(),
  queryArtifact: vi.fn(),
  queryArtifactCommitDiff: vi.fn(),
  queryArtifactCommitGraph: vi.fn(),
  queryCommitAvailabilityV2: vi.fn(),
  queryCommitCoverage: vi.fn(),
  queryTaskList: vi.fn(),
  queryGatewayPoolList: vi.fn(),
  queryStorageVolumeList: vi.fn(),
  materializeCommit: vi.fn(),
  retryTask: vi.fn(),
  cancelTask: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const commitId = 'c'.repeat(64);
const parentCommitId = 'p'.repeat(64);
const artifact: ArtifactView = {
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'artifact-a',
  display_name: 'Vision set',
  description: 'Test artifact',
  initialization: { mode: 'empty' },
  head_commit_id: commitId,
  resource_version: '1',
  lifecycle: { state: 'active', generation: '1' },
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
};
const graph: CommitGraphView = {
  graph_version: '1',
  head_commit_id: commitId,
  nodes: [
    {
      commit_id: commitId,
      parent_commit_id: parentCommitId,
      message: 'Release candidate',
      description: 'Ready for review',
      tag_names: ['rc'],
      data_layout: 'fast_cdc',
      created_at_unix_ms: '2',
    },
    {
      commit_id: parentCommitId,
      message: 'Initial import',
      tag_names: [],
      data_layout: 'fast_cdc',
      created_at_unix_ms: '1',
    },
  ],
};

function volume(storageVolumeId: string, displayName: string): StorageVolumeView {
  return {
    tenant_id: 'tenant-a',
    storage_volume_id: storageVolumeId,
    display_name: displayName,
    edge_cluster_id: `edge-${storageVolumeId}`,
    backend_type: 'pvc',
    access_mode: 'read_write_once',
    region: 'ap-southeast-1',
    allowed_delivery_modes: ['copy'],
    hardlink_policy: 'disabled',
    max_whole_file_bytes: '1024',
    copy_reserve_bytes: '0',
    state: 'ready',
    resource_version: '1',
    lifecycle: { state: 'active', generation: '1' },
    created_at_unix_ms: '1',
    updated_at_unix_ms: '2',
  };
}

const coverage = [
  {
    object_namespace_id: 'artifact-a',
    commit_id: commitId,
    storage_volume_id: 'volume-a',
    placement_generation: '1',
    object_set_digest: 'd'.repeat(64),
    total_objects: '4',
    verified_objects: '4',
    total_bytes: '400',
    verified_bytes: '400',
    missing_objects: '0',
    missing_bytes: '0',
    state: 'complete' as const,
  },
  {
    object_namespace_id: 'artifact-a',
    commit_id: commitId,
    storage_volume_id: 'volume-b',
    placement_generation: '1',
    object_set_digest: 'd'.repeat(64),
    total_objects: '4',
    verified_objects: '3',
    total_bytes: '400',
    verified_bytes: '300',
    missing_objects: '1',
    missing_bytes: '100',
    state: 'partial' as const,
  },
];

const availability: QueryCommitAvailabilityV2Response = {
  availability: {
    object_namespace_id: 'artifact-a',
    commit_id: commitId,
    object_count: '4',
    content_presence: 'available',
    source_serving: 'available',
    durability: 'under_replicated',
    target_coverage: 'partial',
    view_readiness: 'not_ready',
    complete_volume_count: '1',
    missing_objects: [],
    verified_storage_volume_ids: ['volume-a'],
  },
};

const repairJob: MaterializationView = {
  materialization_id: 'materialization-volume-b',
  tenant_id: 'tenant-a',
  artifact_id: 'artifact-a',
  object_namespace_id: 'artifact-a',
  commit_id: commitId,
  target_storage_volume_id: 'volume-b',
  plan_revision: '3',
  coverage_goal: 'complete',
  state: 'complete',
  object_set_digest: 'd'.repeat(64),
  verified_objects: '3',
  total_objects: '4',
  verified_bytes: '300',
  total_bytes: '400',
  missing_objects: '1',
  missing_bytes: '100',
  source_count: '1',
};

function taskForMaterialization(
  value: MaterializationView,
  state: TaskView['state'] = value.state === 'complete' ? 'succeeded' : 'queued',
): TaskView {
  return {
    task_id: value.materialization_id,
    task_kind: 'commit.materialize',
    state,
    phase: value.state,
    tenant_id: value.tenant_id,
    project_id: artifact.project_id,
    ...(value.artifact_id ? { artifact_id: value.artifact_id } : {}),
    object_namespace_id: value.object_namespace_id,
    commit_id: value.commit_id,
    storage_volume_id: value.target_storage_volume_id,
    request_id: 'materialize-request',
    request_digest: 'e'.repeat(64),
    actor: 'test-user',
    attempt: value.plan_revision,
    progress: {
      completed: value.verified_objects,
      total: value.total_objects,
      completed_bytes: value.verified_bytes,
      total_bytes: value.total_bytes,
    },
    detail_kind: 'materialization',
    detail_id: value.materialization_id,
    deadline_unix_ms: '9999999999999',
    created_at_unix_ms: '1',
    updated_at_unix_ms: '2',
    resource_version: '1',
    origin: 'user',
    executable: true,
  };
}

function prepareApi(): void {
  api.queryApiVersion.mockResolvedValue({
    data: {
      api_version: '1',
      server_version: '0.2.0',
      capabilities: ['artifact_commit_graph', 'artifact_commit_diff', 'commit_materialization_v2'],
    },
    requestId: 'version',
  });
  api.queryArtifact.mockResolvedValue({ data: { artifact }, requestId: 'artifact' });
  api.queryArtifactCommitGraph.mockResolvedValue({ data: { graph }, requestId: 'graph' });
  api.queryArtifactCommitDiff.mockResolvedValue({
    data: {
      diff: {
        base_commit: graph.nodes[1],
        target_commit: graph.nodes[0],
        summary: {
          files_added: '1',
          files_modified: '0',
          files_deleted: '0',
          files_renamed: '0',
          bytes_added: '100',
          bytes_removed: '0',
        },
        changes: [],
      },
    },
    requestId: 'diff',
  });
  api.queryCommitCoverage.mockResolvedValue({ data: { coverage }, requestId: 'coverage' });
  api.queryCommitAvailabilityV2.mockResolvedValue({
    data: availability,
    requestId: 'availability',
  });
  api.queryTaskList.mockResolvedValue({
    data: { items: [taskForMaterialization(repairJob)] },
    requestId: 'materializations',
  });
  api.queryStorageVolumeList.mockResolvedValue({
    data: { items: [volume('volume-a', 'Primary'), volume('volume-b', 'Repair target')] },
    requestId: 'volumes',
  });
  api.queryGatewayPoolList.mockResolvedValue({
    data: {
      items: [
        {
          gateway_pool_id: 'pool-volume-a',
          edge_cluster_id: 'edge-volume-a',
          display_name: 'Gateway A',
          agent_endpoint: 'https://gateway-a.example.test',
          desired_replicas: 1,
          minimum_ready_replicas: 1,
          state: 'ready',
          config_generation: '1',
          resource_version: '1',
          created_at_unix_ms: '1',
          updated_at_unix_ms: '2',
        },
        {
          gateway_pool_id: 'pool-volume-b',
          edge_cluster_id: 'edge-volume-b',
          display_name: 'Gateway B',
          agent_endpoint: 'https://gateway-b.example.test',
          desired_replicas: 1,
          minimum_ready_replicas: 1,
          state: 'ready',
          config_generation: '1',
          resource_version: '1',
          created_at_unix_ms: '1',
          updated_at_unix_ms: '2',
        },
      ],
    },
    requestId: 'gateways',
  });
  api.retryTask.mockResolvedValue({
    data: {
      task: taskForMaterialization(
        { ...repairJob, state: 'planning', plan_revision: '4' },
        'running',
      ),
      replayed: false,
    } satisfies TaskMutationResponse,
    requestId: 'retry',
  });
  api.materializeCommit.mockResolvedValue({
    data: {
      materialization: { ...repairJob, target_storage_volume_id: 'volume-c', state: 'queued' },
      task: taskForMaterialization(
        { ...repairJob, target_storage_volume_id: 'volume-c', state: 'queued' },
        'queued',
      ),
      replayed: false,
    },
    requestId: 'materialize',
  });
}

async function mountPage() {
  prepareApi();
  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      permissions: ['artifact.commit.replicate', 'gateway.read'],
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '2',
    },
  ];
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/commits/:commitId',
        name: 'commit-detail',
        component: CommitDetailPage,
      },
    ],
  });
  await router.push({
    name: 'commit-detail',
    params: {
      tenantId: 'tenant-a',
      projectId: 'project-a',
      artifactId: 'artifact-a',
      commitId,
    },
  });
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(CommitDetailPage, {
    global: { plugins: [ElementPlus, pinia, [VueQueryPlugin, { queryClient }], router] },
  });
  await flushPromises();
  return { wrapper, queryClient, router };
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('Commit detail page', () => {
  it('renders v2 availability and per-volume coverage on its own route', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryCommitAvailabilityV2).toHaveBeenCalledWith(
      expect.objectContaining({ object_namespace_id: 'artifact-a', commit_id: commitId }),
    );
    expect(wrapper.text()).toContain('Release candidate');
    expect(wrapper.text()).toContain('副本不足');
    expect(wrapper.text()).toContain('部分覆盖');
    expect(wrapper.text()).toContain('Repair target');
    expect(wrapper.text()).toContain('文件 Diff');
    expect(wrapper.text()).toContain('与基线没有文件变化');

    wrapper.unmount();
    queryClient.clear();
  });

  it('navigates from a Commit to its parent on the same detail route', async () => {
    const { wrapper, queryClient, router } = await mountPage();

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '查看父 Commit')!
      .trigger('click');
    await flushPromises();

    expect(router.currentRoute.value.name).toBe('commit-detail');
    expect(router.currentRoute.value.params).toMatchObject({
      tenantId: 'tenant-a',
      projectId: 'project-a',
      artifactId: 'artifact-a',
      commitId: parentCommitId,
    });

    wrapper.unmount();
    queryClient.clear();
  });

  it('checks one replica and repairs its existing materialization job', async () => {
    const { wrapper, queryClient } = await mountPage();

    const coverageRow = wrapper
      .findAll('.commit-coverage-row')
      .find((row) => row.text().includes('Repair target'));
    expect(coverageRow).toBeDefined();
    await coverageRow!
      .findAll('button')
      .find((button) => button.text() === '检查')!
      .trigger('click');
    await flushPromises();
    expect(api.queryCommitCoverage).toHaveBeenCalledWith(
      expect.objectContaining({ storage_volume_id: 'volume-b' }),
    );
    expect(api.queryCommitAvailabilityV2).toHaveBeenCalledWith(
      expect.objectContaining({ target_storage_volume_id: 'volume-b' }),
    );

    await coverageRow!
      .findAll('button')
      .find((button) => button.text() === '修复副本')!
      .trigger('click');
    await flushPromises();
    const retryRequest = api.retryTask.mock.calls[0]?.[0] as Record<string, unknown> | undefined;
    expect(retryRequest).toMatchObject({
      task_id: repairJob.materialization_id,
    });

    wrapper.unmount();
    queryClient.clear();
  });
});
