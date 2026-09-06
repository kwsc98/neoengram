import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus, { ElButton } from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import SnapshotDetailPage from '@/pages/SnapshotDetailPage.vue';
import { useTenantsStore } from '@/stores/tenants';
import type { RetrySnapshotDeliveryRequest, TenantView, TaskView } from '@/api/types';

const api = vi.hoisted(() => ({
  cancelTask: vi.fn(),
  deleteSnapshotDelivery: vi.fn(),
  queryApiVersion: vi.fn(),
  queryCommitAvailabilityV2: vi.fn(),
  queryCommitCoverage: vi.fn(),
  queryTaskList: vi.fn(),
  queryGatewayPoolList: vi.fn(),
  querySnapshot: vi.fn(),
  querySnapshotDeliveryList: vi.fn(),
  queryStorageVolume: vi.fn(),
  queryStorageVolumeList: vi.fn(),
  materializeCommit: vi.fn(),
  retryTask: vi.fn(),
  retrySnapshotDelivery: vi.fn(),
}));
vi.mock('@/api/operations', () => api);

const commitId = 'a'.repeat(64);

function snapshot(
  state: 'creating' | 'ready' | 'abnormal' = 'ready',
  dataLayout: 'fast_cdc' | 'whole_file' = 'fast_cdc',
) {
  return {
    snapshot_id: 'snapshot-a',
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    commit_id: commitId,
    delivery_id: 'delivery-a',
    edge_cluster_id: 'edge-a',
    storage_volume_id: 'volume-a',
    delivery_mode: 'copy' as const,
    data_layout: dataLayout,
    message: 'Freeze training data',
    tag_names: ['dataset/v1'],
    state,
    data_health: 'available' as const,
    ...(state === 'abnormal'
      ? { issue: { code: 'DELIVERY_FAILED', message: 'Delivery failed', retryable: true } }
      : {}),
    integrity: {
      state:
        state === 'ready'
          ? ('verified' as const)
          : state === 'abnormal'
            ? ('failed' as const)
            : ('pending' as const),
      files_verified: state === 'ready' ? '3' : '0',
      bytes_verified: state === 'ready' ? '30' : '0',
    },
    logical_file_count: '3',
    logical_size_bytes: '30',
    created_at_unix_ms: '1',
    updated_at_unix_ms: '2',
  };
}

function materializationTask(
  taskId: string,
  state: TaskView['state'],
  targetStorageVolumeId = 'volume-a',
  attempt = '1',
  issue?: TaskView['issue'],
): TaskView {
  return {
    task_id: taskId,
    intent_kind: 'commit.materialize',
    purpose: 'copy',
    state,
    tenant_id: 'tenant-a',
    primary_resource: { resource_kind: 'materialization', resource_id: taskId },
    resource_links: [
      { resource_kind: 'project', resource_id: 'project-a', role: 'related' },
      { resource_kind: 'artifact', resource_id: 'artifact-a', role: 'related' },
      { resource_kind: 'object_namespace', resource_id: 'artifact-a', role: 'related' },
      { resource_kind: 'commit', resource_id: commitId, role: 'source' },
      { resource_kind: 'storage_volume', resource_id: targetStorageVolumeId, role: 'target' },
    ],
    execution_id: `execution-${taskId}`,
    execution_key_digest: 'e'.repeat(64),
    execution_reused: false,
    current_stage: {
      stage_key: state === 'succeeded' ? 'finalize' : 'transfer',
      stage_kind: state === 'succeeded' ? 'finalize' : 'transfer',
      ordinal: state === 'succeeded' ? '6' : '3',
      dependencies: state === 'succeeded' ? ['publish_coverage'] : ['plan'],
      state: state === 'queued' ? 'ready' : state,
      stage_attempt: attempt,
      progress: {
        completed: state === 'succeeded' ? '3' : '2',
        total: '3',
        completed_bytes: state === 'succeeded' ? '30' : '20',
        total_bytes: '30',
      },
      created_at_unix_ms: '1',
      updated_at_unix_ms: '2',
      resource_version: '1',
    },
    stages: [],
    request_id: `request-${taskId}`,
    request_digest: 'e'.repeat(64),
    actor: 'test-user',
    attempt,
    progress: {
      completed: state === 'succeeded' ? '3' : '2',
      total: '3',
      completed_bytes: state === 'succeeded' ? '30' : '20',
      total_bytes: '30',
    },
    deadline_unix_ms: '9999999999999',
    ...(issue ? { issue } : {}),
    created_at_unix_ms: '1',
    updated_at_unix_ms: '2',
    resource_version: '1',
    origin: 'user',
    executable: true,
  };
}

async function mountPage(
  state: 'creating' | 'ready' | 'abnormal' = 'ready',
  permissions: TenantView['permissions'] = ['s3.access.read', 'artifact.commit.replicate'],
  dataLayout: 'fast_cdc' | 'whole_file' = 'fast_cdc',
) {
  api.queryApiVersion.mockResolvedValue({
    data: {
      api_version: 1,
      capabilities: [
        's3_readonly_access_point',
        'commit_materialization_v2',
        'snapshot_delivery_fuse_v2',
        'snapshot_delivery_copy_v2',
        'snapshot_delivery_hardlink_v2',
      ],
    },
    requestId: 'request-version',
  });
  api.querySnapshot.mockResolvedValue({
    data: { snapshot: snapshot(state, dataLayout) },
    requestId: 'request-snapshot',
  });
  api.queryStorageVolume.mockResolvedValue({
    data: {
      storage_volume: {
        tenant_id: 'tenant-a',
        storage_volume_id: 'volume-a',
        display_name: 'Volume A',
        edge_cluster_id: 'edge-a',
        region: 'cn-shanghai',
        backend_type: 'nfs',
        access_mode: 'read_write_many',
        allowed_delivery_modes: ['fuse', 'copy', 'hardlink'],
        hardlink_policy: 'sealed_acl',
        max_whole_file_bytes: '1073741824',
        copy_reserve_bytes: '1024',
        state: 'ready',
        resource_version: '1',
        lifecycle: { state: 'active', generation: '1', resource_version: '1' },
        created_at_unix_ms: '1',
        updated_at_unix_ms: '2',
      },
    },
    requestId: 'request-volume',
  });
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
          allowed_delivery_modes: ['fuse', 'copy', 'hardlink'],
          hardlink_policy: 'sealed_acl',
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
  api.querySnapshotDeliveryList.mockResolvedValue({
    data: {
      items: [
        {
          delivery_id: 'delivery-a',
          snapshot_id: 'snapshot-a',
          commit_id: commitId,
          storage_volume_id: 'volume-a',
          mode: 'copy',
          target_relative_root: 'snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-a',
          state: state === 'ready' ? 'ready' : state === 'abnormal' ? 'failed' : 'requested',
          source_index_digest: 'b'.repeat(64),
          delivery_generation: '1',
          file_count: '3',
          size_bytes: '30',
          object_set_digest: 'c'.repeat(64),
          resource_version: '1',
          ...(state === 'abnormal'
            ? {
                issue: {
                  code: 'DELIVERY_FAILED',
                  message: 'Delivery failed',
                  retryable: true,
                },
              }
            : {}),
          created_at_unix_ms: '1',
          updated_at_unix_ms: '2',
        },
      ],
    },
    requestId: 'request-deliveries',
  });
  api.queryTaskList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-tasks',
  });
  api.queryCommitCoverage.mockResolvedValue({
    data: { coverage: [] },
    requestId: 'request-coverage',
  });
  api.queryCommitAvailabilityV2.mockResolvedValue({
    data: {
      availability: {
        object_namespace_id: 'artifact-a',
        commit_id: commitId,
        object_count: '3',
        content_presence: 'available',
        source_serving: 'available',
        durability: 'satisfied',
        target_coverage: 'not_requested',
        view_readiness: 'ready',
        complete_volume_count: '1',
        missing_objects: [],
        verified_storage_volume_ids: [],
      },
    },
    requestId: 'request-availability',
  });
  api.queryGatewayPoolList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-gateway-pools',
  });
  api.materializeCommit.mockResolvedValue({
    data: {
      materialization: {
        materialization_id: 'materialization-a',
        tenant_id: 'tenant-a',
        artifact_id: 'artifact-a',
        object_namespace_id: 'artifact-a',
        commit_id: commitId,
        target_storage_volume_id: 'volume-a',
        plan_revision: '1',
        state: 'queued',
        object_set_digest: 'd'.repeat(64),
        verified_objects: '0',
        total_objects: '3',
        verified_bytes: '0',
        total_bytes: '30',
        missing_objects: '3',
        missing_bytes: '30',
        source_count: '0',
      },
      task: materializationTask('materialization-a', 'queued'),
      request_replayed: false,
      execution_reused: false,
    },
    requestId: 'request-materialize',
  });
  api.retryTask.mockResolvedValue({
    data: {
      task: materializationTask('materialization-a', 'queued', 'volume-a', '2'),
      request_replayed: false,
      execution_reused: false,
    },
    requestId: 'request-retry-task',
  });
  api.cancelTask.mockResolvedValue({
    data: {
      task: materializationTask('materialization-a', 'cancelled'),
      request_replayed: false,
      execution_reused: false,
    },
    requestId: 'request-cancel-task',
  });
  api.retrySnapshotDelivery.mockResolvedValue({
    data: { delivery: {}, request_replayed: false },
    requestId: 'request-retry-delivery',
  });
  api.deleteSnapshotDelivery.mockResolvedValue({
    data: { delivery: {}, request_replayed: false },
    requestId: 'request-delete-delivery',
  });
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/:snapshotId',
        name: 'snapshot-detail',
        component: SnapshotDetailPage,
      },
      { path: '/artifact', name: 'artifact-detail', component: { template: '<div />' } },
      {
        path: '/tenants/:tenantId/object-storage',
        name: 'object-storage-list',
        component: { template: '<div />' },
      },
    ],
  });
  await router.push(
    '/tenants/tenant-a/projects/project-a/artifacts/artifact-a/snapshots/snapshot-a',
  );
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '1',
      permissions,
    },
  ];
  const wrapper = mount(SnapshotDetailPage, {
    global: { plugins: [pinia, ElementPlus, [VueQueryPlugin, { queryClient }], router] },
  });
  await flushPromises();
  return { wrapper, queryClient, router };
}

afterEach(() => vi.resetAllMocks());

describe('Snapshot detail page', () => {
  it('queries the real Snapshot and presents its immutable read-only placement', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.querySnapshot).toHaveBeenCalledWith('tenant-a', 'snapshot-a');
    expect(wrapper.text()).toContain('Snapshot 与唯一 Delivery 均已就绪，可浏览对象存储');
    expect(wrapper.text()).toContain('Volume A');
    expect(wrapper.text()).toContain('edge-a');
    expect(wrapper.text()).toContain('delivery-a');
    expect(wrapper.text()).toContain(commitId);
    expect(wrapper.text()).toContain('只读');
    expect(wrapper.text()).not.toContain('重试交付');

    wrapper.unmount();
    queryClient.clear();
  });

  it('refreshes live StorageVolume availability for the immutable delivery target', async () => {
    vi.useFakeTimers();
    const mounted = await mountPage();
    try {
      const { wrapper } = mounted;
      expect(api.queryStorageVolumeList).toHaveBeenCalledTimes(1);
      expect(api.queryStorageVolume).toHaveBeenCalledTimes(1);

      api.queryStorageVolumeList.mockResolvedValueOnce({
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
              allowed_delivery_modes: ['fuse', 'copy', 'hardlink'],
              hardlink_policy: 'sealed_acl',
              max_whole_file_bytes: '1073741824',
              copy_reserve_bytes: '1024',
              state: 'unavailable',
              resource_version: '2',
              lifecycle: { state: 'active', generation: '1', resource_version: '2' },
              created_at_unix_ms: '1',
              updated_at_unix_ms: '3',
            },
          ],
        },
        requestId: 'request-volume-list-unavailable',
      });
      api.queryStorageVolume.mockResolvedValueOnce({
        data: {
          storage_volume: {
            tenant_id: 'tenant-a',
            storage_volume_id: 'volume-a',
            display_name: 'Volume A',
            edge_cluster_id: 'edge-a',
            region: 'cn-shanghai',
            backend_type: 'nfs',
            access_mode: 'read_write_many',
            allowed_delivery_modes: ['fuse', 'copy', 'hardlink'],
            hardlink_policy: 'sealed_acl',
            max_whole_file_bytes: '1073741824',
            copy_reserve_bytes: '1024',
            state: 'unavailable',
            resource_version: '2',
            lifecycle: { state: 'active', generation: '1', resource_version: '2' },
            created_at_unix_ms: '1',
            updated_at_unix_ms: '3',
          },
        },
        requestId: 'request-volume-unavailable',
      });
      await vi.advanceTimersByTimeAsync(5_000);
      await flushPromises();

      expect(api.queryStorageVolumeList).toHaveBeenCalledTimes(2);
      expect(api.queryStorageVolume).toHaveBeenCalledTimes(2);
      expect(wrapper.text()).toContain('目标 StorageVolume：volume-a');
      expect(wrapper.text()).toContain('对象存储尚未就绪');
    } finally {
      mounted.wrapper.unmount();
      mounted.queryClient.clear();
      vi.useRealTimers();
    }
  });

  it('shows the Snapshot creation state without inventing delivery progress', async () => {
    const { wrapper, queryClient } = await mountPage('creating');

    expect(wrapper.text()).toContain('创建中');
    expect(wrapper.text()).toContain('只读 Snapshot');
    expect(wrapper.text()).toContain('正在冻结不可变 Commit');

    wrapper.unmount();
    queryClient.clear();
  });

  it('links a ready Snapshot to its object storage context', async () => {
    const { wrapper, queryClient, router } = await mountPage();
    const push = vi.spyOn(router, 'push').mockResolvedValue(undefined);
    const objectStorageButton = wrapper
      .findAllComponents(ElButton)
      .find((button) => button.text().trim() === '对象存储');

    expect(objectStorageButton).toBeDefined();
    await objectStorageButton!.trigger('click');

    expect(push).toHaveBeenCalledWith({
      name: 'object-storage-list',
      params: { tenantId: 'tenant-a' },
      query: {
        snapshotId: 'snapshot-a',
        projectId: 'project-a',
        artifactId: 'artifact-a',
      },
    });

    wrapper.unmount();
    queryClient.clear();
  });

  it('does not expose object storage while a Snapshot is creating', async () => {
    const { wrapper, queryClient } = await mountPage('creating');

    expect(
      wrapper.findAllComponents(ElButton).some((button) => button.text().trim() === '对象存储'),
    ).toBe(false);

    wrapper.unmount();
    queryClient.clear();
  });

  it('hides object storage when Central does not advertise the capability', async () => {
    api.queryApiVersion.mockResolvedValueOnce({
      data: { api_version: 1, capabilities: [] },
      requestId: 'request-version',
    });
    const { wrapper, queryClient } = await mountPage();

    expect(
      wrapper.findAllComponents(ElButton).some((button) => button.text().trim() === '对象存储'),
    ).toBe(false);

    wrapper.unmount();
    queryClient.clear();
  });

  it('hides object storage without s3.access.read in the current Tenant', async () => {
    const { wrapper, queryClient } = await mountPage('ready', ['snapshot.read']);

    expect(
      wrapper.findAllComponents(ElButton).some((button) => button.text().trim() === '对象存储'),
    ).toBe(false);

    wrapper.unmount();
    queryClient.clear();
  });

  it('hides Commit replication when Central does not advertise the capability', async () => {
    api.queryApiVersion.mockResolvedValueOnce({
      data: {
        api_version: 1,
        capabilities: ['snapshot_delivery_copy_v2'],
      },
      requestId: 'request-version',
    });
    const { wrapper, queryClient } = await mountPage('ready', ['artifact.commit.replicate']);

    expect(wrapper.find('[aria-label="Commit 复制"]').exists()).toBe(false);
    expect(api.queryTaskList).not.toHaveBeenCalled();
    expect(api.materializeCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('shows the bound Delivery mode as fixed and does not expose a second-create control', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryStorageVolume).toHaveBeenCalledWith('tenant-a', 'volume-a', 'snapshot-a');
    expect(wrapper.text()).toContain('固定模式：全部复制');
    expect(wrapper.text()).toContain('唯一 Delivery：delivery-a');
    expect(
      wrapper.findAllComponents(ElButton).some((button) => button.text().trim() === '创建交付'),
    ).toBe(false);

    wrapper.unmount();
    queryClient.clear();
  });

  it('keeps a non-bound delivery mode unavailable even for a WholeFile Commit', async () => {
    const { wrapper, queryClient } = await mountPage('ready', ['snapshot.read'], 'whole_file');

    expect(wrapper.text()).toContain('硬链接未就绪');
    expect(wrapper.text()).toContain('Snapshot 创建时已固定其他交付模式');

    wrapper.unmount();
    queryClient.clear();
  });

  it('does not enumerate StorageVolumes for a Snapshot-only reader', async () => {
    const { wrapper, queryClient } = await mountPage('ready', ['snapshot.read']);

    expect(api.queryStorageVolume).toHaveBeenCalledWith('tenant-a', 'volume-a', 'snapshot-a');
    expect(api.queryStorageVolumeList).not.toHaveBeenCalled();
    expect(api.queryGatewayPoolList).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('creates a Commit materialization with project scope and a bounded stable request ID', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryTaskList).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      object_namespace_id: 'artifact-a',
      commit_id: commitId,
      intent_kind: ['commit.materialize'],
      page_size: 100,
    });
    expect(api.queryGatewayPoolList).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain('路由由 Central 校验');

    await wrapper
      .findAllComponents(ElButton)
      .find((button) => button.text().trim() === '复制 Commit')!
      .trigger('click');
    await flushPromises();

    const request = api.materializeCommit.mock.calls[0]?.[0] as
      { request_id?: string; [key: string]: unknown } | undefined;
    expect(request).toMatchObject({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      commit_id: commitId,
      target_storage_volume_id: 'volume-a',
      purpose: 'copy',
    });
    expect(request?.request_id).toMatch(/^commit-materialize-[a-f0-9]{32}$/);
    expect(request?.request_id?.length).toBeLessThanOrEqual(128);

    wrapper.unmount();
    queryClient.clear();
  });

  it('restores and cancels an active Commit replication after opening the page', async () => {
    api.queryTaskList.mockResolvedValueOnce({
      data: {
        items: [
          {
            task_id: 'replication-active',
            intent_kind: 'commit.materialize',
            state: 'running',
            purpose: 'copy',
            tenant_id: 'tenant-a',
            primary_resource: {
              resource_kind: 'materialization',
              resource_id: 'replication-active',
            },
            resource_links: [
              { resource_kind: 'project', resource_id: 'project-a', role: 'related' },
              { resource_kind: 'artifact', resource_id: 'artifact-a', role: 'related' },
              { resource_kind: 'object_namespace', resource_id: 'artifact-a', role: 'related' },
              { resource_kind: 'commit', resource_id: commitId, role: 'source' },
              { resource_kind: 'storage_volume', resource_id: 'volume-a', role: 'target' },
            ],
            execution_id: 'execution-replication-active',
            execution_key_digest: 'e'.repeat(64),
            execution_reused: false,
            current_stage: {
              stage_key: 'transfer',
              stage_kind: 'transfer',
              ordinal: '3',
              dependencies: ['plan'],
              state: 'running',
              stage_attempt: '7',
              progress: { completed: '2', total: '3', completed_bytes: '20', total_bytes: '30' },
              created_at_unix_ms: '1',
              updated_at_unix_ms: '2',
              resource_version: '1',
            },
            stages: [],
            request_id: 'request-active-replication',
            attempt: '7',
            progress: { completed: '2', total: '3', completed_bytes: '20', total_bytes: '30' },
            deadline_unix_ms: '9999999999999',
            created_at_unix_ms: '1',
            updated_at_unix_ms: '2',
            resource_version: '1',
            origin: 'user',
            executable: true,
          },
        ],
      },
      requestId: 'request-active-replication',
    });
    const { wrapper, queryClient } = await mountPage();

    expect(wrapper.text()).toContain('replication-active');
    expect(wrapper.text()).toContain('2 / 3 objects');
    expect(
      wrapper
        .findAllComponents(ElButton)
        .find((button) => button.text().trim() === '复制进行中')
        ?.attributes('disabled'),
    ).toBeDefined();

    await wrapper
      .findAllComponents(ElButton)
      .find((button) => button.text().trim() === '取消')!
      .trigger('click');
    await flushPromises();

    expect(api.cancelTask.mock.calls[0]?.[0]).toEqual({
      tenant_id: 'tenant-a',
      task_id: 'replication-active',
    });
    expect(api.materializeCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('retries the failed task instead of creating another task for the same target', async () => {
    api.queryTaskList.mockResolvedValueOnce({
      data: {
        items: [
          {
            task_id: 'replication-failed',
            intent_kind: 'commit.materialize',
            state: 'failed',
            purpose: 'copy',
            tenant_id: 'tenant-a',
            primary_resource: {
              resource_kind: 'materialization',
              resource_id: 'replication-failed',
            },
            resource_links: [
              { resource_kind: 'project', resource_id: 'project-a', role: 'related' },
              { resource_kind: 'artifact', resource_id: 'artifact-a', role: 'related' },
              { resource_kind: 'object_namespace', resource_id: 'artifact-a', role: 'related' },
              { resource_kind: 'commit', resource_id: commitId, role: 'source' },
              { resource_kind: 'storage_volume', resource_id: 'volume-a', role: 'target' },
            ],
            execution_id: 'execution-replication-failed',
            execution_key_digest: 'e'.repeat(64),
            execution_reused: false,
            current_stage: {
              stage_key: 'transfer',
              stage_kind: 'transfer',
              ordinal: '3',
              dependencies: ['plan'],
              state: 'failed',
              stage_attempt: '3',
              progress: { completed: '1', total: '3', completed_bytes: '10', total_bytes: '30' },
              created_at_unix_ms: '1',
              updated_at_unix_ms: '2',
              resource_version: '1',
            },
            stages: [],
            request_id: 'request-failed-replication',
            attempt: '3',
            progress: { completed: '1', total: '3', completed_bytes: '10', total_bytes: '30' },
            deadline_unix_ms: '9999999999999',
            issue: { code: 'ROUTE_LOST', message: 'Route lease expired', retryable: true },
            created_at_unix_ms: '1',
            updated_at_unix_ms: '2',
            resource_version: '1',
            origin: 'user',
            executable: true,
          },
        ],
      },
      requestId: 'request-failed-replication',
    });
    const { wrapper, queryClient } = await mountPage();

    expect(wrapper.text()).toContain('Route lease expired');
    await wrapper
      .findAllComponents(ElButton)
      .find((button) => button.text().trim() === '重试')!
      .trigger('click');
    await flushPromises();

    expect(api.retryTask.mock.calls[0]?.[0]).toEqual({
      tenant_id: 'tenant-a',
      task_id: 'replication-failed',
    });
    expect(api.materializeCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('uses visible Gateway health to disable an unavailable cluster route', async () => {
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
    const { wrapper, queryClient } = await mountPage('ready', [
      'artifact.commit.replicate',
      'gateway.read',
    ]);

    expect(api.queryGatewayPoolList).toHaveBeenCalledWith({});
    expect(wrapper.text()).toContain('路由不可用');
    expect(
      wrapper
        .findAllComponents(ElButton)
        .find((button) => button.text().trim() === '复制 Commit')
        ?.attributes('disabled'),
    ).toBeDefined();

    wrapper.unmount();
    queryClient.clear();
  });

  it('retries a failed delivery with a fresh request identity', async () => {
    api.querySnapshotDeliveryList.mockResolvedValueOnce({
      data: {
        items: [
          {
            delivery_id: 'delivery-a',
            snapshot_id: 'snapshot-a',
            commit_id: commitId,
            storage_volume_id: 'volume-a',
            mode: 'copy',
            target_relative_root: 'snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-a',
            state: 'failed',
            source_index_digest: 'b'.repeat(64),
            delivery_generation: '1',
            file_count: '3',
            size_bytes: '30',
            object_set_digest: 'c'.repeat(64),
            resource_version: '1',
            issue: {
              code: 'DELIVERY_OBJECT_UNAVAILABLE',
              message: 'Object storage is temporarily unavailable',
              retryable: true,
            },
            created_at_unix_ms: '1',
            updated_at_unix_ms: '2',
          },
        ],
      },
      requestId: 'request-deliveries',
    });
    const { wrapper, queryClient } = await mountPage();
    const retry = wrapper
      .findAllComponents(ElButton)
      .find((button) => button.text().trim() === '重试');

    expect(wrapper.text()).toContain('DELIVERY_OBJECT_UNAVAILABLE');
    expect(retry).toBeDefined();
    await retry!.trigger('click');
    await flushPromises();

    const retryRequest = api.retrySnapshotDelivery.mock.calls[0]?.[0] as unknown as
      RetrySnapshotDeliveryRequest | undefined;
    expect(retryRequest).toMatchObject({
      tenant_id: 'tenant-a',
      delivery_id: 'delivery-a',
    });
    expect(retryRequest?.request_id).toMatch(/^retry-delivery-a-/);

    wrapper.unmount();
    queryClient.clear();
  });

  it('hides retry for a deterministic delivery failure', async () => {
    api.querySnapshotDeliveryList.mockResolvedValueOnce({
      data: {
        items: [
          {
            delivery_id: 'delivery-a',
            snapshot_id: 'snapshot-a',
            commit_id: commitId,
            storage_volume_id: 'volume-a',
            mode: 'hardlink',
            target_relative_root: 'snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-a',
            state: 'failed',
            source_index_digest: 'b'.repeat(64),
            delivery_generation: '1',
            file_count: '3',
            size_bytes: '30',
            object_set_digest: 'c'.repeat(64),
            resource_version: '1',
            issue: {
              code: 'HARDLINK_CROSS_FILESYSTEM',
              message: 'Source and target are on different filesystems',
              retryable: false,
            },
            created_at_unix_ms: '1',
            updated_at_unix_ms: '2',
          },
        ],
      },
      requestId: 'request-deliveries',
    });
    const { wrapper, queryClient } = await mountPage();

    expect(wrapper.text()).toContain('HARDLINK_CROSS_FILESYSTEM');
    expect(
      wrapper.findAllComponents(ElButton).some((button) => button.text().trim() === '重试'),
    ).toBe(false);

    wrapper.unmount();
    queryClient.clear();
  });
});
