import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus, { ElButton } from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import SnapshotDetailPage from '@/pages/SnapshotDetailPage.vue';
import { useTenantsStore } from '@/stores/tenants';
import type {
  CreateCommitReplicationRequest,
  RetrySnapshotDeliveryRequest,
  TenantView,
} from '@/api/types';

const api = vi.hoisted(() => ({
  createSnapshotDelivery: vi.fn(),
  cancelCommitReplication: vi.fn(),
  deleteSnapshotDelivery: vi.fn(),
  queryApiVersion: vi.fn(),
  queryCommitAvailability: vi.fn(),
  queryCommitReplicationList: vi.fn(),
  queryGatewayPoolList: vi.fn(),
  querySnapshot: vi.fn(),
  querySnapshotDeliveryList: vi.fn(),
  queryStorageVolume: vi.fn(),
  queryStorageVolumeList: vi.fn(),
  replicateCommit: vi.fn(),
  retryCommitReplication: vi.fn(),
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
        'artifact_commit_replication',
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
    data: { items: [] },
    requestId: 'request-deliveries',
  });
  api.queryCommitReplicationList.mockResolvedValue({
    data: {
      replications: [],
    },
    requestId: 'request-replications',
  });
  api.queryCommitAvailability.mockResolvedValue({
    data: {
      availability: {
        commit_id: commitId,
        data_health: 'available',
        verified_placements: '1',
        missing_objects: '0',
        verified_storage_volume_ids: [],
      },
    },
    requestId: 'request-availability',
  });
  api.queryGatewayPoolList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-gateway-pools',
  });
  api.replicateCommit.mockResolvedValue({
    data: {
      replication: {
        replication_id: 'replication-a',
        tenant_id: 'tenant-a',
        artifact_id: 'artifact-a',
        commit_id: commitId,
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
  api.retryCommitReplication.mockResolvedValue({
    data: {
      replication: {
        replication_id: 'replication-a',
        tenant_id: 'tenant-a',
        artifact_id: 'artifact-a',
        commit_id: commitId,
        target_storage_volume_id: 'volume-a',
        attempt: '2',
        state: 'queued',
        object_set_digest: 'd'.repeat(64),
        completed_objects: '0',
        total_objects: '3',
        completed_bytes: '0',
        total_bytes: '30',
      },
    },
    requestId: 'request-retry-replication',
  });
  api.cancelCommitReplication.mockResolvedValue({
    data: {
      replication: {
        replication_id: 'replication-a',
        tenant_id: 'tenant-a',
        artifact_id: 'artifact-a',
        commit_id: commitId,
        target_storage_volume_id: 'volume-a',
        attempt: '1',
        state: 'cancelled',
        object_set_digest: 'd'.repeat(64),
        completed_objects: '0',
        total_objects: '3',
        completed_bytes: '0',
        total_bytes: '30',
      },
    },
    requestId: 'request-cancel-replication',
  });
  api.createSnapshotDelivery.mockResolvedValue({
    data: { delivery: {}, replayed: false },
    requestId: 'request-create-delivery',
  });
  api.retrySnapshotDelivery.mockResolvedValue({
    data: { delivery: {}, replayed: false },
    requestId: 'request-retry-delivery',
  });
  api.deleteSnapshotDelivery.mockResolvedValue({
    data: { delivery: {}, replayed: false },
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
    expect(wrapper.text()).toContain('Snapshot 已固定，可按需创建独立只读交付');
    expect(wrapper.text()).toContain('Volume A');
    expect(wrapper.text()).toContain(commitId);
    expect(wrapper.text()).toContain('只读');
    expect(wrapper.text()).not.toContain('重试交付');

    wrapper.unmount();
    queryClient.clear();
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
    expect(api.queryCommitReplicationList).not.toHaveBeenCalled();
    expect(api.replicateCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('keeps Delivery unavailable until the target PlacementSet is published', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryStorageVolume).toHaveBeenCalledWith('tenant-a', 'volume-a');
    expect(wrapper.text()).toContain('FUSE不可用');
    expect(wrapper.text()).toContain('全部复制不可用');
    expect(wrapper.text()).toContain('硬链接不可用');
    expect(wrapper.text()).toContain('请先将 Commit 复制到当前目标 Volume');
    const deliveryModeInputs = wrapper.findAll<HTMLInputElement>('.el-segmented__item-input');
    await deliveryModeInputs[1]!.setValue(true);
    await flushPromises();
    expect(wrapper.text()).toContain('请先将 Commit 复制到当前目标 Volume');

    wrapper.unmount();
    queryClient.clear();
  });

  it('keeps Hardlink gated until a WholeFile Commit is replicated', async () => {
    const { wrapper, queryClient } = await mountPage('ready', ['snapshot.read'], 'whole_file');

    expect(wrapper.text()).toContain('硬链接不可用');
    expect(wrapper.text()).toContain('请先将 Commit 复制到当前目标 Volume');

    wrapper.unmount();
    queryClient.clear();
  });

  it('creates a Commit replication with project scope and a bounded stable request ID', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryCommitReplicationList).toHaveBeenCalledWith({
      tenant_id: 'tenant-a',
      commit_id: commitId,
    });
    expect(api.queryGatewayPoolList).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain('路由由 Central 校验');

    await wrapper
      .findAllComponents(ElButton)
      .find((button) => button.text().trim() === '复制 Commit')!
      .trigger('click');
    await flushPromises();

    const request = api.replicateCommit.mock.calls[0]?.[0] as
      CreateCommitReplicationRequest | undefined;
    expect(request).toMatchObject({
      tenant_id: 'tenant-a',
      project_id: 'project-a',
      artifact_id: 'artifact-a',
      commit_id: commitId,
      target_storage_volume_id: 'volume-a',
    });
    expect(request?.request_id).toMatch(/^commit-replicate-[a-f0-9]{32}$/);
    expect(request?.request_id.length).toBeLessThanOrEqual(128);

    wrapper.unmount();
    queryClient.clear();
  });

  it('restores and cancels an active Commit replication after opening the page', async () => {
    api.queryCommitReplicationList.mockResolvedValueOnce({
      data: {
        replications: [
          {
            replication_id: 'replication-active',
            tenant_id: 'tenant-a',
            artifact_id: 'artifact-a',
            commit_id: commitId,
            target_storage_volume_id: 'volume-a',
            attempt: '7',
            state: 'transferring',
            object_set_digest: 'd'.repeat(64),
            completed_objects: '2',
            total_objects: '3',
            completed_bytes: '20',
            total_bytes: '30',
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

    expect(api.cancelCommitReplication.mock.calls[0]?.[0]).toEqual({
      tenant_id: 'tenant-a',
      replication_id: 'replication-active',
      expected_attempt: '7',
    });
    expect(api.replicateCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('retries the failed task instead of creating another task for the same target', async () => {
    api.queryCommitReplicationList.mockResolvedValueOnce({
      data: {
        replications: [
          {
            replication_id: 'replication-failed',
            tenant_id: 'tenant-a',
            artifact_id: 'artifact-a',
            commit_id: commitId,
            target_storage_volume_id: 'volume-a',
            attempt: '3',
            state: 'failed',
            object_set_digest: 'd'.repeat(64),
            completed_objects: '1',
            total_objects: '3',
            completed_bytes: '10',
            total_bytes: '30',
            issue: { code: 'ROUTE_LOST', message: 'Route lease expired', retryable: true },
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

    expect(api.retryCommitReplication.mock.calls[0]?.[0]).toEqual({
      tenant_id: 'tenant-a',
      replication_id: 'replication-failed',
      expected_attempt: '3',
    });
    expect(api.replicateCommit).not.toHaveBeenCalled();

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
