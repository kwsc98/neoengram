import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount, type VueWrapper } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import SnapshotDetailPage from '@/pages/SnapshotDetailPage.vue';

const api = vi.hoisted(() => ({
  queryArtifactCommitDiff: vi.fn(),
  querySnapshot: vi.fn(),
  querySnapshotActivityList: vi.fn(),
  querySnapshotDatasetProfile: vi.fn(),
  querySnapshotFileList: vi.fn(),
  retrySnapshotDelivery: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const ElButtonStub = {
  emits: ['click'],
  template: '<button type="button" @click="$emit(\'click\')"><slot /></button>',
};

function snapshot(state: 'creating' | 'ready' | 'abnormal' = 'ready') {
  return {
    snapshot_id: 'snapshot-a',
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    commit_id: 'commit-a',
    storage_volume_id: 'volume-a',
    region: 'cn-shanghai',
    message: 'Freeze training data',
    tag_names: ['dataset/v1'],
    state,
    phase: state === 'creating' ? ('materializing' as const) : ('idle' as const),
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

function mockQueries(state: 'creating' | 'ready' | 'abnormal' = 'ready'): void {
  api.querySnapshot.mockResolvedValue({
    data: { snapshot: snapshot(state) },
    requestId: 'request-snapshot',
  });
  api.queryArtifactCommitDiff.mockResolvedValue({
    data: {
      diff: {
        target_commit: {
          commit_id: 'commit-a',
          message: 'Freeze training data',
          tag_names: ['dataset/v1'],
          created_at_unix_ms: '1',
        },
        summary: {
          files_added: '1',
          files_modified: '0',
          files_deleted: '0',
          files_renamed: '0',
          bytes_added: '30',
          bytes_removed: '0',
        },
        changes: [],
      },
    },
    requestId: 'request-diff',
  });
  api.querySnapshotFileList.mockResolvedValue({
    data: {
      items: [
        {
          path: 'dataset/a.parquet',
          entry_type: 'file',
          size_bytes: '30',
          format: 'parquet',
          row_count: '3',
        },
      ],
    },
    requestId: 'request-files',
  });
  api.querySnapshotActivityList.mockResolvedValue({
    data: {
      items: [
        {
          activity_id: 'activity-a',
          activity_type: 'ready',
          summary: 'Integrity verified',
          phase: 'idle',
          created_at_unix_ms: '2',
        },
      ],
    },
    requestId: 'request-activity',
  });
  api.querySnapshotDatasetProfile.mockResolvedValue({
    data: {
      profile: {
        state: 'ready',
        summary: {
          format_count: 1,
          logical_file_count: '3',
          logical_size_bytes: '30',
          row_count: '3',
          field_count: 1,
        },
        schema: { fields: [{ name: 'id', data_type: 'string', nullable: false }] },
      },
    },
    requestId: 'request-profile',
  });
  api.retrySnapshotDelivery.mockResolvedValue({
    data: { snapshot: snapshot('creating'), replayed: false },
    requestId: 'request-retry',
  });
}

async function mountPage(tab = 'overview', state: 'creating' | 'ready' | 'abnormal' = 'ready') {
  mockQueries(state);
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/:snapshotId',
        name: 'snapshot-detail',
        component: SnapshotDetailPage,
      },
      { path: '/snapshots', name: 'snapshot-list', component: { template: '<div />' } },
      { path: '/artifact', name: 'artifact-detail', component: { template: '<div />' } },
    ],
  });
  await router.push(
    `/tenants/tenant-a/projects/project-a/artifacts/artifact-a/snapshots/snapshot-a?tab=${tab}`,
  );
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = shallowMount(SnapshotDetailPage, {
    global: {
      plugins: [ElementPlus, [VueQueryPlugin, { queryClient }], router],
      stubs: { ElButton: ElButtonStub, PageHeading: false },
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

describe('Snapshot detail page', () => {
  it.each([
    [
      'files',
      'querySnapshotFileList',
      { tenant_id: 'tenant-a', snapshot_id: 'snapshot-a', page_size: 50 },
    ],
    [
      'activity',
      'querySnapshotActivityList',
      { tenant_id: 'tenant-a', snapshot_id: 'snapshot-a', page_size: 25 },
    ],
    [
      'profile',
      'querySnapshotDatasetProfile',
      { tenant_id: 'tenant-a', snapshot_id: 'snapshot-a' },
    ],
  ] as const)(
    'loads the %s resource from its public query API',
    async (tab, operation, request) => {
      const { wrapper, queryClient } = await mountPage(tab);

      expect(api[operation]).toHaveBeenCalledWith(request);

      wrapper.unmount();
      queryClient.clear();
    },
  );

  it('retries an Abnormal delivery with a stable retry request identity', async () => {
    const { wrapper, queryClient } = await mountPage('overview', 'abnormal');
    api.retrySnapshotDelivery
      .mockRejectedValueOnce(new TypeError('transport interrupted'))
      .mockResolvedValueOnce({
        data: { snapshot: snapshot('creating'), replayed: false },
        requestId: 'request-retry-success',
      });

    await elementButton(wrapper, '重试交付').trigger('click');
    await flushPromises();
    await elementButton(wrapper, '重试交付').trigger('click');
    await flushPromises();

    expect(api.retrySnapshotDelivery).toHaveBeenCalledTimes(2);
    const retryRequests = (api.retrySnapshotDelivery.mock.calls as unknown[][]).map(
      ([request]) => request as Record<string, unknown>,
    );
    expect(retryRequests[0]).toMatchObject({
      tenant_id: 'tenant-a',
      snapshot_id: 'snapshot-a',
    });
    expect(retryRequests[0]?.retry_request_id).toMatch(/^snapshot-retry-/);
    expect(retryRequests[1]).toEqual(retryRequests[0]);

    wrapper.unmount();
    queryClient.clear();
  });
});
