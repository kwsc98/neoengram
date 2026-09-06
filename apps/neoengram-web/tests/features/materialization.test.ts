import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import { defineComponent, h, ref } from 'vue';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type {
  MaterializationView,
  QueryCommitAvailabilityV2Response,
  QueryCommitCoverageResponse,
  TaskMutationResponse,
  TaskView,
} from '@/api/types';
import {
  isMaterializationActive,
  isMaterializationRetryable,
  materializationRequestId,
  materializationRetryRequestId,
  useCommitMaterialization,
} from '@/features/materialization';

const api = vi.hoisted(() => ({
  cancelTask: vi.fn(),
  materializeCommit: vi.fn(),
  queryCommitAvailabilityV2: vi.fn(),
  queryCommitCoverage: vi.fn(),
  queryTaskList: vi.fn(),
  retryTask: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const requestScope = {
  tenantId: 'tenant-a',
  projectId: 'project-a',
  artifactId: 'artifact-a',
  commitId: 'c'.repeat(64),
};

function result<T>(data: T): { data: T; requestId: string } {
  return { data, requestId: 'request-test' };
}

function materialization(
  state: MaterializationView['state'],
  overrides: Partial<MaterializationView> = {},
): MaterializationView {
  return {
    materialization_id: 'materialization-a',
    tenant_id: requestScope.tenantId,
    artifact_id: requestScope.artifactId,
    object_namespace_id: requestScope.artifactId,
    commit_id: requestScope.commitId,
    target_storage_volume_id: 'volume-a',
    purpose: 'copy',
    plan_revision: '1',
    coverage_goal: 'complete',
    state,
    object_set_digest: 'd'.repeat(64),
    verified_objects: '0',
    total_objects: '2',
    verified_bytes: '0',
    total_bytes: '20',
    missing_objects: '2',
    missing_bytes: '20',
    source_count: '1',
    ...overrides,
  };
}

function task(value: MaterializationView, state: TaskView['state'] = 'queued'): TaskView {
  return {
    task_id: value.materialization_id,
    intent_kind: 'commit.materialize',
    purpose: value.purpose,
    state,
    tenant_id: value.tenant_id,
    primary_resource: { resource_kind: 'materialization', resource_id: value.materialization_id },
    resource_links: [
      { resource_kind: 'project', resource_id: requestScope.projectId, role: 'related' },
      ...(value.artifact_id
        ? [{ resource_kind: 'artifact', resource_id: value.artifact_id, role: 'related' as const }]
        : []),
      {
        resource_kind: 'object_namespace',
        resource_id: value.object_namespace_id,
        role: 'related',
      },
      { resource_kind: 'commit', resource_id: value.commit_id, role: 'source' },
      {
        resource_kind: 'storage_volume',
        resource_id: value.target_storage_volume_id,
        role: 'target',
      },
    ],
    execution_id: `execution-${value.materialization_id}`,
    execution_key_digest: value.object_set_digest,
    execution_reused: false,
    current_stage: {
      stage_key: state === 'succeeded' ? 'finalize' : 'transfer',
      stage_kind: state === 'succeeded' ? 'finalize' : 'transfer',
      ordinal: state === 'succeeded' ? '6' : '3',
      dependencies: state === 'succeeded' ? ['publish_coverage'] : ['plan'],
      state: state === 'queued' ? 'ready' : state,
      stage_attempt: value.plan_revision,
      progress: {
        completed: value.verified_objects,
        total: value.total_objects,
        completed_bytes: value.verified_bytes,
        total_bytes: value.total_bytes,
      },
      created_at_unix_ms: '1',
      updated_at_unix_ms: '1',
      resource_version: '1',
    },
    stages: [],
    request_id: 'request-test',
    request_digest: 'e'.repeat(64),
    actor: 'test-user',
    attempt: value.plan_revision,
    progress: {
      completed: value.verified_objects,
      total: value.total_objects,
      completed_bytes: value.verified_bytes,
      total_bytes: value.total_bytes,
    },
    deadline_unix_ms: '9999999999999',
    created_at_unix_ms: '1',
    updated_at_unix_ms: '1',
    resource_version: '1',
    origin: 'user',
    executable: true,
  };
}

const availability: QueryCommitAvailabilityV2Response = {
  availability: {
    object_namespace_id: requestScope.artifactId,
    commit_id: requestScope.commitId,
    object_count: '2',
    content_presence: 'available',
    source_serving: 'available',
    durability: 'satisfied',
    target_coverage: 'not_requested',
    view_readiness: 'ready',
    complete_volume_count: '1',
    missing_objects: [],
    verified_storage_volume_ids: ['volume-source'],
  },
};

function prepareQueries(
  materializations: MaterializationView[] = [],
  coverage: QueryCommitCoverageResponse['coverage'] = [],
): void {
  api.queryTaskList.mockResolvedValue(
    result({
      items: materializations.map((value) =>
        task(value, value.state === 'complete' ? 'succeeded' : 'queued'),
      ),
    }),
  );
  api.queryCommitCoverage.mockResolvedValue(result<QueryCommitCoverageResponse>({ coverage }));
  api.queryCommitAvailabilityV2.mockResolvedValue(result(availability));
  api.materializeCommit.mockResolvedValue(
    result({ materialization: materialization('queued'), request_replayed: false }),
  );
  api.retryTask.mockResolvedValue(
    result<TaskMutationResponse>({
      task: task(materialization('queued', { plan_revision: '2' })),
      request_replayed: false,
      execution_reused: false,
    }),
  );
  api.cancelTask.mockResolvedValue(
    result<TaskMutationResponse>({
      task: task(materialization('cancelled'), 'cancelled'),
      request_replayed: false,
      execution_reused: false,
    }),
  );
}

async function mountFeature() {
  const state = ref<ReturnType<typeof useCommitMaterialization>>();
  const Harness = defineComponent({
    setup() {
      state.value = useCommitMaterialization(requestScope, {
        enabled: true,
        activePollIntervalMs: false,
        idlePollIntervalMs: false,
      });
      return () => h('div');
    },
  });
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(Harness, {
    global: { plugins: [[VueQueryPlugin, { queryClient }]] },
  });
  await flushPromises();
  return { queryClient, state, wrapper };
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('Commit materialization feature', () => {
  it('classifies active and repairable states including degraded completed Jobs', () => {
    expect(isMaterializationActive('waiting_for_sources')).toBe(true);
    expect(isMaterializationActive('stalled')).toBe(false);
    expect(isMaterializationRetryable('complete')).toBe(true);
    expect(isMaterializationRetryable('stalled')).toBe(true);
    expect(isMaterializationRetryable('materializing')).toBe(false);
  });

  it('builds bounded deterministic request IDs for create and retry', () => {
    const requestId = materializationRequestId({
      ...requestScope,
      targetStorageVolumeId: 'volume-a',
    });
    expect(requestId).toBe(
      materializationRequestId({ ...requestScope, targetStorageVolumeId: 'volume-a' }),
    );
    expect(requestId.length).toBeLessThanOrEqual(128);
    expect(requestId).toMatch(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/);

    const retryRequestId = materializationRetryRequestId('materialization-a', '1');
    expect(retryRequestId).toMatch(/^commit-materialize-retry-/);
    expect(retryRequestId).not.toBe(materializationRetryRequestId('materialization-a', '2'));
  });

  it('uses canonical v2 query requests and a single create path for a new target', async () => {
    prepareQueries();
    const { queryClient, state, wrapper } = await mountFeature();
    expect(state.value?.materializationsQuery.isSuccess).toBe(true);
    expect(api.queryTaskList).toHaveBeenCalledWith({
      tenant_id: requestScope.tenantId,
      object_namespace_id: requestScope.artifactId,
      commit_id: requestScope.commitId,
      intent_kind: ['commit.materialize'],
      page_size: 100,
    });
    expect(api.queryCommitCoverage).toHaveBeenCalledWith({
      tenant_id: requestScope.tenantId,
      object_namespace_id: requestScope.artifactId,
      commit_id: requestScope.commitId,
      page_size: 100,
    });
    expect(api.queryCommitAvailabilityV2).toHaveBeenCalledWith({
      tenant_id: requestScope.tenantId,
      object_namespace_id: requestScope.artifactId,
      commit_id: requestScope.commitId,
      page_size: 100,
    });

    const action = await state.value!.materializeOrRepair('volume-target');
    expect(action.mode).toBe('created');
    expect(api.materializeCommit.mock.calls[0]?.[0]).toEqual(
      expect.objectContaining({
        tenant_id: requestScope.tenantId,
        project_id: requestScope.projectId,
        artifact_id: requestScope.artifactId,
        object_namespace_id: requestScope.artifactId,
        commit_id: requestScope.commitId,
        target_storage_volume_id: 'volume-target',
        purpose: 'copy',
        coverage_goal: 'complete',
      }),
    );
    expect(api.retryTask).not.toHaveBeenCalled();
    wrapper.unmount();
    queryClient.clear();
  });

  it('repairs an existing degraded target through the same materialization action', async () => {
    prepareQueries(
      [materialization('complete')],
      [
        {
          object_namespace_id: requestScope.artifactId,
          commit_id: requestScope.commitId,
          storage_volume_id: 'volume-a',
          placement_generation: '1',
          object_set_digest: 'd'.repeat(64),
          total_objects: '2',
          verified_objects: '1',
          total_bytes: '20',
          verified_bytes: '10',
          missing_objects: '1',
          missing_bytes: '10',
          state: 'partial',
        },
      ],
    );
    const { queryClient, state, wrapper } = await mountFeature();
    const action = await state.value!.materializeOrRepair('volume-a');
    expect(action.mode).toBe('repaired');
    const retryRequest = api.retryTask.mock.calls[0]?.[0] as unknown;
    expect(retryRequest).toMatchObject({
      tenant_id: requestScope.tenantId,
      task_id: 'materialization-a',
    });
    if (!retryRequest || typeof retryRequest !== 'object') {
      throw new Error('retry request was not captured');
    }
    expect(api.materializeCommit).not.toHaveBeenCalled();
    wrapper.unmount();
    queryClient.clear();
  });

  it('does not create a duplicate task when the target already has an active task', async () => {
    prepareQueries([
      materialization('materializing', { target_storage_volume_id: 'volume-target' }),
    ]);
    const { queryClient, state, wrapper } = await mountFeature();

    const action = await state.value!.materializeOrRepair('volume-target');

    expect(action.mode).toBe('in_flight');
    expect(action.materialization?.target_storage_volume_id).toBe('volume-target');
    expect(api.materializeCommit).not.toHaveBeenCalled();
    expect(api.retryTask).not.toHaveBeenCalled();
    wrapper.unmount();
    queryClient.clear();
  });
});
