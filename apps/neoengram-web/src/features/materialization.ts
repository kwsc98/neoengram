import { useMutation, useQuery } from '@tanstack/vue-query';
import { computed, toValue, type MaybeRefOrGetter } from 'vue';

import {
  cancelTask,
  materializeCommit,
  queryTaskList,
  queryCommitAvailabilityV2,
  queryCommitCoverage,
  retryTask,
} from '@/api/operations';
import type { ApiResult } from '@/api/operations';
import type {
  CreateCommitMaterializationRequest,
  CreateCommitMaterializationResponse,
  MaterializationView,
  QueryCommitAvailabilityV2Response,
  QueryCommitCoverageResponse,
  TaskMutationResponse,
  TaskView,
} from '@/api/types';

/** The route/namespace scope shared by every materialization operation. */
export interface CommitMaterializationScope {
  tenantId: MaybeRefOrGetter<string>;
  projectId: MaybeRefOrGetter<string>;
  artifactId: MaybeRefOrGetter<string>;
  commitId: MaybeRefOrGetter<string>;
  /** Defaults to artifactId while the initial v2 namespace mapping is one-to-one. */
  objectNamespaceId?: MaybeRefOrGetter<string>;
}

export interface CommitMaterializationOptions {
  /** Disable all requests while the parent page is hidden or lacks the capability. */
  enabled?: MaybeRefOrGetter<boolean>;
  /** Poll interval for active materialization Jobs. Defaults to one second. */
  activePollIntervalMs?: number | false;
  /** Poll interval for idle Jobs and derived integrity views. Defaults to five seconds. */
  idlePollIntervalMs?: number | false;
}

export type MaterializationCoverageGoal = Exclude<
  CreateCommitMaterializationRequest['coverage_goal'],
  undefined
>;

export type MaterializationActionMode = 'created' | 'repaired' | 'in_flight' | 'noop';

export interface MaterializationActionResult {
  mode: MaterializationActionMode;
  materialization?: MaterializationView;
  result?: ApiResult<CreateCommitMaterializationResponse> | ApiResult<TaskMutationResponse>;
}

interface MaterializationTaskListResponse {
  materializations: MaterializationView[];
  next_cursor?: string;
}

function hash32(value: string, seed: number): string {
  let hash = seed;
  for (let index = 0; index < value.length; index += 1) {
    hash = Math.imul(hash ^ value.charCodeAt(index), 16_777_619);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
}

const activeStates = new Set<MaterializationView['state']>([
  'queued',
  'planning',
  'waiting_for_sources',
  'materializing',
  'verifying',
]);

function materializationFromTask(task: TaskView): MaterializationView {
  const state: MaterializationView['state'] =
    task.state === 'running'
      ? 'materializing'
      : task.state === 'waiting'
        ? 'waiting_for_sources'
        : task.state === 'succeeded'
          ? 'complete'
          : task.state;
  return {
    materialization_id: task.task_id,
    tenant_id: task.tenant_id,
    ...(task.artifact_id === undefined ? {} : { artifact_id: task.artifact_id }),
    object_namespace_id: task.object_namespace_id ?? task.artifact_id ?? 'unknown',
    commit_id: task.commit_id ?? '',
    target_storage_volume_id: task.storage_volume_id ?? '',
    plan_revision: task.attempt,
    coverage_goal: 'complete',
    state,
    object_set_digest: '',
    verified_objects: task.progress.completed,
    total_objects: task.progress.total,
    verified_bytes: task.progress.completed_bytes,
    total_bytes: task.progress.total_bytes,
    missing_objects: '0',
    missing_bytes: '0',
    source_count: '0',
    ...(task.issue
      ? {
          issue: {
            code: task.issue.code,
            message: task.issue.message,
            retryable: task.issue.retryable,
          },
        }
      : {}),
  };
}

const retryableStates = new Set<MaterializationView['state']>([
  'complete',
  'stalled',
  'failed',
  'cancelled',
]);

export function isMaterializationActive(state: MaterializationView['state']): boolean {
  return activeStates.has(state);
}

export function isMaterializationRetryable(state: MaterializationView['state']): boolean {
  return retryableStates.has(state);
}

export function materializationRequestId(scope: {
  tenantId: string;
  projectId: string;
  artifactId: string;
  commitId: string;
  targetStorageVolumeId: string;
}): string {
  const canonicalScope = [
    scope.tenantId,
    scope.projectId,
    scope.artifactId,
    scope.commitId,
    scope.targetStorageVolumeId,
  ]
    .map((part) => `${part.length}:${part}`)
    .join('|');
  const digest = [
    hash32(canonicalScope, 2_166_136_261),
    hash32(canonicalScope, 2_166_136_261 ^ 0x9e3779b9),
    hash32(canonicalScope, 2_166_136_261 ^ 0x85ebca6b),
    hash32(canonicalScope, 2_166_136_261 ^ 0xc2b2ae35),
  ].join('');
  return `commit-materialize-${digest}`;
}

export function materializationRetryRequestId(
  materializationId: string,
  expectedPlanRevision: string,
): string {
  const canonicalScope = `${materializationId.length}:${materializationId}|${expectedPlanRevision.length}:${expectedPlanRevision}`;
  const digest = [
    hash32(canonicalScope, 2_166_136_261),
    hash32(canonicalScope, 2_166_136_261 ^ 0x9e3779b9),
    hash32(canonicalScope, 2_166_136_261 ^ 0x85ebca6b),
    hash32(canonicalScope, 2_166_136_261 ^ 0xc2b2ae35),
  ].join('');
  return `commit-materialize-retry-${digest}`;
}

/**
 * Shared Commit materialization state and actions.
 *
 * Both creating a new target copy and repairing a degraded target use the same
 * Materialization Job model. `materializeOrRepair` only chooses whether to call
 * the create or retry endpoint after looking at the current target evidence.
 */
export function useCommitMaterialization(
  scope: CommitMaterializationScope,
  options: CommitMaterializationOptions = {},
) {
  const activePollIntervalMs = options.activePollIntervalMs ?? 1_000;
  const idlePollIntervalMs = options.idlePollIntervalMs ?? 5_000;

  const values = computed(() => ({
    tenantId: toValue(scope.tenantId),
    projectId: toValue(scope.projectId),
    artifactId: toValue(scope.artifactId),
    commitId: toValue(scope.commitId),
    objectNamespaceId: toValue(scope.objectNamespaceId) ?? toValue(scope.artifactId),
  }));
  const scopeReady = computed(() => {
    const current = values.value;
    return Boolean(
      current.tenantId &&
      current.projectId &&
      current.artifactId &&
      current.commitId &&
      current.objectNamespaceId,
    );
  });
  const enabled = computed(
    () => scopeReady.value && toValue(options.enabled === undefined ? true : options.enabled),
  );
  const queryKeyScope = computed(() => {
    const current = values.value;
    return [current.tenantId, current.objectNamespaceId, current.commitId] as const;
  });

  const materializationsQuery = useQuery({
    queryKey: computed(() => ['commit-materializations', ...queryKeyScope.value]),
    queryFn: (): Promise<ApiResult<MaterializationTaskListResponse>> => {
      const current = values.value;
      return queryTaskList({
        tenant_id: current.tenantId,
        object_namespace_id: current.objectNamespaceId,
        commit_id: current.commitId,
        task_kind: ['commit.materialize'],
        page_size: 100,
      }).then((result) => ({
        requestId: result.requestId,
        data: {
          materializations: result.data.items.map(materializationFromTask),
          ...(result.data.next_cursor === undefined
            ? {}
            : { next_cursor: result.data.next_cursor }),
        },
      }));
    },
    enabled,
    refetchInterval: (query) => {
      const items = query.state.data?.data.materializations ?? [];
      return items.some((item) => isMaterializationActive(item.state))
        ? activePollIntervalMs
        : idlePollIntervalMs;
    },
    refetchOnWindowFocus: true,
  });

  const coverageQuery = useQuery({
    queryKey: computed(() => ['commit-coverage', ...queryKeyScope.value]),
    queryFn: (): Promise<ApiResult<QueryCommitCoverageResponse>> => {
      const current = values.value;
      return queryCommitCoverage({
        tenant_id: current.tenantId,
        object_namespace_id: current.objectNamespaceId,
        commit_id: current.commitId,
        page_size: 100,
      });
    },
    enabled,
    refetchInterval: idlePollIntervalMs,
    refetchOnWindowFocus: true,
  });

  const availabilityQuery = useQuery({
    queryKey: computed(() => ['commit-availability', ...queryKeyScope.value]),
    queryFn: (): Promise<ApiResult<QueryCommitAvailabilityV2Response>> => {
      const current = values.value;
      return queryCommitAvailabilityV2({
        tenant_id: current.tenantId,
        object_namespace_id: current.objectNamespaceId,
        commit_id: current.commitId,
        page_size: 100,
      });
    },
    enabled,
    refetchInterval: idlePollIntervalMs,
    refetchOnWindowFocus: true,
  });

  const createMutation = useMutation({ mutationFn: materializeCommit });
  const retryMutation = useMutation({ mutationFn: retryTask });
  const cancelMutation = useMutation({ mutationFn: cancelTask });

  const materializations = computed(
    () => materializationsQuery.data.value?.data.materializations ?? [],
  );
  const coverage = computed(() => coverageQuery.data.value?.data.coverage ?? []);
  const availability = computed(() => availabilityQuery.data.value?.data.availability);
  const isBusy = computed(
    () =>
      createMutation.isPending.value ||
      retryMutation.isPending.value ||
      cancelMutation.isPending.value,
  );
  const actionError = computed(
    () => createMutation.error.value ?? retryMutation.error.value ?? cancelMutation.error.value,
  );

  function targetMaterialization(targetStorageVolumeId: string): MaterializationView | undefined {
    const matching = materializations.value.filter(
      (item) => item.target_storage_volume_id === targetStorageVolumeId,
    );
    return (
      matching.find((item) => isMaterializationActive(item.state)) ??
      matching.find((item) => item.state !== 'cancelled') ??
      matching[0]
    );
  }

  function hasCompleteCoverage(targetStorageVolumeId: string): boolean {
    return coverage.value.some(
      (item) => item.storage_volume_id === targetStorageVolumeId && item.state === 'complete',
    );
  }

  async function refresh(): Promise<void> {
    if (!enabled.value) return;
    await Promise.all([
      materializationsQuery.refetch(),
      coverageQuery.refetch(),
      availabilityQuery.refetch(),
    ]);
  }

  /**
   * Refreshes the two authority views used to inspect one replica. The server may derive a
   * different Coverage row for the requested volume, so the scoped requests are intentionally
   * retained even though the composable also refreshes the complete list afterwards.
   */
  async function checkReplica(targetStorageVolumeId: string): Promise<{
    coverage: ApiResult<QueryCommitCoverageResponse>;
    availability: ApiResult<QueryCommitAvailabilityV2Response>;
  }> {
    if (!enabled.value || !targetStorageVolumeId) {
      throw new Error('materialization scope and target_storage_volume_id are required');
    }
    const current = values.value;
    const [coverageResult, availabilityResult] = await Promise.all([
      queryCommitCoverage({
        tenant_id: current.tenantId,
        object_namespace_id: current.objectNamespaceId,
        commit_id: current.commitId,
        storage_volume_id: targetStorageVolumeId,
        page_size: 100,
      }),
      queryCommitAvailabilityV2({
        tenant_id: current.tenantId,
        object_namespace_id: current.objectNamespaceId,
        commit_id: current.commitId,
        target_storage_volume_id: targetStorageVolumeId,
        page_size: 100,
      }),
    ]);
    await refreshMaterializationQueries();
    return { coverage: coverageResult, availability: availabilityResult };
  }

  async function createMaterialization(
    targetStorageVolumeId: string,
    actionOptions: {
      coverageGoal?: MaterializationCoverageGoal;
      requestId?: string;
    } = {},
  ): Promise<ApiResult<CreateCommitMaterializationResponse>> {
    if (!enabled.value || !targetStorageVolumeId) {
      throw new Error('materialization scope and target_storage_volume_id are required');
    }
    const current = values.value;
    const requestId =
      actionOptions.requestId ??
      materializationRequestId({
        tenantId: current.tenantId,
        projectId: current.projectId,
        artifactId: current.artifactId,
        commitId: current.commitId,
        targetStorageVolumeId,
      });
    const result = await createMutation.mutateAsync({
      tenant_id: current.tenantId,
      project_id: current.projectId,
      artifact_id: current.artifactId,
      object_namespace_id: current.objectNamespaceId,
      commit_id: current.commitId,
      target_storage_volume_id: targetStorageVolumeId,
      coverage_goal: actionOptions.coverageGoal ?? 'complete',
      request_id: requestId,
    });
    await refreshMaterializationQueries();
    return result;
  }

  async function repairMaterialization(
    materialization: MaterializationView,
  ): Promise<ApiResult<TaskMutationResponse>> {
    if (!enabled.value) throw new Error('materialization scope is not enabled');
    if (!isMaterializationRetryable(materialization.state)) {
      throw new Error(`materialization ${materialization.materialization_id} is not retryable`);
    }
    const current = values.value;
    const result = await retryMutation.mutateAsync({
      tenant_id: current.tenantId,
      task_id: materialization.materialization_id,
    });
    await refreshMaterializationQueries();
    return result;
  }

  async function materializeOrRepair(
    targetStorageVolumeId: string,
    actionOptions: {
      coverageGoal?: MaterializationCoverageGoal;
      requestId?: string;
    } = {},
  ): Promise<MaterializationActionResult> {
    if (!enabled.value || !targetStorageVolumeId) {
      throw new Error('materialization scope and target_storage_volume_id are required');
    }
    const current = targetMaterialization(targetStorageVolumeId);
    if (hasCompleteCoverage(targetStorageVolumeId)) {
      return { mode: 'noop', ...(current ? { materialization: current } : {}) };
    }
    if (current && isMaterializationActive(current.state)) {
      return { mode: 'in_flight', materialization: current };
    }
    if (current && isMaterializationRetryable(current.state)) {
      const result = await repairMaterialization(current);
      return {
        mode: 'repaired',
        materialization: materializationFromTask(result.data.task),
        result,
      };
    }
    const result = await createMaterialization(targetStorageVolumeId, actionOptions);
    return { mode: 'created', materialization: result.data.materialization, result };
  }

  async function cancelMaterialization(
    materialization: MaterializationView,
  ): Promise<ApiResult<TaskMutationResponse>> {
    if (!enabled.value) throw new Error('materialization scope is not enabled');
    const current = values.value;
    const result = await cancelMutation.mutateAsync({
      tenant_id: current.tenantId,
      task_id: materialization.materialization_id,
    });
    await refreshMaterializationQueries();
    return result;
  }

  async function refreshMaterializationQueries(): Promise<void> {
    if (!enabled.value) return;
    await Promise.all([
      materializationsQuery.refetch(),
      coverageQuery.refetch(),
      availabilityQuery.refetch(),
    ]);
  }

  function resetMutations(): void {
    createMutation.reset();
    retryMutation.reset();
    cancelMutation.reset();
  }

  return {
    values,
    enabled,
    materializationsQuery,
    coverageQuery,
    availabilityQuery,
    createMutation,
    retryMutation,
    cancelMutation,
    materializations,
    coverage,
    availability,
    isBusy,
    actionError,
    targetMaterialization,
    hasCompleteCoverage,
    refresh,
    checkReplica,
    createMaterialization,
    repairMaterialization,
    materializeOrRepair,
    cancelMaterialization,
    resetMutations,
  };
}
