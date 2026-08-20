import type { DeletionOperationState, ResourceRef } from '@/api/types';

export type LifecycleTagType = 'success' | 'warning' | 'danger' | 'info';

export function resourceRefId(resource: ResourceRef): string {
  switch (resource.type) {
    case 'storage_volume':
      return resource.storage_volume_id;
    case 'artifact':
      return resource.artifact_id;
    case 'playground':
      return resource.playground_id;
    case 'snapshot':
      return resource.snapshot_id;
  }
}

export function resourceRefLabel(resource: ResourceRef): string {
  switch (resource.type) {
    case 'storage_volume':
      return 'StorageVolume';
    case 'artifact':
      return 'Artifact';
    case 'playground':
      return 'Playground';
    case 'snapshot':
      return 'Snapshot';
  }
}

export function resourceRefScope(resource: ResourceRef): string {
  if (resource.type === 'artifact') return `${resource.project_id} / ${resource.artifact_id}`;
  if (resource.type === 'playground') {
    return `${resource.project_id} / ${resource.artifact_id} / ${resource.playground_id}`;
  }
  return resourceRefId(resource);
}

export function lifecycleResourceVersion(resource: unknown): string {
  if (
    resource &&
    typeof resource === 'object' &&
    'resource_version' in resource &&
    typeof resource.resource_version === 'string'
  ) {
    return resource.resource_version;
  }
  throw new Error('Lifecycle-enabled resource response is missing resource_version');
}

export function deletionStateLabel(state: DeletionOperationState): string {
  return {
    requested: '已请求',
    quiescing: '停止访问',
    quarantining: '隔离中',
    recoverable: '可恢复',
    restoring: '恢复中',
    purging: '永久清理中',
    finalizing: '收尾中',
    completed: '已完成',
    blocked: '已阻塞',
    failed: '失败',
  }[state];
}

export function deletionStateTagType(state: DeletionOperationState): LifecycleTagType {
  if (state === 'recoverable' || state === 'completed') return 'success';
  if (state === 'blocked' || state === 'failed') return 'danger';
  if (state === 'purging' || state === 'finalizing') return 'warning';
  return 'info';
}

export function deletionCanRestore(state: DeletionOperationState): boolean {
  return ['requested', 'quiescing', 'quarantining', 'recoverable', 'blocked', 'failed'].includes(
    state,
  );
}

export function deletionCanRetry(state: DeletionOperationState): boolean {
  return state === 'blocked' || state === 'failed';
}
