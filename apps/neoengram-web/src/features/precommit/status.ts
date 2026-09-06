import type {
  WorkspaceState,
  WorkspaceStorageAvailability,
  WorkspaceView,
  PreCommitPhase,
  PreCommitState,
  PreCommitView,
} from '@/api/types';

export const preCommitPhaseLabels: Record<PreCommitPhase, string> = {
  queued: '等待处理',
  scanning: '扫描文件',
  hashing: '计算内容摘要',
  uploading: '上传变化数据',
  validating: '一致性校验',
  idle: '处理完成',
};

export const preCommitStateLabels: Record<PreCommitState, string> = {
  running: '处理中',
  ready: '可提交',
  abnormal: '已阻断',
  cancelled: '已取消',
  committed: '已提交',
};

export function preCommitStateTagType(
  state: PreCommitState,
): 'warning' | 'success' | 'danger' | 'info' {
  if (state === 'running') return 'warning';
  if (state === 'ready' || state === 'committed') return 'success';
  if (state === 'abnormal') return 'danger';
  return 'info';
}

export function preCommitPollInterval(state?: PreCommitState): 1000 | false {
  return state === 'running' ? 1000 : false;
}

export function canCommitPreCommit(precommit?: PreCommitView): boolean {
  return Boolean(
    precommit &&
    precommit.state === 'ready' &&
    precommit.phase === 'idle' &&
    precommit.candidate_index_version &&
    precommit.blockers.length === 0,
  );
}

export function workspaceLifecycleLabel(state: WorkspaceState): string {
  return { creating: '创建中', ready: '已物化', abnormal: '异常' }[state];
}

export function workspaceLifecycleTagType(state: WorkspaceState): 'warning' | 'success' | 'danger' {
  if (state === 'creating') return 'warning';
  if (state === 'abnormal') return 'danger';
  return 'success';
}

type WorkspaceRuntimeState = Pick<WorkspaceView, 'state'> &
  Partial<Pick<WorkspaceView, 'active_precommit_id' | 'storage_availability'>>;

export const WORKSPACE_ACTIVE_POLL_INTERVAL_MS = 1_000;
export const WORKSPACE_IDLE_POLL_INTERVAL_MS = 5_000;

export function workspacePollInterval(
  workspace: WorkspaceRuntimeState | undefined,
): typeof WORKSPACE_ACTIVE_POLL_INTERVAL_MS | typeof WORKSPACE_IDLE_POLL_INTERVAL_MS {
  return workspace?.state === 'creating' || workspace?.active_precommit_id
    ? WORKSPACE_ACTIVE_POLL_INTERVAL_MS
    : WORKSPACE_IDLE_POLL_INTERVAL_MS;
}

export function workspaceListPollInterval(
  workspaces: readonly WorkspaceRuntimeState[],
): typeof WORKSPACE_ACTIVE_POLL_INTERVAL_MS | typeof WORKSPACE_IDLE_POLL_INTERVAL_MS {
  return workspaces.some(
    (workspace) => workspace.state === 'creating' || workspace.active_precommit_id,
  )
    ? WORKSPACE_ACTIVE_POLL_INTERVAL_MS
    : WORKSPACE_IDLE_POLL_INTERVAL_MS;
}

export function workspaceStorageAvailability(
  workspace: Partial<Pick<WorkspaceView, 'state' | 'storage_availability'>> | undefined,
): WorkspaceStorageAvailability {
  const availability = workspace?.storage_availability;
  return availability === 'ready' || availability === 'degraded' || availability === 'unavailable'
    ? availability
    : 'unknown';
}

export function workspaceStorageAvailabilityLabel(
  availability: WorkspaceStorageAvailability,
): string {
  return {
    ready: '存储可达',
    degraded: '存储降级',
    unavailable: '存储不可达',
    unknown: '存储状态未知',
  }[availability];
}

export function workspaceStorageAvailabilityTagType(
  availability: WorkspaceStorageAvailability,
): 'warning' | 'success' | 'danger' | 'info' {
  if (availability === 'ready') return 'success';
  if (availability === 'degraded') return 'warning';
  if (availability === 'unavailable') return 'danger';
  return 'info';
}

export function isWorkspaceOperational(workspace: WorkspaceRuntimeState | undefined): boolean {
  return workspace?.state === 'ready' && workspaceStorageAvailability(workspace) === 'ready';
}

export function workspaceOperationUnavailableReason(
  workspace: WorkspaceRuntimeState | undefined,
): string | undefined {
  if (!workspace) return 'Workspace 状态未知';
  if (workspace.state === 'creating') return 'Workspace 正在创建';
  if (workspace.state === 'abnormal') return 'Workspace 生命周期异常';

  const availability = workspaceStorageAvailability(workspace);
  if (availability === 'degraded') return 'StorageVolume 当前处于降级状态';
  if (availability === 'unavailable') return 'StorageVolume 当前不可达';
  if (availability === 'unknown') return 'StorageVolume 状态未知';
  return undefined;
}
