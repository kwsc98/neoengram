import type {
  PlaygroundState,
  PlaygroundStorageAvailability,
  PlaygroundView,
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

export function playgroundLifecycleLabel(state: PlaygroundState): string {
  return { creating: '创建中', ready: '已物化', abnormal: '异常' }[state];
}

export function playgroundLifecycleTagType(
  state: PlaygroundState,
): 'warning' | 'success' | 'danger' {
  if (state === 'creating') return 'warning';
  if (state === 'abnormal') return 'danger';
  return 'success';
}

type PlaygroundRuntimeState = Pick<PlaygroundView, 'state'> &
  Partial<Pick<PlaygroundView, 'active_precommit_id' | 'storage_availability'>>;

export const PLAYGROUND_ACTIVE_POLL_INTERVAL_MS = 1_000;
export const PLAYGROUND_IDLE_POLL_INTERVAL_MS = 5_000;

export function playgroundPollInterval(
  playground: PlaygroundRuntimeState | undefined,
): typeof PLAYGROUND_ACTIVE_POLL_INTERVAL_MS | typeof PLAYGROUND_IDLE_POLL_INTERVAL_MS {
  return playground?.state === 'creating' || playground?.active_precommit_id
    ? PLAYGROUND_ACTIVE_POLL_INTERVAL_MS
    : PLAYGROUND_IDLE_POLL_INTERVAL_MS;
}

export function playgroundListPollInterval(
  playgrounds: readonly PlaygroundRuntimeState[],
): typeof PLAYGROUND_ACTIVE_POLL_INTERVAL_MS | typeof PLAYGROUND_IDLE_POLL_INTERVAL_MS {
  return playgrounds.some(
    (playground) => playground.state === 'creating' || playground.active_precommit_id,
  )
    ? PLAYGROUND_ACTIVE_POLL_INTERVAL_MS
    : PLAYGROUND_IDLE_POLL_INTERVAL_MS;
}

export function playgroundStorageAvailability(
  playground: Partial<Pick<PlaygroundView, 'state' | 'storage_availability'>> | undefined,
): PlaygroundStorageAvailability {
  const availability = playground?.storage_availability;
  return availability === 'ready' || availability === 'degraded' || availability === 'unavailable'
    ? availability
    : 'unknown';
}

export function playgroundStorageAvailabilityLabel(
  availability: PlaygroundStorageAvailability,
): string {
  return {
    ready: '存储可达',
    degraded: '存储降级',
    unavailable: '存储不可达',
    unknown: '存储状态未知',
  }[availability];
}

export function playgroundStorageAvailabilityTagType(
  availability: PlaygroundStorageAvailability,
): 'warning' | 'success' | 'danger' | 'info' {
  if (availability === 'ready') return 'success';
  if (availability === 'degraded') return 'warning';
  if (availability === 'unavailable') return 'danger';
  return 'info';
}

export function isPlaygroundOperational(playground: PlaygroundRuntimeState | undefined): boolean {
  return playground?.state === 'ready' && playgroundStorageAvailability(playground) === 'ready';
}

export function playgroundOperationUnavailableReason(
  playground: PlaygroundRuntimeState | undefined,
): string | undefined {
  if (!playground) return 'Playground 状态未知';
  if (playground.state === 'creating') return 'Playground 正在创建';
  if (playground.state === 'abnormal') return 'Playground 生命周期异常';

  const availability = playgroundStorageAvailability(playground);
  if (availability === 'degraded') return 'StorageVolume 当前处于降级状态';
  if (availability === 'unavailable') return 'StorageVolume 当前不可达';
  if (availability === 'unknown') return 'StorageVolume 状态未知';
  return undefined;
}
