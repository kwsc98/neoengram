import type { PlaygroundState, PreCommitPhase, PreCommitState, PreCommitView } from '@/api/types';

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

export function playgroundAvailabilityLabel(state: PlaygroundState): string {
  return { creating: '创建中', ready: '可用', abnormal: '异常' }[state];
}

export function playgroundAvailabilityTagType(
  state: PlaygroundState,
): 'warning' | 'success' | 'danger' {
  if (state === 'creating') return 'warning';
  if (state === 'abnormal') return 'danger';
  return 'success';
}
