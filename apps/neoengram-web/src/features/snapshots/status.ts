import type {
  DatasetProfileState,
  SnapshotActivityType,
  SnapshotIntegrityState,
  SnapshotPhase,
  SnapshotState,
} from '@/api/types';

export function snapshotStateLabel(state: SnapshotState): string {
  return { creating: '创建中', ready: '可用', abnormal: '异常' }[state];
}

export function snapshotStateTagType(state: SnapshotState): 'warning' | 'success' | 'danger' {
  if (state === 'creating') return 'warning';
  if (state === 'abnormal') return 'danger';
  return 'success';
}

export function snapshotPhaseLabel(phase: SnapshotPhase): string {
  return {
    planning: '规划交付',
    materializing: '物化数据',
    verifying: '完整性校验',
    idle: '处理完成',
  }[phase];
}

export function snapshotPollInterval(state?: SnapshotState): 1000 | false {
  return state === 'creating' ? 1000 : false;
}

export function snapshotIntegrityLabel(state: SnapshotIntegrityState): string {
  return { pending: '待校验', verified: '已校验', failed: '校验失败' }[state];
}

export function snapshotIntegrityTagType(
  state: SnapshotIntegrityState,
): 'warning' | 'success' | 'danger' {
  if (state === 'pending') return 'warning';
  if (state === 'failed') return 'danger';
  return 'success';
}

export function snapshotActivityTypeLabel(type: SnapshotActivityType): string {
  return {
    created: 'Snapshot 已创建',
    phase_changed: '交付阶段更新',
    ready: 'Snapshot 已可用',
    failed: '交付失败',
    retry_started: '重新开始交付',
  }[type];
}

export function snapshotActivityTagType(
  type: SnapshotActivityType,
): 'primary' | 'success' | 'danger' | 'warning' {
  if (type === 'ready') return 'success';
  if (type === 'failed') return 'danger';
  if (type === 'retry_started') return 'warning';
  return 'primary';
}

export function datasetProfileStateLabel(state: DatasetProfileState): string {
  return { not_declared: '未声明', ready: '可用', rejected: '已拒绝' }[state];
}

export function datasetProfileTagType(state: DatasetProfileState): 'info' | 'success' | 'danger' {
  if (state === 'ready') return 'success';
  if (state === 'rejected') return 'danger';
  return 'info';
}
