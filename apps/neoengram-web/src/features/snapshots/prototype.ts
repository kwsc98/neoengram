import type { SnapshotPhase, SnapshotState } from '@/api/types';

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
    idle: '无运行中任务',
  }[phase];
}
