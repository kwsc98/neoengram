import { reactive } from 'vue';

import type { PlaygroundState } from '@/api/types';

export type PreCommitPhase =
  'queued' | 'scanning' | 'hashing' | 'uploading' | 'validating' | 'ready';

export interface ActivePreCommitSummary {
  jobId: string;
  phase: PreCommitPhase;
  progress: number;
  filesCompleted: string;
  filesTotal: string;
  startedAt: string;
}

const activePreCommits = reactive<Record<string, ActivePreCommitSummary>>({
  [preCommitScopeKey('tenant-a', 'project-vision', 'quality-reports', 'nightly-review')]: {
    jobId: 'precommit-nightly-0729',
    phase: 'hashing',
    progress: 52,
    filesCompleted: '9,648',
    filesTotal: '18,554',
    startedAt: '今天 14:06',
  },
  [preCommitScopeKey('tenant-a', 'project-vision', 'road-scenes', 'fog-augmentation')]: {
    jobId: 'precommit-fog-0729',
    phase: 'validating',
    progress: 88,
    filesCompleted: '18,554',
    filesTotal: '18,554',
    startedAt: '今天 13:48',
  },
});

export function preCommitScopeKey(
  tenantId: string,
  projectId: string,
  artifactId: string,
  playgroundId: string,
): string {
  return [tenantId, projectId, artifactId, playgroundId].join('\0');
}

export const preCommitPhaseLabels: Record<PreCommitPhase, string> = {
  queued: '等待 Agent',
  scanning: '扫描文件',
  hashing: '计算内容摘要',
  uploading: '上传新增对象',
  validating: '中心一致性校验',
  ready: '候选待 Review',
};

const phaseProgress: Record<
  PreCommitPhase,
  Pick<ActivePreCommitSummary, 'progress' | 'filesCompleted'>
> = {
  queued: { progress: 8, filesCompleted: '0' },
  scanning: { progress: 28, filesCompleted: '3,184' },
  hashing: { progress: 52, filesCompleted: '9,648' },
  uploading: { progress: 71, filesCompleted: '18,554' },
  validating: { progress: 88, filesCompleted: '18,554' },
  ready: { progress: 100, filesCompleted: '18,554' },
};

export function getActivePreCommit(scopeKey: string): ActivePreCommitSummary | undefined {
  return activePreCommits[scopeKey];
}

export function activePreCommitLabel(scopeKey: string): string | undefined {
  const active = getActivePreCommit(scopeKey);
  return active ? preCommitPhaseLabels[active.phase] : undefined;
}

export function startPrototypePreCommit(
  scopeKey: string,
  options: { jobId?: string; phase?: PreCommitPhase } = {},
): ActivePreCommitSummary {
  const phase = options.phase ?? 'queued';
  const progress = phaseProgress[phase];
  const active = reactive({
    jobId:
      options.jobId ??
      `precommit-${scopeKey.split('\0').at(-1) ?? 'playground'}-${Date.now().toString(36)}`,
    phase,
    progress: progress.progress,
    filesCompleted: progress.filesCompleted,
    filesTotal: '18,554',
    startedAt: '刚刚',
  });
  activePreCommits[scopeKey] = active;
  return active;
}

export function advancePrototypePreCommit(scopeKey: string, phase: PreCommitPhase): void {
  const active = activePreCommits[scopeKey];
  if (!active) return;
  const progress = phaseProgress[phase];
  active.phase = phase;
  active.progress = progress.progress;
  active.filesCompleted = progress.filesCompleted;
}

export function cancelPrototypePreCommit(scopeKey: string): void {
  delete activePreCommits[scopeKey];
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
