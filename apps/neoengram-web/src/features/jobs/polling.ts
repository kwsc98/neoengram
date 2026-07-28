import type { JobState } from '@/api/types';

const terminalStates = new Set<JobState>([
  'succeeded',
  'conflicted',
  'rejected',
  'failed',
  'cancelled',
  'timed_out',
  'recovery_required',
]);

export function jobPollInterval(state: JobState | undefined): number | false {
  if (!state || state === 'prepared' || terminalStates.has(state)) return false;
  return 2000;
}
