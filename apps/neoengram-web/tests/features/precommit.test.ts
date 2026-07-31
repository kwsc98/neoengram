import { describe, expect, it } from 'vitest';

import {
  canCommitPreCommit,
  preCommitPhaseLabels,
  preCommitPollInterval,
} from '@/features/precommit/status';
import type { PreCommitView } from '@/api/types';

function precommit(overrides: Partial<PreCommitView> = {}): PreCommitView {
  return {
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    playground_id: 'playground-a',
    precommit_id: 'precommit-a',
    precommit_request_id: 'request-a',
    attempt: 1,
    state: 'ready',
    phase: 'idle',
    progress: { percent: 100, files_completed: '1', bytes_completed: '1' },
    checks: [],
    warnings: [],
    blockers: [],
    source_index_version: { revision: '1', digest: 'sha256:source' },
    candidate_index_version: { revision: '2', digest: 'sha256:candidate' },
    created_at_unix_ms: '1',
    updated_at_unix_ms: '2',
    ...overrides,
  };
}

describe('Pre-commit status helpers', () => {
  it('treats ready as a state and idle as its completed phase', () => {
    expect(preCommitPhaseLabels.idle).toBe('处理完成');
    expect(canCommitPreCommit(precommit())).toBe(true);
    expect(canCommitPreCommit(precommit({ phase: 'validating' }))).toBe(false);
    expect(
      canCommitPreCommit(precommit({ blockers: [{ code: 'BLOCKED', message: 'blocked' }] })),
    ).toBe(false);
  });

  it('polls only while the server reports running', () => {
    expect(preCommitPollInterval('running')).toBe(1000);
    expect(preCommitPollInterval('ready')).toBe(false);
    expect(preCommitPollInterval('abnormal')).toBe(false);
  });
});
