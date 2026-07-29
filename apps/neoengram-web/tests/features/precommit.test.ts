import { describe, expect, it } from 'vitest';

import {
  cancelPrototypePreCommit,
  getActivePreCommit,
  preCommitScopeKey,
  startPrototypePreCommit,
} from '@/features/precommit/prototype';

describe('prototype Pre-commit state', () => {
  it('isolates same-named Playgrounds across Artifact scopes', () => {
    const first = preCommitScopeKey('tenant-a', 'project-a', 'artifact-a', 'review');
    const second = preCommitScopeKey('tenant-a', 'project-a', 'artifact-b', 'review');

    startPrototypePreCommit(first, { jobId: 'precommit-first' });
    startPrototypePreCommit(second, { jobId: 'precommit-second' });

    expect(getActivePreCommit(first)?.jobId).toBe('precommit-first');
    expect(getActivePreCommit(second)?.jobId).toBe('precommit-second');

    cancelPrototypePreCommit(first);
    expect(getActivePreCommit(first)).toBeUndefined();
    expect(getActivePreCommit(second)?.jobId).toBe('precommit-second');
    cancelPrototypePreCommit(second);
  });
});
