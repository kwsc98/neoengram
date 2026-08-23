import { describe, expect, it } from 'vitest';

import type { CommitReplication, CommitReplicationState } from '@/features/commit-replication';
import {
  commitReplicationRequestId,
  findActiveCommitReplication,
  findCommitReplicationForTarget,
  isCommitReplicationActive,
} from '@/features/commit-replication';

function replication(
  replicationId: string,
  targetStorageVolumeId: string,
  state: CommitReplication['state'],
): CommitReplication {
  return {
    replication_id: replicationId,
    tenant_id: 'tenant-a',
    commit_id: 'a'.repeat(64),
    target_storage_volume_id: targetStorageVolumeId,
    attempt: '1',
    state,
    object_set_digest: 'b'.repeat(64),
    completed_objects: '0',
    total_objects: '1',
    completed_bytes: '0',
    total_bytes: '10',
  };
}

describe('Commit replication helpers', () => {
  it('recognizes only non-terminal replication states as active', () => {
    const active: CommitReplicationState[] = ['queued', 'planning', 'transferring', 'verifying'];
    const terminal: CommitReplicationState[] = ['published', 'failed', 'cancelled'];

    expect(active.every(isCommitReplicationActive)).toBe(true);
    expect(terminal.some(isCommitReplicationActive)).toBe(false);
  });

  it('prefers an active task for the selected target and restores any active task', () => {
    const items = [
      replication('published-a', 'volume-a', 'published'),
      replication('active-b', 'volume-b', 'transferring'),
      replication('active-a', 'volume-a', 'planning'),
    ];

    expect(findCommitReplicationForTarget(items, 'volume-a')?.replication_id).toBe('active-a');
    expect(findActiveCommitReplication(items)?.replication_id).toBe('active-b');
    expect(findCommitReplicationForTarget(items, 'volume-c')).toBeUndefined();
  });

  it('prefers a published task over an older failed task for one target', () => {
    const items = [
      replication('failed-a', 'volume-a', 'failed'),
      replication('published-a', 'volume-a', 'published'),
    ];

    expect(findCommitReplicationForTarget(items, 'volume-a')?.replication_id).toBe('published-a');
  });

  it('builds a stable request ID that remains within the API limit', () => {
    const scope = {
      tenantId: 'tenant-a',
      projectId: 'project-a',
      artifactId: 'artifact-a',
      commitId: 'a'.repeat(64),
      targetStorageVolumeId: 'volume-'.padEnd(128, 'x'),
    };
    const first = commitReplicationRequestId(scope);

    expect(commitReplicationRequestId(scope)).toBe(first);
    expect(first).toMatch(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/);
    expect(first.length).toBeLessThanOrEqual(128);
    expect(
      commitReplicationRequestId({
        ...scope,
        targetStorageVolumeId: `${scope.targetStorageVolumeId}y`,
      }),
    ).not.toBe(first);
  });
});
