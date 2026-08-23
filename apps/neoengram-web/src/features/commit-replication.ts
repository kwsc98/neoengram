import type { QueryCommitReplicationListResponse } from '@/api/types';

export type CommitReplication = QueryCommitReplicationListResponse['replications'][number];
export type CommitReplicationState = CommitReplication['state'];

const activeStates = new Set<CommitReplicationState>([
  'queued',
  'planning',
  'transferring',
  'verifying',
]);

export function isCommitReplicationActive(state: CommitReplicationState): boolean {
  return activeStates.has(state);
}

export function findCommitReplicationForTarget(
  replications: readonly CommitReplication[],
  targetStorageVolumeId: string,
): CommitReplication | undefined {
  const matching = replications.filter(
    (replication) => replication.target_storage_volume_id === targetStorageVolumeId,
  );
  return (
    matching.find((replication) => isCommitReplicationActive(replication.state)) ??
    matching.find((replication) => replication.state === 'published') ??
    matching[0]
  );
}

export function findActiveCommitReplication(
  replications: readonly CommitReplication[],
): CommitReplication | undefined {
  return replications.find((replication) => isCommitReplicationActive(replication.state));
}

interface CommitReplicationRequestScope {
  tenantId: string;
  projectId: string;
  artifactId: string;
  commitId: string;
  targetStorageVolumeId: string;
}

function hash32(value: string, seed: number): string {
  let hash = seed;
  for (let index = 0; index < value.length; index += 1) {
    hash = Math.imul(hash ^ value.charCodeAt(index), 16_777_619);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
}

export function commitReplicationRequestId(scope: CommitReplicationRequestScope): string {
  const canonicalScope = [
    scope.tenantId,
    scope.projectId,
    scope.artifactId,
    scope.commitId,
    scope.targetStorageVolumeId,
  ]
    .map((part) => `${part.length}:${part}`)
    .join('|');
  const digest = [
    hash32(canonicalScope, 2_166_136_261),
    hash32(canonicalScope, 2_166_136_261 ^ 0x9e3779b9),
    hash32(canonicalScope, 2_166_136_261 ^ 0x85ebca6b),
    hash32(canonicalScope, 2_166_136_261 ^ 0xc2b2ae35),
  ].join('');
  return `commit-replicate-${digest}`;
}
