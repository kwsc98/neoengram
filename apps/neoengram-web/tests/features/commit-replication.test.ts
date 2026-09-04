import { describe, expect, it } from 'vitest';

import type { MaterializationView } from '@/api/types';
import {
  isMaterializationActive,
  materializationRequestId,
  materializationRetryRequestId,
} from '@/features/materialization';

describe('Commit materialization helpers', () => {
  it('recognizes only non-terminal materialization states as active', () => {
    const active: MaterializationView['state'][] = [
      'queued',
      'planning',
      'waiting_for_sources',
      'materializing',
      'verifying',
    ];
    const terminal: MaterializationView['state'][] = ['complete', 'failed', 'cancelled'];

    expect(active.every(isMaterializationActive)).toBe(true);
    expect(terminal.some(isMaterializationActive)).toBe(false);
  });

  it('builds stable bounded request IDs', () => {
    const scope = {
      tenantId: 'tenant-a',
      projectId: 'project-a',
      artifactId: 'artifact-a',
      commitId: 'a'.repeat(64),
      targetStorageVolumeId: 'volume-'.padEnd(128, 'x'),
    };
    const first = materializationRequestId(scope);

    expect(materializationRequestId(scope)).toBe(first);
    expect(first).toMatch(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/);
    expect(first.length).toBeLessThanOrEqual(128);
    expect(
      materializationRequestId({
        ...scope,
        targetStorageVolumeId: `${scope.targetStorageVolumeId}y`,
      }),
    ).not.toBe(first);
    expect(materializationRetryRequestId('materialization-a', '1')).not.toBe(
      materializationRetryRequestId('materialization-a', '2'),
    );
  });
});
