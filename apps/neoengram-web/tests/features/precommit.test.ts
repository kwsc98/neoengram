import { describe, expect, it } from 'vitest';

import {
  canCommitPreCommit,
  isPlaygroundOperational,
  playgroundLifecycleLabel,
  playgroundListPollInterval,
  playgroundOperationUnavailableReason,
  playgroundPollInterval,
  playgroundStorageAvailability,
  playgroundStorageAvailabilityLabel,
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
    data_layout: 'fast_cdc',
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

  it('separates the persisted lifecycle from live storage availability', () => {
    expect(playgroundLifecycleLabel('ready')).toBe('已物化');
    expect(playgroundStorageAvailabilityLabel('unavailable')).toBe('存储不可达');
    expect(isPlaygroundOperational({ state: 'ready', storage_availability: 'ready' })).toBe(true);
    expect(isPlaygroundOperational({ state: 'ready', storage_availability: 'unavailable' })).toBe(
      false,
    );
    expect(isPlaygroundOperational({ state: 'abnormal', storage_availability: 'ready' })).toBe(
      false,
    );
  });

  it('keeps polling ready Playgrounds so live Agent availability changes are discovered', () => {
    expect(playgroundPollInterval({ state: 'creating', storage_availability: 'unknown' })).toBe(
      1_000,
    );
    expect(
      playgroundPollInterval({
        state: 'ready',
        storage_availability: 'ready',
        active_precommit_id: 'precommit-a',
      }),
    ).toBe(1_000);
    expect(playgroundPollInterval({ state: 'ready', storage_availability: 'ready' })).toBe(5_000);
    expect(playgroundPollInterval({ state: 'ready', storage_availability: 'unavailable' })).toBe(
      5_000,
    );
    expect(
      playgroundListPollInterval([
        { state: 'ready', storage_availability: 'ready' },
        { state: 'creating', storage_availability: 'unknown' },
      ]),
    ).toBe(1_000);
    expect(playgroundListPollInterval([])).toBe(5_000);
  });

  it('explains lifecycle failures before live storage availability', () => {
    expect(
      playgroundOperationUnavailableReason({
        state: 'abnormal',
        storage_availability: 'ready',
      }),
    ).toBe('Playground 生命周期异常');
    expect(
      playgroundOperationUnavailableReason({
        state: 'abnormal',
        storage_availability: 'unavailable',
      }),
    ).toBe('Playground 生命周期异常');
    expect(
      playgroundOperationUnavailableReason({
        state: 'ready',
        storage_availability: 'unavailable',
      }),
    ).toBe('StorageVolume 当前不可达');
    expect(
      playgroundOperationUnavailableReason({ state: 'ready', storage_availability: 'ready' }),
    ).toBeUndefined();
  });

  it('treats a legacy response without storage availability as unknown and non-operational', () => {
    const legacyPlayground = { state: 'ready' as const };

    expect(playgroundStorageAvailability(legacyPlayground)).toBe('unknown');
    expect(isPlaygroundOperational(legacyPlayground)).toBe(false);
  });
});
