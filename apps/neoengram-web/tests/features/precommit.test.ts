import { describe, expect, it } from 'vitest';

import {
  canCommitPreCommit,
  isWorkspaceOperational,
  workspaceLifecycleLabel,
  workspaceListPollInterval,
  workspaceOperationUnavailableReason,
  workspacePollInterval,
  workspaceStorageAvailability,
  workspaceStorageAvailabilityLabel,
  preCommitPhaseLabels,
  preCommitPollInterval,
} from '@/features/precommit/status';
import type { PreCommitView } from '@/api/types';

function precommit(overrides: Partial<PreCommitView> = {}): PreCommitView {
  return {
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    workspace_id: 'workspace-a',
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
    expect(workspaceLifecycleLabel('ready')).toBe('已物化');
    expect(workspaceStorageAvailabilityLabel('unavailable')).toBe('存储不可达');
    expect(isWorkspaceOperational({ state: 'ready', storage_availability: 'ready' })).toBe(true);
    expect(isWorkspaceOperational({ state: 'ready', storage_availability: 'unavailable' })).toBe(
      false,
    );
    expect(isWorkspaceOperational({ state: 'abnormal', storage_availability: 'ready' })).toBe(
      false,
    );
  });

  it('keeps polling ready Workspaces so live Agent availability changes are discovered', () => {
    expect(workspacePollInterval({ state: 'creating', storage_availability: 'unknown' })).toBe(
      1_000,
    );
    expect(
      workspacePollInterval({
        state: 'ready',
        storage_availability: 'ready',
        active_precommit_id: 'precommit-a',
      }),
    ).toBe(1_000);
    expect(workspacePollInterval({ state: 'ready', storage_availability: 'ready' })).toBe(5_000);
    expect(workspacePollInterval({ state: 'ready', storage_availability: 'unavailable' })).toBe(
      5_000,
    );
    expect(
      workspaceListPollInterval([
        { state: 'ready', storage_availability: 'ready' },
        { state: 'creating', storage_availability: 'unknown' },
      ]),
    ).toBe(1_000);
    expect(workspaceListPollInterval([])).toBe(5_000);
  });

  it('explains lifecycle failures before live storage availability', () => {
    expect(
      workspaceOperationUnavailableReason({
        state: 'abnormal',
        storage_availability: 'ready',
      }),
    ).toBe('Workspace 生命周期异常');
    expect(
      workspaceOperationUnavailableReason({
        state: 'abnormal',
        storage_availability: 'unavailable',
      }),
    ).toBe('Workspace 生命周期异常');
    expect(
      workspaceOperationUnavailableReason({
        state: 'ready',
        storage_availability: 'unavailable',
      }),
    ).toBe('StorageVolume 当前不可达');
    expect(
      workspaceOperationUnavailableReason({ state: 'ready', storage_availability: 'ready' }),
    ).toBeUndefined();
  });

  it('treats a legacy response without storage availability as unknown and non-operational', () => {
    const legacyWorkspace = { state: 'ready' as const };

    expect(workspaceStorageAvailability(legacyWorkspace)).toBe('unknown');
    expect(isWorkspaceOperational(legacyWorkspace)).toBe(false);
  });
});
