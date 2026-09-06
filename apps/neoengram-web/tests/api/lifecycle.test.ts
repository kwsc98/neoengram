import { describe, expect, it } from 'vitest';

import {
  createDeletion,
  createRetentionHold,
  queryDeletion,
  queryDeletionImpact,
  queryDeletionList,
  releaseRetentionHold,
  restoreDeletion,
} from '@/api/operations';

describe('resource lifecycle control operations', () => {
  it('requires a fresh impact confirmation and supports restore with idempotent mutations', async () => {
    const resource = { type: 'snapshot' as const, snapshot_id: 'snap-dialog-2-bj-01' };
    const impact = await queryDeletionImpact({
      tenant_id: 'tenant-a',
      resource,
      cascade: false,
      confirm_managed_data_erase: false,
      expected_resource_version: '6',
    });
    expect(impact.data.impact.targets).toHaveLength(1);

    const request = {
      tenant_id: 'tenant-a',
      resource,
      cascade: false,
      confirm_managed_data_erase: false,
      expected_resource_version: '6',
      impact_digest: impact.data.impact_digest,
      request_id: 'lifecycle-snapshot-delete-1',
    };
    const created = await createDeletion(request);
    expect(created.data.deletion.state).toBe('recoverable');
    expect(created.data.request_replayed).toBe(false);

    const replay = await createDeletion(request);
    expect(replay.data.request_replayed).toBe(true);
    expect(replay.data.deletion.deletion_id).toBe(created.data.deletion.deletion_id);

    const listed = await queryDeletionList({ tenant_id: 'tenant-a', states: ['recoverable'] });
    expect(listed.data.items.map((item) => item.deletion_id)).toContain(
      created.data.deletion.deletion_id,
    );

    const restored = await restoreDeletion({
      tenant_id: 'tenant-a',
      deletion_id: created.data.deletion.deletion_id,
      expected_resource_version: created.data.deletion.resource_version,
      request_id: 'lifecycle-snapshot-restore-1',
    });
    expect(restored.data.deletion.completion).toBe('restored');
    expect(
      (
        await queryDeletion({
          tenant_id: 'tenant-a',
          deletion_id: created.data.deletion.deletion_id,
        })
      ).data.deletion.state,
    ).toBe('completed');
  });

  it('surfaces cascade and managed-directory confirmations in the impact response', async () => {
    const resource = {
      type: 'storage_volume' as const,
      storage_volume_id: 'volume-shanghai-vision',
    };
    const impact = await queryDeletionImpact({
      tenant_id: 'tenant-a',
      resource,
      cascade: false,
      confirm_managed_data_erase: false,
      expected_resource_version: '4',
    });
    expect(impact.data.impact.blockers.map((blocker) => blocker.code)).toEqual(
      expect.arrayContaining(['CASCADE_REQUIRED', 'MANAGED_DATA_ERASE_CONFIRMATION_REQUIRED']),
    );
  });

  it('creates and releases a retention hold', async () => {
    const deletionId = 'deletion-example-recoverable';
    const detail = await queryDeletion({ tenant_id: 'tenant-a', deletion_id: deletionId });
    const created = await createRetentionHold({
      tenant_id: 'tenant-a',
      deletion_id: deletionId,
      expected_resource_version: detail.data.deletion.resource_version,
      request_id: 'lifecycle-hold-create-1',
      reason: '合规审计保留',
    });
    expect(created.data.retention_hold.state).toBe('active');

    const released = await releaseRetentionHold({
      tenant_id: 'tenant-a',
      deletion_id: deletionId,
      retention_hold_id: created.data.retention_hold.retention_hold_id,
      expected_resource_version: created.data.deletion.resource_version,
      request_id: 'lifecycle-hold-release-1',
    });
    expect(released.data.retention_hold.state).toBe('released');
  });
});
