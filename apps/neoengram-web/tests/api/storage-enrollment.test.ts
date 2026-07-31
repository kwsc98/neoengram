import { describe, expect, it, vi } from 'vitest';

import {
  approveStorageEnrollment,
  createStorageEnrollmentToken,
  createStorageVolume,
  queryStorageEnrollment,
  queryStorageEnrollmentList,
  queryStorageVolume,
  rejectStorageEnrollment,
} from '@/api/operations';

function tokenRequest(suffix: string) {
  return {
    tenant_id: 'tenant-a',
    token_request_id: `storage-token-request-${suffix}`,
    storage_volume_id: `volume-enrollment-${suffix}`,
    display_name: `Enrollment volume ${suffix}`,
    edge_cluster_id: 'cluster-cn-east-1',
    region: 'cn-shanghai',
    access_mode: 'read_write_many' as const,
    pvc_reference: {
      namespace: 'neoengram-data',
      claim_name: `enrollment-${suffix}`,
    },
  };
}

describe('storage enrollment public operations', () => {
  it('creates a replayable one-time token without simulating Agent bootstrap', async () => {
    const request = tokenRequest('token');
    const created = await createStorageEnrollmentToken(request);
    expect(created.data).toMatchObject({ replayed: false });
    expect(created.data.bootstrap_token).toMatch(/^ngenr_v1_/);

    const replayed = await createStorageEnrollmentToken(request);
    expect(replayed.data).toMatchObject({
      replayed: true,
      token_id: created.data.token_id,
      bootstrap_token: created.data.bootstrap_token,
    });
    await expect(
      createStorageEnrollmentToken({ ...request, display_name: 'Changed payload' }),
    ).rejects.toMatchObject({ status: 409 });

    const list = await queryStorageEnrollmentList({
      tenant_id: 'tenant-a',
      state: 'pending_approval',
      query: request.storage_volume_id,
    });
    expect(list.data.items).toEqual([]);

    const seeded = await queryStorageEnrollmentList({
      tenant_id: 'tenant-a',
      state: 'pending_approval',
      page_size: 100,
    });
    const serialized = JSON.stringify(seeded.data.items);
    expect(serialized).not.toContain(created.data.bootstrap_token);
    for (const enrollment of seeded.data.items) {
      expect(enrollment.identity_fingerprint).toMatch(/^[0-9a-f]{64}$/);
      expect(enrollment).not.toHaveProperty('mount_path');
      expect(enrollment).not.toHaveProperty('certificate');
      expect(enrollment).not.toHaveProperty('owner_generation');
    }
  });

  it('approves with CAS, creates an unavailable Volume and replays the same decision', async () => {
    const pending = (
      await queryStorageEnrollmentList({
        tenant_id: 'tenant-a',
        state: 'pending_approval',
        query: 'volume-review-pvc',
      })
    ).data.items[0];
    if (!pending) throw new Error('expected a pending enrollment');

    const approval = {
      tenant_id: 'tenant-a',
      storage_enrollment_id: pending.storage_enrollment_id,
      approval_request_id: 'storage-approval-request-test',
      expected_resource_version: pending.resource_version,
      confirm_replacement: false,
    };
    const approved = await approveStorageEnrollment(approval);
    expect(approved.data).toMatchObject({
      replayed: false,
      enrollment: { state: 'approved' },
      storage_volume: { state: 'unavailable' },
    });
    expect(
      (await queryStorageVolume('tenant-a', pending.storage_volume_id)).data.storage_volume.state,
    ).toBe('unavailable');
    expect((await approveStorageEnrollment(approval)).data.replayed).toBe(true);
    await expect(
      approveStorageEnrollment({ ...approval, confirm_replacement: true }),
    ).rejects.toMatchObject({ status: 409 });
  });

  it('rejects without creating a Volume and keeps cross-tenant enrollment queries hidden', async () => {
    const pending = (
      await queryStorageEnrollmentList({
        tenant_id: 'tenant-a',
        state: 'pending_approval',
        query: 'volume-review-pvc',
      })
    ).data.items[0];
    if (!pending) throw new Error('expected a pending enrollment');

    const rejected = await rejectStorageEnrollment({
      tenant_id: 'tenant-a',
      storage_enrollment_id: pending.storage_enrollment_id,
      rejection_request_id: 'storage-rejection-request-test',
      expected_resource_version: pending.resource_version,
      reason: 'PVC descriptor is not approved',
    });
    expect(rejected.data.enrollment).toMatchObject({ state: 'rejected' });
    expect(rejected.data.enrollment).not.toHaveProperty('review_reason');
    expect(JSON.stringify(rejected.data)).not.toContain('PVC descriptor is not approved');
    const queried = await queryStorageEnrollment('tenant-a', pending.storage_enrollment_id);
    expect(queried.data.enrollment).not.toHaveProperty('review_reason');
    await expect(queryStorageVolume('tenant-a', pending.storage_volume_id)).rejects.toMatchObject({
      status: 404,
      code: 'STORAGE_VOLUME_NOT_FOUND',
    });
    await expect(
      queryStorageEnrollment('tenant-b', pending.storage_enrollment_id),
    ).rejects.toMatchObject({ status: 403, code: 'AUTHORIZATION_DENIED' });
  });

  it('binds opaque cursors to the enrollment filter', async () => {
    const first = await queryStorageEnrollmentList({
      tenant_id: 'tenant-a',
      state: 'pending_approval',
      page_size: 1,
    });
    expect(first.data.items).toHaveLength(1);
    expect(first.data.next_cursor).toBeTruthy();
    if (!first.data.next_cursor) throw new Error('expected a second enrollment page');

    const second = await queryStorageEnrollmentList({
      tenant_id: 'tenant-a',
      state: 'pending_approval',
      page_size: 1,
      cursor: first.data.next_cursor,
    });
    expect(second.data.items).toHaveLength(1);
    await expect(
      queryStorageEnrollmentList({
        tenant_id: 'tenant-a',
        state: 'approved',
        page_size: 1,
        cursor: first.data.next_cursor,
      }),
    ).rejects.toMatchObject({ status: 409, code: 'CURSOR_INVALID' });
  });

  it('derives expiry before list, query and review CAS', async () => {
    const pending = (
      await queryStorageEnrollmentList({
        tenant_id: 'tenant-a',
        state: 'pending_approval',
        query: 'volume-review-pvc',
      })
    ).data.items[0];
    if (!pending) throw new Error('expected a pending enrollment');

    const dateNow = vi.spyOn(Date, 'now').mockReturnValue(Number(pending.expires_at_unix_ms) + 1);
    const pendingAfterExpiry = await queryStorageEnrollmentList({
      tenant_id: 'tenant-a',
      state: 'pending_approval',
      query: pending.storage_volume_id,
    });
    expect(pendingAfterExpiry.data.items).toEqual([]);
    const expired = await queryStorageEnrollment('tenant-a', pending.storage_enrollment_id);
    expect(expired.data.enrollment).toMatchObject({ state: 'expired', resource_version: '2' });

    await expect(
      approveStorageEnrollment({
        tenant_id: 'tenant-a',
        storage_enrollment_id: pending.storage_enrollment_id,
        approval_request_id: 'approve-expired-enrollment',
        expected_resource_version: pending.resource_version,
        confirm_replacement: false,
      }),
    ).rejects.toMatchObject({ status: 409 });
    await expect(
      rejectStorageEnrollment({
        tenant_id: 'tenant-a',
        storage_enrollment_id: pending.storage_enrollment_id,
        rejection_request_id: 'reject-expired-enrollment',
        expected_resource_version: pending.resource_version,
      }),
    ).rejects.toMatchObject({ status: 409 });
    dateNow.mockRestore();
  });

  it('enforces enrollment permissions per Tenant while preserving cross-Tenant 404', async () => {
    const deniedToken = { ...tokenRequest('tenant-b-denied'), tenant_id: 'tenant-b' };
    await expect(createStorageEnrollmentToken(deniedToken)).rejects.toMatchObject({ status: 403 });
    await expect(
      queryStorageEnrollmentList({ tenant_id: 'tenant-b', state: 'pending_approval' }),
    ).rejects.toMatchObject({ status: 403 });
    await expect(
      queryStorageEnrollment('tenant-b', 'storage-enrollment-tenant-b-01'),
    ).rejects.toMatchObject({ status: 403 });
    await expect(
      approveStorageEnrollment({
        tenant_id: 'tenant-b',
        storage_enrollment_id: 'storage-enrollment-tenant-b-01',
        approval_request_id: 'tenant-b-approve-denied',
        expected_resource_version: '1',
        confirm_replacement: false,
      }),
    ).rejects.toMatchObject({ status: 403 });
    await expect(
      rejectStorageEnrollment({
        tenant_id: 'tenant-b',
        storage_enrollment_id: 'storage-enrollment-tenant-b-01',
        rejection_request_id: 'tenant-b-reject-denied',
        expected_resource_version: '1',
      }),
    ).rejects.toMatchObject({ status: 403 });
    await expect(
      queryStorageEnrollment('tenant-b', 'storage-enrollment-review-01'),
    ).rejects.toMatchObject({ status: 403, code: 'AUTHORIZATION_DENIED' });
    await expect(
      approveStorageEnrollment({
        tenant_id: 'tenant-b',
        storage_enrollment_id: 'storage-enrollment-review-01',
        approval_request_id: 'tenant-b-cross-enrollment-approve-denied',
        expected_resource_version: '1',
        confirm_replacement: false,
      }),
    ).rejects.toMatchObject({ status: 403, code: 'AUTHORIZATION_DENIED' });
    await expect(
      rejectStorageEnrollment({
        tenant_id: 'tenant-b',
        storage_enrollment_id: 'storage-enrollment-review-01',
        rejection_request_id: 'tenant-b-cross-enrollment-reject-denied',
        expected_resource_version: '1',
      }),
    ).rejects.toMatchObject({ status: 403, code: 'AUTHORIZATION_DENIED' });
  });

  it('requires explicit replacement confirmation and exact existing Volume descriptor', async () => {
    const replacement = (
      await queryStorageEnrollmentList({
        tenant_id: 'tenant-a',
        state: 'pending_approval',
        registration_kind: 'replacement',
      })
    ).data.items[0];
    if (!replacement) throw new Error('expected a replacement enrollment');

    await expect(
      approveStorageEnrollment({
        tenant_id: 'tenant-a',
        storage_enrollment_id: replacement.storage_enrollment_id,
        approval_request_id: 'replacement-without-confirmation',
        expected_resource_version: replacement.resource_version,
        confirm_replacement: false,
      }),
    ).rejects.toMatchObject({ status: 409 });
    const approved = await approveStorageEnrollment({
      tenant_id: 'tenant-a',
      storage_enrollment_id: replacement.storage_enrollment_id,
      approval_request_id: 'replacement-confirmed',
      expected_resource_version: replacement.resource_version,
      confirm_replacement: true,
    });
    expect(approved.data).toMatchObject({
      enrollment: { state: 'approved', registration_kind: 'replacement' },
      storage_volume: {
        storage_volume_id: 'volume-shanghai-vision',
        state: 'unavailable',
      },
    });
  });

  it('rejects duplicate PVC identities, descriptor drift and invalid Kubernetes names', async () => {
    await expect(
      createStorageEnrollmentToken({
        ...tokenRequest('duplicate-pvc'),
        edge_cluster_id: 'cluster-cn-east-1',
        pvc_reference: { namespace: 'neoengram-data', claim_name: 'vision-data' },
      }),
    ).rejects.toMatchObject({ status: 409, code: 'PVC_ALREADY_ENROLLED' });
    await expect(
      createStorageEnrollmentToken({
        ...tokenRequest('descriptor-drift'),
        storage_volume_id: 'volume-shanghai-vision',
        display_name: 'Wrong display name',
        edge_cluster_id: 'cluster-cn-east-1',
        pvc_reference: { namespace: 'neoengram-data', claim_name: 'vision-data' },
      }),
    ).rejects.toMatchObject({ status: 409, code: 'STORAGE_VOLUME_DESCRIPTOR_CONFLICT' });
    await expect(
      createStorageVolume({
        tenant_id: 'tenant-a',
        storage_volume_id: 'duplicate-direct-volume',
        display_name: 'Duplicate direct PVC',
        edge_cluster_id: 'cluster-cn-east-1',
        region: 'cn-shanghai',
        backend_type: 'pvc',
        access_mode: 'read_write_many',
        pvc_reference: { namespace: 'neoengram-data', claim_name: 'vision-data' },
      }),
    ).rejects.toMatchObject({ status: 409, code: 'PVC_ALREADY_ENROLLED' });
    await expect(
      createStorageEnrollmentToken({
        ...tokenRequest('invalid-namespace'),
        pvc_reference: { namespace: 'not.a.namespace', claim_name: 'valid-claim' },
      }),
    ).rejects.toMatchObject({ status: 422 });
    await expect(
      createStorageEnrollmentToken({
        ...tokenRequest('invalid-claim'),
        pvc_reference: { namespace: 'valid-namespace', claim_name: 'a'.repeat(64) },
      }),
    ).rejects.toMatchObject({ status: 422 });
  });
});
