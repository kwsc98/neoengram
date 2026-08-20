import { describe, expect, it } from 'vitest';

import {
  createS3AccessPoint,
  createS3Credential,
  createS3DownloadUrl,
  disableS3AccessPoint,
  queryS3AccessPointList,
  queryS3CredentialList,
  queryS3ObjectList,
} from '@/api/operations';

describe('read-only S3 control operations', () => {
  it('lists bucket prefixes and creates a short-lived object download URL', async () => {
    const accessPoints = await queryS3AccessPointList({
      tenant_id: 'tenant-a',
      page_size: 50,
    });
    const accessPoint = accessPoints.data.items[0]!;
    expect(accessPoint.endpoint).toBe(window.location.origin);

    const root = await queryS3ObjectList({
      tenant_id: 'tenant-a',
      access_point_id: accessPoint.access_point_id,
      prefix: '',
      delimiter: '/',
      page_size: 100,
    });
    expect(root.data.items).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ key: 'images/', entry_type: 'prefix' }),
        expect.objectContaining({ key: 'manifests/', entry_type: 'prefix' }),
        expect.objectContaining({ key: 'README.md', entry_type: 'object' }),
      ]),
    );

    const nested = await queryS3ObjectList({
      tenant_id: 'tenant-a',
      access_point_id: accessPoint.access_point_id,
      prefix: 'images/',
      delimiter: '/',
    });
    expect(nested.data.items.map((entry) => entry.key)).toEqual(['images/day/', 'images/night/']);

    const download = await createS3DownloadUrl({
      tenant_id: 'tenant-a',
      access_point_id: accessPoint.access_point_id,
      key: 'README.md',
      expires_seconds: 300,
    });
    expect(download.data.url).toContain(`${accessPoint.bucket_name}/README.md`);
    expect(Number(download.data.expires_at_unix_ms)).toBeGreaterThan(Date.now());
  });

  it('serves the mock object data plane from the configured endpoint', async () => {
    const accessPoints = await queryS3AccessPointList({
      tenant_id: 'tenant-a',
      page_size: 50,
    });
    const accessPoint = accessPoints.data.items[0]!;
    const response = await fetch(
      `${accessPoint.endpoint}/${accessPoint.bucket_name}/README.md?expires=${Date.now() + 300_000}`,
    );

    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('text/markdown');
    expect(response.headers.get('content-disposition')).toContain('README.md');
    expect(await response.text()).toContain('mock S3 object');
  });

  it('returns a Secret once, limits active rotation, and revokes credentials on disable', async () => {
    const request = {
      tenant_id: 'tenant-a',
      snapshot_id: 'snap-dialog-2-bj-01',
      bucket_name: 'dialog-corpus-snapshot',
      request_id: 's3-access-point-test-1',
    };
    const created = await createS3AccessPoint(request);
    expect(created.data.secret_access_key).toBeTruthy();
    expect(created.data.replayed).toBe(false);

    const replay = await createS3AccessPoint(request);
    expect(replay.data.replayed).toBe(true);
    expect(replay.data.secret_access_key).toBeUndefined();

    const accessPointId = created.data.access_point.access_point_id;
    const rotated = await createS3Credential({
      tenant_id: 'tenant-a',
      access_point_id: accessPointId,
      request_id: 's3-credential-test-1',
    });
    expect(rotated.data.secret_access_key).toBeTruthy();
    const rotationReplay = await createS3Credential({
      tenant_id: 'tenant-a',
      access_point_id: accessPointId,
      request_id: 's3-credential-test-1',
    });
    expect(rotationReplay.data.replayed).toBe(true);
    expect(rotationReplay.data.secret_access_key).toBeUndefined();

    await expect(
      createS3Credential({
        tenant_id: 'tenant-a',
        access_point_id: accessPointId,
        request_id: 's3-credential-test-2',
      }),
    ).rejects.toMatchObject({ status: 409, code: 'S3_CREDENTIAL_LIMIT' });

    await disableS3AccessPoint({
      tenant_id: 'tenant-a',
      access_point_id: accessPointId,
      request_id: 's3-disable-test-1',
    });
    const credentials = await queryS3CredentialList({
      tenant_id: 'tenant-a',
      access_point_id: accessPointId,
    });
    expect(credentials.data.items).toHaveLength(2);
    expect(credentials.data.items.every((credential) => credential.state === 'revoked')).toBe(true);
  });
});
