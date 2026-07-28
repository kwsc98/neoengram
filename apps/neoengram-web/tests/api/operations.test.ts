import { describe, expect, it } from 'vitest';

import { createAddJob, finalizeAddJob, queryJob } from '@/api/operations';
import { ApiProblem } from '@/api/problem';
import type { CreateAddJobRequest } from '@/api/types';

function request(overrides: Partial<CreateAddJobRequest> = {}): CreateAddJobRequest {
  return {
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    playground_id: 'playground-a',
    job_id: 'job-test-1',
    expected_index_version: { revision: '0', digest: 'a'.repeat(64) },
    deadline_unix_ms: String(Date.now() + 60_000),
    paths: ['dataset/images'],
    all: false,
    future_mode: 'strict',
    ...overrides,
  };
}

describe('public Job operations', () => {
  it('injects auth/version/request headers and replays an identical create request', async () => {
    const body = request();
    const first = await createAddJob(body);
    const replay = await createAddJob(body);

    expect(first.data.replayed).toBe(false);
    expect(replay.data.replayed).toBe(true);
    expect(first.requestId).toMatch(/^req-/);
    expect(replay.data.job.job_id).toBe('job-test-1');
  });

  it('includes unknown extension fields in the mock idempotency comparison', async () => {
    await createAddJob(request());

    await expect(createAddJob(request({ future_mode: 'relaxed' }))).rejects.toMatchObject({
      status: 409,
      code: 'JOB_ID_REUSED',
      retryable: false,
    });
  });

  it('returns stable not-found, deadline and invalid-state problems', async () => {
    await expect(queryJob('tenant-a', 'job-missing')).rejects.toMatchObject({
      status: 404,
      code: 'JOB_NOT_FOUND',
    });
    await expect(
      createAddJob(request({ job_id: 'job-expired', deadline_unix_ms: '1' })),
    ).rejects.toMatchObject({ status: 408, code: 'JOB_DEADLINE_EXCEEDED' });
    await createAddJob(request({ job_id: 'job-not-prepared' }));
    await expect(finalizeAddJob('tenant-a', 'job-not-prepared')).rejects.toMatchObject({
      status: 409,
      code: 'JOB_INVALID_STATE',
    });
  });

  it('advances through Prepared and replays the stable finalize decision', async () => {
    await createAddJob(request());
    expect((await queryJob('tenant-a', 'job-test-1')).data.job.state).toBe('running');
    expect((await queryJob('tenant-a', 'job-test-1')).data.job.state).toBe('prepared');

    const finalized = await finalizeAddJob('tenant-a', 'job-test-1');
    const replay = await finalizeAddJob('tenant-a', 'job-test-1');
    expect(finalized.data.replayed).toBe(false);
    expect(finalized.data.job.state).toBe('succeeded');
    expect(replay.data.replayed).toBe(true);
    expect(replay.data.decision).toEqual(finalized.data.decision);
  });

  it('maps validation and unavailable responses to RFC 9457 errors', async () => {
    await expect(createAddJob(request({ paths: [] }))).rejects.toBeInstanceOf(ApiProblem);
    await expect(createAddJob(request({ tenant_id: 'tenant-unavailable' }))).rejects.toMatchObject({
      status: 503,
      code: 'STORAGE_FAILURE',
      retryable: true,
      retryAfterMs: 1000,
    });
  });
});
