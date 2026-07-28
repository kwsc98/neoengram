import { createPinia, setActivePinia } from 'pinia';
import { beforeEach, describe, expect, it } from 'vitest';

import { useRecentJobsStore } from '@/stores/recent-jobs';

describe('recent Job store', () => {
  beforeEach(() => setActivePinia(createPinia()));

  it('deduplicates and limits browser-local records to 50', () => {
    const store = useRecentJobsStore();
    for (let index = 0; index < 55; index += 1) store.remember('tenant-a', `job-${index}`);
    store.remember('tenant-a', 'job-25');

    expect(store.jobs).toHaveLength(50);
    expect(store.jobs[0]?.jobId).toBe('job-25');
    expect(new Set(store.jobs.map((job) => job.jobId)).size).toBe(50);
  });

  it('persists only tenant, Job ID and last-seen time', () => {
    const store = useRecentJobsStore();
    store.remember('tenant-a', 'job-a');
    const persisted = window.localStorage.getItem('neoengram.recent-jobs.v1') ?? '';

    expect(persisted).toContain('tenant-a');
    expect(persisted).not.toContain('token');
    expect(persisted).not.toContain('progress');
  });

  it('clears recent Jobs only for the active Tenant', () => {
    const store = useRecentJobsStore();
    store.remember('tenant-a', 'job-a');
    store.remember('tenant-b', 'job-b');
    store.clearTenant('tenant-a');

    expect(store.jobs).toHaveLength(1);
    expect(store.jobs[0]?.tenantId).toBe('tenant-b');
  });
});
