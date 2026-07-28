import { describe, expect, it } from 'vitest';

import { jobPollInterval } from '@/features/jobs/polling';

describe('Job polling policy', () => {
  it('polls active states every two seconds', () => {
    expect(jobPollInterval('queued')).toBe(2000);
    expect(jobPollInterval('running')).toBe(2000);
    expect(jobPollInterval('publishing')).toBe(2000);
  });

  it('stops for Prepared and terminal states', () => {
    expect(jobPollInterval('prepared')).toBe(false);
    expect(jobPollInterval('succeeded')).toBe(false);
    expect(jobPollInterval('recovery_required')).toBe(false);
  });
});
