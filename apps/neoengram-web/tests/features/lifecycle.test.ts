import { describe, expect, it } from 'vitest';

import { supportsResourceLifecycle } from '@/features/capabilities';
import {
  deletionCanRestore,
  deletionCanRetry,
  deletionStateLabel,
  resourceRefScope,
} from '@/features/lifecycle';

describe('resource lifecycle UI helpers', () => {
  it('gates the recycle bin on the dedicated capability', () => {
    expect(supportsResourceLifecycle(['resource_lifecycle_v1'])).toBe(true);
    expect(supportsResourceLifecycle(['artifact_catalog'])).toBe(false);
    expect(supportsResourceLifecycle(undefined)).toBe(false);
  });

  it('keeps recovery actions unavailable after purge starts', () => {
    expect(deletionCanRestore('recoverable')).toBe(true);
    expect(deletionCanRestore('purging')).toBe(false);
    expect(deletionCanRetry('blocked')).toBe(true);
    expect(deletionCanRetry('recoverable')).toBe(false);
    expect(deletionStateLabel('recoverable')).toBe('可恢复');
  });

  it('renders fully scoped resource identities', () => {
    expect(
      resourceRefScope({
        type: 'playground',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        playground_id: 'review-a',
      }),
    ).toBe('project-a / artifact-a / review-a');
  });
});
