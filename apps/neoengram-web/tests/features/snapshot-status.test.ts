import { describe, expect, it } from 'vitest';

import {
  datasetProfileStateLabel,
  snapshotActivityTypeLabel,
  snapshotIntegrityLabel,
  snapshotPollInterval,
  snapshotStateLabel,
} from '@/features/snapshots/status';

describe('Snapshot status helpers', () => {
  it('polls only while delivery is creating', () => {
    expect(snapshotPollInterval('creating')).toBe(1000);
    expect(snapshotPollInterval('ready')).toBe(false);
    expect(snapshotPollInterval('abnormal')).toBe(false);
    expect(snapshotPollInterval(undefined)).toBe(false);
  });

  it('maps public Snapshot state without simulated delivery progress', () => {
    expect(snapshotStateLabel('ready')).toBe('可用');
    expect(snapshotIntegrityLabel('verified')).toBe('已校验');
  });

  it('labels activity and Dataset Profile states from the public API', () => {
    expect(snapshotActivityTypeLabel('created')).toBe('Snapshot 已创建');
    expect(datasetProfileStateLabel('not_declared')).toBe('未声明');
    expect(datasetProfileStateLabel('rejected')).toBe('已拒绝');
  });
});
