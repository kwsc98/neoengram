import { describe, expect, it } from 'vitest';

import { DevelopmentAuthService } from '@/auth/development';

describe('development authentication', () => {
  it('keeps the fixed token in memory and presents an authenticated local principal', async () => {
    const service = new DevelopmentAuthService('local-token', 'developer-a');

    await expect(service.initialize()).resolves.toEqual({
      subject: 'developer-a',
      displayName: 'developer-a（本地开发）',
    });
    await expect(service.getAccessToken()).resolves.toBe('local-token');
    expect(sessionStorage.length).toBe(0);
    expect(localStorage.length).toBe(0);
  });

  it('rejects empty and whitespace-bearing tokens', () => {
    expect(() => new DevelopmentAuthService('', 'developer-a')).toThrow(/non-empty/);
    expect(() => new DevelopmentAuthService('not a bearer', 'developer-a')).toThrow(/whitespace/);
  });
});
