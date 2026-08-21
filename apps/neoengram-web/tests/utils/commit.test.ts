import { describe, expect, it } from 'vitest';

import { commitDataLayoutLabel } from '@/utils/commit';

describe('commitDataLayoutLabel', () => {
  it('labels WholeFile commits as full-file archives', () => {
    expect(commitDataLayoutLabel('whole_file')).toBe('全文件');
  });

  it('labels FastCDC commits as chunked archives', () => {
    expect(commitDataLayoutLabel('fast_cdc')).toBe('分块');
  });
});
