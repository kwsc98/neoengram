import { describe, expect, it } from 'vitest';

import { createJobFormSchema, parsePathLines } from '@/features/jobs/create-form';

function validForm() {
  return {
    tenantId: 'tenant-a',
    projectId: 'project-a',
    artifactId: 'artifact-a',
    playgroundId: 'playground-a',
    jobId: 'job-a',
    revision: '0',
    digest: 'a'.repeat(64),
    deadline: new Date(Date.now() + 60_000),
    all: false,
    paths: ['dataset/images'],
  };
}

describe('create Job form', () => {
  it('normalizes path lines without reordering them', () => {
    expect(parsePathLines(' b/file \n\na/file\n')).toEqual(['b/file', 'a/file']);
  });

  it('requires paths unless all is selected', () => {
    expect(createJobFormSchema.safeParse({ ...validForm(), paths: [] }).success).toBe(false);
    expect(createJobFormSchema.safeParse({ ...validForm(), all: true, paths: [] }).success).toBe(
      true,
    );
  });

  it('rejects duplicate and non-relative paths', () => {
    expect(createJobFormSchema.safeParse({ ...validForm(), paths: ['a', 'a'] }).success).toBe(
      false,
    );
    expect(createJobFormSchema.safeParse({ ...validForm(), paths: ['/absolute'] }).success).toBe(
      false,
    );
  });
});
