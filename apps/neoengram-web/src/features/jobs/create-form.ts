import { z } from 'zod';

const resourceId = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const digest = /^[0-9a-f]{64}$/;

export const createJobFormSchema = z
  .object({
    tenantId: z.string().regex(resourceId, '请输入合法 Tenant ID'),
    projectId: z.string().regex(resourceId, '请输入合法 Project ID'),
    artifactId: z.string().regex(resourceId, '请输入合法 Artifact ID'),
    playgroundId: z.string().regex(resourceId, '请输入合法 Playground ID'),
    jobId: z.string().regex(resourceId, '请输入合法 Job ID'),
    revision: z
      .string()
      .regex(/^(0|[1-9][0-9]{0,19})$/, 'Revision 必须是 canonical decimal string'),
    digest: z.string().regex(digest, 'Digest 必须是 64 位小写十六进制'),
    deadline: z.date().refine((value) => value.getTime() > Date.now(), 'Deadline 必须晚于当前时间'),
    all: z.boolean(),
    paths: z.array(z.string().min(1)).max(4096),
  })
  .superRefine((value, context) => {
    if (!value.all && value.paths.length === 0) {
      context.addIssue({
        code: 'custom',
        path: ['paths'],
        message: '未选择全部路径时至少填写一个路径',
      });
    }
    if (new Set(value.paths).size !== value.paths.length) {
      context.addIssue({ code: 'custom', path: ['paths'], message: '路径不能重复' });
    }
    const invalidPath = value.paths.some(
      (path) =>
        path.startsWith('/') ||
        path.includes('\\') ||
        path.split('/').some((part) => !part || part === '.' || part === '..'),
    );
    if (invalidPath) {
      context.addIssue({
        code: 'custom',
        path: ['paths'],
        message: '路径必须是合法的 repository-relative 路径',
      });
    }
  });

export function parsePathLines(value: string): string[] {
  return value
    .split('\n')
    .map((path) => path.trim())
    .filter(Boolean);
}
