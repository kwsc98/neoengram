import type { ProblemDetails } from './types';

function isProblemDetails(value: unknown): value is ProblemDetails {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<ProblemDetails>;
  return (
    typeof candidate.type === 'string' &&
    typeof candidate.title === 'string' &&
    typeof candidate.status === 'number' &&
    typeof candidate.detail === 'string' &&
    typeof candidate.code === 'string' &&
    typeof candidate.request_id === 'string' &&
    typeof candidate.retryable === 'boolean'
  );
}

export class ApiProblem extends Error {
  readonly status: number;
  readonly code: string;
  readonly requestId: string;
  readonly retryable: boolean;
  readonly retryAfterMs: number | undefined;
  readonly violations: ProblemDetails['violations'];

  constructor(problem: ProblemDetails) {
    super(problem.detail);
    this.name = 'ApiProblem';
    this.status = problem.status;
    this.code = problem.code;
    this.requestId = problem.request_id;
    this.retryable = problem.retryable;
    this.retryAfterMs = problem.retry_after_ms ? Number(problem.retry_after_ms) : undefined;
    this.violations = problem.violations;
  }
}

export function toApiProblem(error: unknown, response: Response): ApiProblem {
  if (isProblemDetails(error)) return new ApiProblem(error);
  return new ApiProblem({
    type: 'urn:neoengram:problem:unexpected-response',
    title: 'Unexpected response',
    status: response.status || 500,
    detail: response.statusText || '服务返回了无法识别的响应',
    instance: new URL(response.url || window.location.href).pathname,
    code: 'UNEXPECTED_RESPONSE',
    request_id: response.headers.get('X-Request-ID') ?? 'request-id-unavailable',
    retryable: response.status >= 500,
  });
}

export function isApiProblem(error: unknown): error is ApiProblem {
  return error instanceof ApiProblem;
}
