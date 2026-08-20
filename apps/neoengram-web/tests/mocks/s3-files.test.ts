import type { IncomingMessage, ServerResponse } from 'node:http';
import { describe, expect, it } from 'vitest';

import { mockS3ObjectPlugin } from '../../vite.config';
import { resolveMockS3Path } from '../../src/mocks/s3-files';

type MockResponse = {
  statusCode: number;
  headers: Map<string, string | number>;
  body: Buffer;
  setHeader(name: string, value: string | number): void;
  end(body?: Buffer | string): void;
};

function response(): MockResponse {
  const result: MockResponse = {
    statusCode: 200,
    headers: new Map(),
    body: Buffer.alloc(0),
    setHeader(name, value) {
      this.headers.set(name.toLowerCase(), value);
    },
    end(body) {
      this.body = Buffer.isBuffer(body) ? body : Buffer.from(body ?? '');
    },
  };
  return result;
}

function middleware() {
  type Handler = (request: IncomingMessage, response: ServerResponse, next: () => void) => void;
  const handlers: Handler[] = [];
  const plugin = mockS3ObjectPlugin();
  if (typeof plugin.configureServer !== 'function') throw new Error('Missing Vite server hook');
  const configureServer = plugin.configureServer as unknown as (server: {
    middlewares: { use: (handler: Handler) => void };
  }) => void;
  configureServer({ middlewares: { use: (handler) => handlers.push(handler) } });
  return handlers[0]!;
}

describe('mock S3 object serving', () => {
  it('resolves encoded and nested keys from the shared object catalog', () => {
    const readme = resolveMockS3Path('/road-scenes-snapshot/README.md');
    expect(readme?.file?.key).toBe('README.md');

    const nested = resolveMockS3Path('/road-scenes-snapshot/images%2Fday%2Fscene-001.jpg');
    expect(nested?.file?.key).toBe('images/day/scene-001.jpg');

    expect(resolveMockS3Path('/unknown-bucket/README.md')).toBeNull();
    expect(resolveMockS3Path('/road-scenes-snapshot/missing.txt')?.file).toBeUndefined();
  });

  it('serves a known object and keeps missing objects out of SPA fallback', () => {
    const handle = middleware();
    const served = response();
    handle(
      {
        method: 'GET',
        url: '/road-scenes-snapshot/README.md?expires=9999999999999',
      } as IncomingMessage,
      served as unknown as ServerResponse,
      () => {
        throw new Error('known S3 path must be handled by the mock middleware');
      },
    );
    expect(served.statusCode).toBe(200);
    expect(served.headers.get('content-type')).toContain('text/markdown');
    expect(served.headers.get('content-disposition')).toContain('README.md');
    expect(served.body.toString('utf8')).toContain('# Road scenes');

    const missing = response();
    handle(
      { method: 'GET', url: '/road-scenes-snapshot/missing.txt' } as IncomingMessage,
      missing as unknown as ServerResponse,
      () => {
        throw new Error('known S3 bucket paths must not fall through to SPA fallback');
      },
    );
    expect(missing.statusCode).toBe(404);
    expect(missing.body.toString('utf8')).toBe('NoSuchKey');
  });
});
