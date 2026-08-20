import { fileURLToPath, URL } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig, loadEnv, type Plugin } from 'vite';

import { resolveMockS3Path } from './src/mocks/s3-files';

export function mockS3ObjectPlugin(): Plugin {
  return {
    name: 'neoengram-mock-s3-objects',
    configureServer(server) {
      server.middlewares.use((request, response, next) => {
        let url: URL;
        try {
          url = new URL(request.url ?? '/', 'http://neoengram.mock');
        } catch {
          next();
          return;
        }

        const resolved = resolveMockS3Path(url.pathname);
        if (!resolved) {
          next();
          return;
        }
        if (request.method !== 'GET' && request.method !== 'HEAD') {
          response.statusCode = 405;
          response.setHeader('Allow', 'GET, HEAD');
          response.end('Method Not Allowed');
          return;
        }
        if (!resolved.key || resolved.key.includes('\\')) {
          response.statusCode = 400;
          response.end('Invalid object key');
          return;
        }
        const expires = url.searchParams.get('expires');
        if (expires !== null) {
          const expiresAt = Number(expires);
          if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
            response.statusCode = 403;
            response.end('Presigned URL has expired');
            return;
          }
        }
        if (!resolved.file) {
          response.statusCode = 404;
          response.end('NoSuchKey');
          return;
        }

        const body = Buffer.from(resolved.file.body, 'utf8');
        const fileName = resolved.file.key.split('/').pop() ?? 'download';
        response.statusCode = 200;
        response.setHeader('Content-Type', resolved.file.content_type);
        response.setHeader('Content-Length', body.byteLength);
        response.setHeader('Accept-Ranges', 'bytes');
        response.setHeader('ETag', resolved.file.etag ?? '');
        response.setHeader(
          'Last-Modified',
          new Date(Number(resolved.file.last_modified_unix_ms ?? Date.now())).toUTCString(),
        );
        response.setHeader(
          'Content-Disposition',
          `attachment; filename="${fileName.replaceAll('"', '')}"`,
        );
        if (request.method === 'HEAD') {
          response.end();
        } else {
          response.end(body);
        }
      });
    },
  };
}

export default defineConfig(({ command, mode }) => {
  const env = loadEnv(mode, process.cwd(), 'VITE_');
  const proxyTarget = env.VITE_API_PROXY_TARGET || 'http://127.0.0.1:8080';
  const localGatewayBuild =
    command === 'build' && mode === 'local-gateway' && env.VITE_LOCAL_GATEWAY_PROFILE === 'true';
  const gatewayE2eBuild =
    command === 'build' &&
    mode === 'mock' &&
    process.env.SYNAPSE_WEB_E2E_GATEWAY === '1' &&
    process.env.VITE_E2E_GATEWAY_MOCKS === 'true';
  if (
    command === 'build' &&
    !gatewayE2eBuild &&
    !localGatewayBuild &&
    (env.VITE_API_MODE === 'mock' ||
      env.VITE_AUTH_MODE === 'mock' ||
      env.VITE_AUTH_MODE === 'development' ||
      Boolean(env.VITE_DEVELOPMENT_TOKEN))
  ) {
    throw new Error('Production builds cannot include mock or development authentication');
  }
  if (localGatewayBuild) {
    const loopbackOrigins = [
      env.VITE_API_BASE_URL,
      env.VITE_GATEWAY_ENDPOINT,
      env.VITE_S3_ENDPOINT,
    ].filter((value): value is string => Boolean(value));
    const hasNonLoopbackOrigin = loopbackOrigins.some((value) => {
      try {
        const url = new URL(value);
        return (
          url.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)
        );
      } catch {
        return true;
      }
    });
    if (
      env.VITE_API_MODE !== 'real' ||
      env.VITE_AUTH_MODE !== 'development' ||
      !env.VITE_DEVELOPMENT_TOKEN ||
      hasNonLoopbackOrigin
    ) {
      throw new Error(
        'The local Gateway build requires real API mode, development auth, a token, and loopback HTTP origins',
      );
    }
  }

  return {
    plugins: [vue(), ...(mode === 'mock' ? [mockS3ObjectPlugin()] : [])],
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./src', import.meta.url)),
      },
    },
    build: {
      rolldownOptions: {
        output: {
          codeSplitting: {
            groups: [
              {
                name: 'element-plus',
                test: /[\\/]node_modules[\\/](?:element-plus|@element-plus)[\\/]/,
              },
            ],
          },
        },
      },
    },
    server: {
      host: '127.0.0.1',
      port: 4173,
      // Mock development can share a workstation with another console instance. Let Vite
      // choose the next free port; real development keeps the fixed-port failure visible.
      strictPort: mode !== 'mock',
      proxy: {
        '/api': proxyTarget,
        '/health': proxyTarget,
      },
    },
  };
});
