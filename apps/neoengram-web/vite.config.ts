import { fileURLToPath, URL } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig, loadEnv } from 'vite';

export default defineConfig(({ command, mode }) => {
  const env = loadEnv(mode, process.cwd(), 'VITE_');
  const proxyTarget = env.VITE_API_PROXY_TARGET || 'http://127.0.0.1:8080';
  const agentProxyTarget = env.VITE_AGENT_PROXY_TARGET || 'http://127.0.0.1:8081';
  if (
    command === 'build' &&
    (env.VITE_API_MODE === 'mock' ||
      env.VITE_AUTH_MODE === 'mock' ||
      env.VITE_AUTH_MODE === 'development' ||
      Boolean(env.VITE_DEVELOPMENT_TOKEN))
  ) {
    throw new Error('Production builds cannot include mock or development authentication');
  }

  return {
    plugins: [vue()],
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
      strictPort: true,
      proxy: {
        '/api': proxyTarget,
        '/agent': agentProxyTarget,
        '/health': proxyTarget,
      },
    },
  };
});
