import { defineConfig, devices } from '@playwright/test';

const noProxy = new Set(
  `${process.env.NO_PROXY ?? ''},${process.env.no_proxy ?? ''}`
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean),
);
noProxy.add('localhost');
noProxy.add('127.0.0.1');
process.env.NO_PROXY = [...noProxy].join(',');
process.env.no_proxy = process.env.NO_PROXY;

const webPort = Number(process.env.PLAYWRIGHT_PORT ?? '4174');
const baseURL = `http://localhost:${webPort}`;
const gatewayAgentPort = webPort + 1;
const gatewayControlPort = webPort + 2;
const gatewayPeerPort = webPort + 3;

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: false,
  timeout: 30_000,
  expect: { timeout: 8000 },
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: {
    command:
      `NEOENGRAM_WEB_E2E_GATEWAY=1 VITE_E2E_GATEWAY_MOCKS=true ` +
      `VITE_GATEWAY_ENDPOINT=http://127.0.0.1:${gatewayAgentPort} ` +
      `VITE_S3_ENDPOINT=http://127.0.0.1:${webPort} npx vite build --mode mock && ` +
      `cargo run -p neoengram-gateway --offline -- ` +
      `--edge-cluster-id edge-playwright ` +
      `--gateway-pool-id pool-playwright ` +
      `--gateway-replica-id replica-playwright ` +
      `--agent-listen 127.0.0.1:${gatewayAgentPort} ` +
      `--control-listen 127.0.0.1:${gatewayControlPort} ` +
      `--peer-listen 127.0.0.1:${gatewayPeerPort} ` +
      `--public-listen 127.0.0.1:${webPort} ` +
      `--console-host localhost ` +
      `--s3-host 127.0.0.1 ` +
      `--web-root dist`,
    url: baseURL,
    reuseExistingServer: false,
    timeout: 180_000,
  },
  projects: [
    {
      name: 'desktop',
      use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 900 } },
    },
    { name: 'mobile', use: { ...devices['Pixel 7'], viewport: { width: 390, height: 844 } } },
  ],
});
