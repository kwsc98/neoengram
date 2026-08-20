export type ApiMode = 'mock' | 'real';
export type AuthMode = 'mock' | 'development' | 'oidc';

const defaultApiMode: ApiMode = import.meta.env.MODE === 'test' ? 'mock' : 'real';
const defaultAuthMode: AuthMode =
  import.meta.env.MODE === 'test' ? 'mock' : import.meta.env.DEV ? 'development' : 'oidc';
const defaultDevelopmentToken = import.meta.env.DEV ? 'local-development-token' : '';
const apiMode = import.meta.env.VITE_API_MODE ?? defaultApiMode;
const localGatewayProfile = import.meta.env.VITE_LOCAL_GATEWAY_PROFILE === 'true';
const configuredS3Endpoint = import.meta.env.VITE_S3_ENDPOINT?.trim();
const defaultS3Endpoint =
  apiMode === 'mock' && typeof window !== 'undefined'
    ? window.location.origin
    : import.meta.env.DEV
      ? 'http://127.0.0.1:8084'
      : '';

export const runtimeConfig = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL ?? '',
  // The mock/local profile has no DNS requirement. Central still supplies the authoritative
  // endpoint in real mode; this value only controls the mock Access Point and E2E profile.
  s3Endpoint: configuredS3Endpoint || defaultS3Endpoint,
  gatewayEndpoint:
    import.meta.env.VITE_GATEWAY_ENDPOINT ?? (import.meta.env.DEV ? 'http://127.0.0.1:8181' : ''),
  gatewayWorkloadTrustDomain: import.meta.env.VITE_GATEWAY_WORKLOAD_TRUST_DOMAIN ?? '',
  // A Web build is provisioned for one GatewayPool/EdgeCluster. Leave this
  // empty only for the loopback HTTP development profile.
  gatewayEdgeClusterId: (import.meta.env.VITE_GATEWAY_EDGE_CLUSTER_ID ?? '').trim(),
  apiMode,
  authMode: import.meta.env.VITE_AUTH_MODE ?? defaultAuthMode,
  development: {
    token: import.meta.env.VITE_DEVELOPMENT_TOKEN ?? defaultDevelopmentToken,
    principal: import.meta.env.VITE_DEVELOPMENT_PRINCIPAL ?? 'development-user',
  },
  oidc: {
    authority: import.meta.env.VITE_OIDC_AUTHORITY ?? '',
    clientId: import.meta.env.VITE_OIDC_CLIENT_ID ?? '',
    scope: import.meta.env.VITE_OIDC_SCOPE ?? 'openid profile',
  },
} as const;

if (
  import.meta.env.PROD &&
  import.meta.env.VITE_E2E_GATEWAY_MOCKS !== 'true' &&
  !localGatewayProfile &&
  (runtimeConfig.apiMode === 'mock' ||
    runtimeConfig.authMode === 'mock' ||
    runtimeConfig.authMode === 'development' ||
    Boolean(import.meta.env.VITE_DEVELOPMENT_TOKEN))
) {
  throw new Error('Mock and development authentication are disabled in production builds');
}

if (
  localGatewayProfile &&
  (runtimeConfig.apiMode !== 'real' || runtimeConfig.authMode !== 'development')
) {
  throw new Error('The local Gateway profile requires real API mode and development auth');
}

if (runtimeConfig.authMode === 'development' && !runtimeConfig.development.token) {
  throw new Error('Development authentication requires VITE_DEVELOPMENT_TOKEN');
}
