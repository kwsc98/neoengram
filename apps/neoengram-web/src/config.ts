export type ApiMode = 'mock' | 'real';
export type AuthMode = 'mock' | 'development' | 'oidc';

const defaultApiMode: ApiMode = import.meta.env.MODE === 'test' ? 'mock' : 'real';
const defaultAuthMode: AuthMode =
  import.meta.env.MODE === 'test' ? 'mock' : import.meta.env.DEV ? 'development' : 'oidc';
const defaultDevelopmentToken = import.meta.env.DEV ? 'local-development-token' : '';

export const runtimeConfig = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL ?? '',
  agentEndpoint:
    import.meta.env.VITE_AGENT_ENDPOINT ?? (import.meta.env.DEV ? 'http://127.0.0.1:8081' : ''),
  apiMode: import.meta.env.VITE_API_MODE ?? defaultApiMode,
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
  (runtimeConfig.apiMode === 'mock' ||
    runtimeConfig.authMode === 'mock' ||
    runtimeConfig.authMode === 'development' ||
    Boolean(import.meta.env.VITE_DEVELOPMENT_TOKEN))
) {
  throw new Error('Mock and development authentication are disabled in production builds');
}

if (runtimeConfig.authMode === 'development' && !runtimeConfig.development.token) {
  throw new Error('Development authentication requires VITE_DEVELOPMENT_TOKEN');
}
