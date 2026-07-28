export type ApiMode = 'mock' | 'real';
export type AuthMode = 'mock' | 'oidc';

const defaultApiMode: ApiMode = import.meta.env.MODE === 'test' ? 'mock' : 'real';
const defaultAuthMode: AuthMode = import.meta.env.MODE === 'test' ? 'mock' : 'oidc';

export const runtimeConfig = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL ?? '',
  apiMode: import.meta.env.VITE_API_MODE ?? defaultApiMode,
  authMode: import.meta.env.VITE_AUTH_MODE ?? defaultAuthMode,
  oidc: {
    authority: import.meta.env.VITE_OIDC_AUTHORITY ?? '',
    clientId: import.meta.env.VITE_OIDC_CLIENT_ID ?? '',
    scope: import.meta.env.VITE_OIDC_SCOPE ?? 'openid profile',
  },
} as const;

if (
  import.meta.env.PROD &&
  (runtimeConfig.apiMode === 'mock' || runtimeConfig.authMode === 'mock')
) {
  throw new Error('Mock API and authentication are disabled in production builds');
}
