import { runtimeConfig } from '@/config';

import { DevelopmentAuthService } from './development';
import { MockAuthService } from './mock';
import { OidcAuthService } from './oidc';
import type { AuthService } from './types';

let service: AuthService | undefined;

export function getAuthService(): AuthService {
  service ??= createAuthService();
  return service;
}

function createAuthService(): AuthService {
  if (runtimeConfig.authMode === 'mock') return new MockAuthService();
  if (runtimeConfig.authMode === 'development') {
    return new DevelopmentAuthService(
      runtimeConfig.development.token,
      runtimeConfig.development.principal,
    );
  }
  return new OidcAuthService(
    runtimeConfig.oidc.authority,
    runtimeConfig.oidc.clientId,
    runtimeConfig.oidc.scope,
  );
}

export function setAuthServiceForTest(next: AuthService | undefined): void {
  service = next;
}
