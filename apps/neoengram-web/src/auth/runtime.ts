import { runtimeConfig } from '@/config';

import { MockAuthService } from './mock';
import { OidcAuthService } from './oidc';
import type { AuthService } from './types';

let service: AuthService | undefined;

export function getAuthService(): AuthService {
  service ??=
    runtimeConfig.authMode === 'mock'
      ? new MockAuthService()
      : new OidcAuthService(
          runtimeConfig.oidc.authority,
          runtimeConfig.oidc.clientId,
          runtimeConfig.oidc.scope,
        );
  return service;
}

export function setAuthServiceForTest(next: AuthService | undefined): void {
  service = next;
}
