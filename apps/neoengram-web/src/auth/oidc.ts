import { UserManager, WebStorageStateStore, type User } from 'oidc-client-ts';

import type { AuthService, AuthUser } from './types';

function toAuthUser(user: User): AuthUser {
  const displayName = user.profile.name || user.profile.preferred_username || user.profile.sub;
  return { subject: user.profile.sub, displayName };
}

export class OidcAuthService implements AuthService {
  readonly mode = 'oidc' as const;
  private readonly manager: UserManager;

  constructor(authority: string, clientId: string, scope: string) {
    if (!authority || !clientId) {
      throw new Error('OIDC mode requires VITE_OIDC_AUTHORITY and VITE_OIDC_CLIENT_ID');
    }
    this.manager = new UserManager({
      authority,
      client_id: clientId,
      redirect_uri: `${window.location.origin}/auth/callback`,
      post_logout_redirect_uri: window.location.origin,
      response_type: 'code',
      scope,
      automaticSilentRenew: true,
      userStore: new WebStorageStateStore({ store: window.sessionStorage }),
    });
  }

  async initialize(): Promise<AuthUser | null> {
    const user = await this.manager.getUser();
    return user && !user.expired ? toAuthUser(user) : null;
  }

  async getAccessToken(): Promise<string | null> {
    const user = await this.manager.getUser();
    return user && !user.expired ? user.access_token : null;
  }

  async login(returnTo: string): Promise<void> {
    await this.manager.signinRedirect({ state: { returnTo } });
  }

  async handleCallback(): Promise<{ user: AuthUser; returnTo: string }> {
    const user = await this.manager.signinRedirectCallback();
    const state = user.state as { returnTo?: unknown } | undefined;
    const returnTo = typeof state?.returnTo === 'string' ? state.returnTo : '/';
    return { user: toAuthUser(user), returnTo };
  }

  async logout(): Promise<void> {
    await this.manager.signoutRedirect();
  }
}
