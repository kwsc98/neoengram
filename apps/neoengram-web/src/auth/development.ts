import type { AuthService, AuthUser } from './types';

export class DevelopmentAuthService implements AuthService {
  readonly mode = 'development' as const;
  private readonly user: AuthUser;

  constructor(
    private readonly token: string,
    principal: string,
  ) {
    if (!token || /\s/.test(token)) {
      throw new Error('Development Bearer token must be non-empty and contain no whitespace');
    }
    this.user = {
      subject: principal,
      displayName: `${principal}（本地开发）`,
    };
  }

  initialize(): Promise<AuthUser> {
    return Promise.resolve(this.user);
  }

  getAccessToken(): Promise<string> {
    return Promise.resolve(this.token);
  }

  login(): Promise<void> {
    return Promise.resolve();
  }

  handleCallback(): Promise<{ user: AuthUser; returnTo: string }> {
    return Promise.resolve({ user: this.user, returnTo: '/' });
  }

  logout(): Promise<void> {
    return Promise.resolve();
  }
}
