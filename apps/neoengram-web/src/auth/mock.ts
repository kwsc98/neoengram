import type { AuthService, AuthUser } from './types';

const mockUser: AuthUser = {
  subject: 'mock-user',
  displayName: '开发用户',
};

export class MockAuthService implements AuthService {
  readonly mode = 'mock' as const;

  initialize(): Promise<AuthUser> {
    return Promise.resolve(mockUser);
  }

  getAccessToken(): Promise<string> {
    return Promise.resolve('mock-access-token');
  }

  login(): Promise<void> {
    return Promise.resolve();
  }

  handleCallback(): Promise<{ user: AuthUser; returnTo: string }> {
    return Promise.resolve({ user: mockUser, returnTo: '/' });
  }

  logout(): Promise<void> {
    return Promise.resolve();
  }
}
