export interface AuthUser {
  subject: string;
  displayName: string;
}

export interface AuthService {
  readonly mode: 'mock' | 'oidc';
  initialize(): Promise<AuthUser | null>;
  getAccessToken(): Promise<string | null>;
  login(returnTo: string): Promise<void>;
  handleCallback(): Promise<{ user: AuthUser; returnTo: string }>;
  logout(): Promise<void>;
}
