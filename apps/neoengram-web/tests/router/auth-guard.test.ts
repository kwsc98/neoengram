import { createPinia, setActivePinia } from 'pinia';
import { http, HttpResponse } from 'msw';
import { createMemoryHistory } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { setAuthServiceForTest } from '@/auth/runtime';
import type { AuthService } from '@/auth/types';
import { createAppRouter } from '@/router';

import { server } from '../support/server';

function authService(authenticated: boolean): {
  service: AuthService;
  login: ReturnType<typeof vi.fn>;
} {
  const login = vi.fn(() => Promise.resolve());
  return {
    login,
    service: {
      mode: 'oidc',
      initialize: () =>
        Promise.resolve(authenticated ? { subject: 'user-a', displayName: 'User A' } : null),
      getAccessToken: () => Promise.resolve('mock-access-token'),
      login,
      handleCallback: () =>
        Promise.resolve({ user: { subject: 'user-a', displayName: 'User A' }, returnTo: '/' }),
      logout: () => Promise.resolve(),
    },
  };
}

afterEach(() => setAuthServiceForTest(undefined));

describe('OIDC route guard', () => {
  it('starts login and cancels protected navigation for an expired session', async () => {
    setActivePinia(createPinia());
    const { service, login } = authService(false);
    setAuthServiceForTest(service);
    const testRouter = createAppRouter(createMemoryHistory());
    await testRouter.push('/tenants/tenant-a/jobs/new');

    expect(login).toHaveBeenCalledWith('/tenants/tenant-a/jobs/new');
    expect(testRouter.currentRoute.value.path).toBe('/');
  });

  it('allows an authenticated user to enter Job routes', async () => {
    setActivePinia(createPinia());
    const { service, login } = authService(true);
    setAuthServiceForTest(service);
    const testRouter = createAppRouter(createMemoryHistory());

    await testRouter.push('/tenants/tenant-a/jobs/query');

    expect(login).not.toHaveBeenCalled();
    expect(testRouter.currentRoute.value.path).toBe('/tenants/tenant-a/jobs/query');
  });

  it('falls back to the first visible tenant for an invisible tenant route', async () => {
    setActivePinia(createPinia());
    const { service } = authService(true);
    setAuthServiceForTest(service);
    const testRouter = createAppRouter(createMemoryHistory());

    await testRouter.push('/tenants/tenant-secret/artifacts');

    expect(testRouter.currentRoute.value.path).toBe('/tenants/tenant-a/overview');
  });

  it.each([
    {
      directLink:
        '/tenants/tenant-a/projects/project-a/artifacts/artifact-a/playgrounds/playground-a/commit',
      fallback:
        '/tenants/tenant-a/projects/project-a/artifacts/artifact-a/playgrounds/playground-a',
    },
    {
      directLink: '/tenants/tenant-a/snapshots',
      fallback: '/tenants/tenant-a/artifacts',
    },
    {
      directLink: '/tenants/tenant-a/projects/project-a/artifacts/artifact-a/snapshots/new',
      fallback: '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
    },
    {
      directLink: '/tenants/tenant-a/projects/project-a/artifacts/artifact-a/snapshots/snapshot-a',
      fallback: '/tenants/tenant-a/projects/project-a/artifacts/artifact-a',
    },
  ])(
    'blocks the capability-gated direct link $directLink in artifact_catalog-only mode',
    async ({ directLink, fallback }) => {
      server.use(
        http.post('*/api/system/version/query', () =>
          HttpResponse.json({
            api_versions: [1],
            agent_protocol_versions: [1],
            capabilities: ['artifact_catalog'],
          }),
        ),
      );
      setActivePinia(createPinia());
      const { service } = authService(true);
      setAuthServiceForTest(service);
      const testRouter = createAppRouter(createMemoryHistory());

      await testRouter.push(directLink);

      expect(testRouter.currentRoute.value.path).toBe(fallback);
    },
  );

  it('allows Snapshot routes only when snapshot_materialize is advertised', async () => {
    server.use(
      http.post('*/api/system/version/query', () =>
        HttpResponse.json({
          api_versions: [1],
          agent_protocol_versions: [1],
          capabilities: ['artifact_catalog', 'snapshot_materialize'],
        }),
      ),
    );
    setActivePinia(createPinia());
    const { service } = authService(true);
    setAuthServiceForTest(service);
    const testRouter = createAppRouter(createMemoryHistory());

    await testRouter.push('/tenants/tenant-a/snapshots');

    expect(testRouter.currentRoute.value.path).toBe('/tenants/tenant-a/snapshots');
  });
});
