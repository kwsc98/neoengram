import {
  createRouter,
  createWebHistory,
  type Router,
  type RouterHistory,
  type RouteRecordRaw,
} from 'vue-router';

import { isApiProblem } from '@/api/problem';
import { useAuthStore } from '@/stores/auth';
import { useTenantsStore } from '@/stores/tenants';

const tenantMeta = { requiresAuth: true, requiresTenant: true };

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    name: 'tenant-entry',
    component: () => import('@/pages/TenantEntryPage.vue'),
    meta: { requiresAuth: true },
  },
  {
    path: '/tenants/:tenantId/overview',
    name: 'tenant-overview',
    component: () => import('@/pages/DashboardPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/artifacts',
    name: 'artifact-list',
    component: () => import('@/pages/ArtifactListPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/storage-volumes',
    name: 'storage-volume-list',
    component: () => import('@/pages/StorageVolumeListPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId',
    name: 'artifact-detail',
    component: () => import('@/pages/ArtifactDetailPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/playgrounds',
    name: 'playground-list',
    component: () => import('@/pages/PlaygroundListPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/playgrounds/:playgroundId',
    name: 'playground-detail',
    component: () => import('@/pages/PlaygroundDetailPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/snapshots',
    name: 'snapshot-list',
    component: () => import('@/pages/SnapshotListPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/playgrounds/:playgroundId/commit',
    name: 'playground-commit-prototype',
    component: () => import('@/pages/ReleaseWorkbenchPrototypePage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/new',
    name: 'snapshot-delivery-prototype',
    component: () => import('@/pages/SnapshotDeliveryPrototypePage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/:snapshotId',
    name: 'snapshot-detail',
    component: () => import('@/pages/SnapshotDetailPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/jobs/new',
    name: 'job-create',
    component: () => import('@/pages/CreateJobPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/jobs/query',
    name: 'job-query',
    component: () => import('@/pages/QueryJobPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/jobs/:jobId',
    name: 'job-detail',
    component: () => import('@/pages/JobDetailPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/auth/callback',
    name: 'auth-callback',
    component: () => import('@/pages/AuthCallbackPage.vue'),
  },
  { path: '/:pathMatch(.*)*', redirect: '/' },
];

export function createAppRouter(history: RouterHistory = createWebHistory()): Router {
  const appRouter = createRouter({ history, routes });
  appRouter.beforeEach(async (to) => {
    const auth = useAuthStore();
    await auth.initialize();
    if (to.meta.requiresAuth && !auth.authenticated) {
      await auth.login(to.fullPath);
      return false;
    }
    if (!to.meta.requiresTenant) return true;

    const tenantId = String(to.params.tenantId ?? '');
    const tenants = useTenantsStore();
    try {
      await tenants.ensure(tenantId);
      tenants.remember(tenantId);
      return true;
    } catch (error) {
      if (!isApiProblem(error) || error.status !== 404) return true;
      const page = await tenants.load();
      const fallback = page.items[0];
      return fallback
        ? { name: 'tenant-overview', params: { tenantId: fallback.tenant_id } }
        : { name: 'tenant-entry' };
    }
  });
  return appRouter;
}

export const router = createAppRouter();
