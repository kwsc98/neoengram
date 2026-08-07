import {
  createRouter,
  createWebHistory,
  type Router,
  type RouterHistory,
  type RouteRecordRaw,
} from 'vue-router';

import { queryApiVersion } from '@/api/operations';
import { isApiProblem } from '@/api/problem';
import {
  supportsArtifactCatalog,
  supportsPlaygroundPreCommit,
  supportsSnapshotMaterialize,
} from '@/features/capabilities';
import { useAuthStore } from '@/stores/auth';
import { useTenantsStore } from '@/stores/tenants';

const tenantMeta = { requiresAuth: true, requiresTenant: true };
const snapshotMaterializeMeta = { ...tenantMeta, requiredCapability: 'snapshot_materialize' };
const playgroundPreCommitMeta = { ...tenantMeta, requiredCapability: 'playground_precommit' };

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
    meta: snapshotMaterializeMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/playgrounds/:playgroundId/commit',
    name: 'playground-commit',
    component: () => import('@/pages/PlaygroundCommitPage.vue'),
    meta: playgroundPreCommitMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/new',
    name: 'snapshot-create',
    component: () => import('@/pages/SnapshotCreatePage.vue'),
    meta: snapshotMaterializeMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/snapshots/:snapshotId',
    name: 'snapshot-detail',
    component: () => import('@/pages/SnapshotDetailPage.vue'),
    meta: snapshotMaterializeMeta,
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
    } catch (error) {
      if (!isApiProblem(error) || error.status !== 404) return true;
      const page = await tenants.load();
      const fallback = page.items[0];
      return fallback
        ? { name: 'tenant-overview', params: { tenantId: fallback.tenant_id } }
        : { name: 'tenant-entry' };
    }

    const requiredCapability =
      typeof to.meta.requiredCapability === 'string' ? to.meta.requiredCapability : '';
    if (!requiredCapability) return true;

    let capabilities: readonly string[] | undefined;
    try {
      capabilities = (await queryApiVersion()).data.capabilities;
    } catch {
      capabilities = undefined;
    }
    if (
      (requiredCapability === 'snapshot_materialize' &&
        supportsSnapshotMaterialize(capabilities)) ||
      (requiredCapability === 'playground_precommit' && supportsPlaygroundPreCommit(capabilities))
    ) {
      return true;
    }

    const projectId = String(to.params.projectId ?? '');
    const artifactId = String(to.params.artifactId ?? '');
    const playgroundId = String(to.params.playgroundId ?? '');
    if (projectId && artifactId && playgroundId) {
      return {
        name: 'playground-detail',
        params: { tenantId, projectId, artifactId, playgroundId },
      };
    }
    if (supportsArtifactCatalog(capabilities) && projectId && artifactId) {
      return { name: 'artifact-detail', params: { tenantId, projectId, artifactId } };
    }
    return supportsArtifactCatalog(capabilities)
      ? { name: 'artifact-list', params: { tenantId } }
      : { name: 'tenant-overview', params: { tenantId } };
  });
  return appRouter;
}

export const router = createAppRouter();
