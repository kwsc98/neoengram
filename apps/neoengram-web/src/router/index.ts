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
  supportsWorkspacePreCommit,
  supportsS3ReadonlyAccessPoint,
  supportsResourceLifecycle,
  supportsSnapshotMaterialize,
} from '@/features/capabilities';
import { useAuthStore } from '@/stores/auth';
import { useTenantsStore } from '@/stores/tenants';

const tenantMeta = { requiresAuth: true, requiresTenant: true };
const snapshotMaterializeMeta = { ...tenantMeta, requiredCapability: 'commit_materialization_v2' };
const workspacePreCommitMeta = { ...tenantMeta, requiredCapability: 'workspace_precommit' };
const s3ReadonlyMeta = {
  ...tenantMeta,
  requiredCapability: 's3_readonly_access_point',
  requiredPermission: 's3.access.read',
};
const resourceLifecycleMeta = {
  ...tenantMeta,
  requiredCapability: 'resource_lifecycle_v1',
  requiredPermission: 'resource.lifecycle.read',
};

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
    path: '/tenants/:tenantId/projects',
    name: 'project-list',
    component: () => import('@/pages/ProjectListPage.vue'),
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
    path: '/tenants/:tenantId/object-storage',
    name: 'object-storage-list',
    component: () => import('@/pages/ObjectStorageListPage.vue'),
    meta: s3ReadonlyMeta,
  },
  {
    path: '/tenants/:tenantId/object-storage/:accessPointId',
    name: 'object-storage-browser',
    component: () => import('@/pages/ObjectStorageBrowserPage.vue'),
    meta: s3ReadonlyMeta,
  },
  {
    path: '/tenants/:tenantId/recycle-bin',
    name: 'recycle-bin',
    component: () => import('@/pages/RecycleBinPage.vue'),
    meta: resourceLifecycleMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId',
    name: 'artifact-detail',
    component: () => import('@/pages/ArtifactDetailPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/commits/:commitId',
    name: 'commit-detail',
    component: () => import('@/pages/CommitDetailPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/workspaces',
    name: 'workspace-list',
    component: () => import('@/pages/WorkspaceListPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/workspaces/:workspaceId',
    name: 'workspace-detail',
    component: () => import('@/pages/WorkspaceDetailPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/snapshots',
    name: 'snapshot-list',
    component: () => import('@/pages/SnapshotListPage.vue'),
    meta: snapshotMaterializeMeta,
  },
  {
    path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/workspaces/:workspaceId/commit',
    name: 'workspace-commit',
    component: () => import('@/pages/WorkspaceCommitPage.vue'),
    meta: workspacePreCommitMeta,
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
    path: '/tenants/:tenantId/tasks',
    name: 'task-list',
    component: () => import('@/pages/TaskListPage.vue'),
    meta: tenantMeta,
  },
  {
    path: '/tenants/:tenantId/tasks/:taskId',
    name: 'task-detail',
    component: () => import('@/pages/TaskDetailPage.vue'),
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

    const requiredPermission =
      typeof to.meta.requiredPermission === 'string' ? to.meta.requiredPermission : '';
    const tenantPermissions = tenants.byId(tenantId)?.permissions as readonly string[] | undefined;
    if (requiredPermission && !tenantPermissions?.includes(requiredPermission)) {
      return { name: 'tenant-overview', params: { tenantId } };
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
      (requiredCapability === 'commit_materialization_v2' &&
        supportsSnapshotMaterialize(capabilities)) ||
      (requiredCapability === 'workspace_precommit' && supportsWorkspacePreCommit(capabilities)) ||
      (requiredCapability === 's3_readonly_access_point' &&
        supportsS3ReadonlyAccessPoint(capabilities)) ||
      (requiredCapability === 'resource_lifecycle_v1' && supportsResourceLifecycle(capabilities))
    ) {
      return true;
    }

    const projectId = String(to.params.projectId ?? '');
    const artifactId = String(to.params.artifactId ?? '');
    const workspaceId = String(to.params.workspaceId ?? '');
    if (projectId && artifactId && workspaceId) {
      return {
        name: 'workspace-detail',
        params: { tenantId, projectId, artifactId, workspaceId },
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
