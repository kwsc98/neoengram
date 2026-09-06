import { VueQueryPlugin, QueryClient } from '@tanstack/vue-query';
import { flushPromises, shallowMount } from '@vue/test-utils';
import { ElMessageBox } from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import WorkspaceCommitPage from '@/pages/WorkspaceCommitPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  cancelWorkspacePreCommit: vi.fn(),
  commitWorkspace: vi.fn(),
  queryApiVersion: vi.fn(),
  queryWorkspace: vi.fn(),
  queryWorkspaceChangeList: vi.fn(),
  queryWorkspacePreCommit: vi.fn(),
  restartWorkspacePreCommit: vi.fn(),
  startWorkspacePreCommit: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const headCommitId = 'a'.repeat(64);
const sourceIndexVersion = { revision: '2', digest: 'sha256:index' };

function workspace(
  activePreCommitId?: string,
  indexVersion: { revision: string; digest: string } = sourceIndexVersion,
  storageAvailability: 'ready' | 'degraded' | 'unavailable' | 'unknown' = 'ready',
) {
  return {
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    workspace_id: 'workspace-a',
    storage_volume_id: 'volume-a',
    region: 'region-a',
    display_name: 'Workspace A',
    head_commit_id: headCommitId,
    index_version: indexVersion,
    state: 'ready' as const,
    storage_availability: storageAvailability,
    ...(activePreCommitId ? { active_precommit_id: activePreCommitId } : {}),
    created_at_unix_ms: '1',
    updated_at_unix_ms: '2',
  };
}

async function mountPage(
  activePreCommitId?: string,
  routedPreCommitId?: string,
  precommitOverrides: Record<string, unknown> = {},
  storageAvailability: 'ready' | 'degraded' | 'unavailable' | 'unknown' = 'ready',
) {
  api.queryApiVersion.mockResolvedValue({
    data: {
      api_version: 1,
      agent_wire_version: 1,
      capabilities: [
        'artifact_commit_graph',
        'commit_materialization_v2',
        'workspace_browser',
        'workspace_precommit',
      ],
    },
    requestId: 'request-version',
  });
  api.queryWorkspace.mockResolvedValue({
    data: {
      workspace: workspace(activePreCommitId, sourceIndexVersion, storageAvailability),
    },
    requestId: 'request-workspace',
  });
  api.queryWorkspacePreCommit.mockResolvedValue({
    data: {
      precommit: {
        tenant_id: 'tenant-a',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        workspace_id: 'workspace-a',
        precommit_id: 'precommit-a',
        precommit_request_id: 'request-a',
        attempt: 1,
        state: 'running',
        phase: 'scanning',
        progress: { percent: 20, files_completed: '2', bytes_completed: '20' },
        checks: [],
        warnings: [],
        blockers: [],
        source_index_version: sourceIndexVersion,
        data_layout: 'fast_cdc',
        created_at_unix_ms: '1',
        updated_at_unix_ms: '2',
        ...precommitOverrides,
      },
    },
    requestId: 'request-precommit',
  });
  api.queryWorkspaceChangeList.mockResolvedValue({
    data: {
      source: 'precommit',
      precommit_id: 'precommit-a',
      index_version: { revision: '2', digest: 'sha256:index' },
      summary: {
        files_added: '0',
        files_modified: '0',
        files_deleted: '0',
        files_renamed: '0',
        bytes_added: '0',
        bytes_removed: '0',
      },
      items: [],
    },
    requestId: 'request-changes',
  });

  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/workspaces/:workspaceId/commit',
        component: WorkspaceCommitPage,
      },
    ],
  });
  await router.push(
    `/tenants/tenant-a/projects/project-a/artifacts/artifact-a/workspaces/workspace-a/commit${routedPreCommitId ? `?precommit_id=${routedPreCommitId}` : ''}`,
  );
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      permissions: ['workspace.read', 'workspace.create'],
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '2',
    },
  ];
  const wrapper = shallowMount(WorkspaceCommitPage, {
    global: {
      plugins: [pinia, [VueQueryPlugin, { queryClient }], router],
      stubs: {
        PageHeading: {
          template: '<section><slot /><slot name="actions" /></section>',
        },
      },
    },
  });
  await flushPromises();
  return { wrapper, queryClient };
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('Workspace Commit page recovery', () => {
  it('does not start a Pre-commit when the page is opened without an active id', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryWorkspace).toHaveBeenCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
      'workspace-a',
    );
    expect(api.startWorkspacePreCommit).not.toHaveBeenCalled();
    expect(api.queryWorkspacePreCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('does not query frozen changes while a running Pre-commit has no candidate', async () => {
    const { wrapper, queryClient } = await mountPage('precommit-a');

    expect(api.queryWorkspacePreCommit).toHaveBeenCalledWith('tenant-a', 'precommit-a');
    expect(api.queryWorkspaceChangeList).not.toHaveBeenCalled();
    expect(api.startWorkspacePreCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('queries ready candidate changes again under a new key when the candidate changes', async () => {
    const firstCandidate = { revision: '3', digest: 'sha256:candidate-3' };
    const secondCandidate = { revision: '4', digest: 'sha256:candidate-4' };
    const { wrapper, queryClient } = await mountPage('precommit-a', undefined, {
      state: 'ready',
      phase: 'idle',
      candidate_index_version: firstCandidate,
    });

    expect(api.queryWorkspaceChangeList).toHaveBeenCalledTimes(1);
    expect(api.queryWorkspaceChangeList).toHaveBeenLastCalledWith(
      expect.objectContaining({
        tenant_id: 'tenant-a',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        workspace_id: 'workspace-a',
        precommit_id: 'precommit-a',
      }),
    );

    api.queryWorkspacePreCommit.mockResolvedValueOnce({
      data: {
        precommit: {
          tenant_id: 'tenant-a',
          project_id: 'project-a',
          artifact_id: 'artifact-a',
          workspace_id: 'workspace-a',
          precommit_id: 'precommit-a',
          precommit_request_id: 'request-a',
          attempt: 1,
          state: 'ready',
          phase: 'idle',
          progress: { percent: 100, files_completed: '2', bytes_completed: '20' },
          checks: [],
          warnings: [],
          blockers: [],
          source_index_version: sourceIndexVersion,
          data_layout: 'fast_cdc',
          candidate_index_version: secondCandidate,
          created_at_unix_ms: '1',
          updated_at_unix_ms: '3',
        },
      },
      requestId: 'request-precommit-refresh',
    });

    const refreshButton = wrapper
      .findAll('el-button, el-button-stub')
      .find((button) => button.text().trim() === '刷新');
    expect(refreshButton).toBeDefined();
    await refreshButton!.trigger('click');
    await flushPromises();

    expect(api.queryWorkspaceChangeList).toHaveBeenCalledTimes(2);
    const changeQueryKeys = queryClient
      .getQueryCache()
      .getAll()
      .filter(
        (query) => query.queryKey[0] === 'workspace-changes' && query.state.data !== undefined,
      )
      .map((query) => query.queryKey);
    expect(changeQueryKeys).toHaveLength(2);
    expect(new Set(changeQueryKeys.map((queryKey) => JSON.stringify(queryKey))).size).toBe(2);

    wrapper.unmount();
    queryClient.clear();
  });

  it('refreshes the Workspace after cancel and starts redetection from the newer revision', async () => {
    const nextIndexVersion = { revision: '3', digest: 'sha256:index-3' };
    const { wrapper, queryClient } = await mountPage('precommit-a', undefined, {
      state: 'ready',
      phase: 'idle',
      candidate_index_version: { revision: '3', digest: 'sha256:candidate-3' },
    });
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue(undefined as never);
    api.cancelWorkspacePreCommit.mockResolvedValue({
      data: {
        precommit: {
          precommit_id: 'precommit-a',
          state: 'cancelled',
          data_layout: 'fast_cdc',
        },
        workspace: workspace(undefined, nextIndexVersion),
        request_replayed: false,
        execution_reused: false,
      },
      requestId: 'request-cancel',
    });
    api.queryWorkspace.mockResolvedValue({
      data: { workspace: workspace(undefined, nextIndexVersion) },
      requestId: 'request-workspace-refresh',
    });
    api.startWorkspacePreCommit.mockResolvedValue({
      data: {
        precommit: { precommit_id: 'precommit-b', state: 'running', data_layout: 'fast_cdc' },
        workspace: workspace('precommit-b', nextIndexVersion),
        request_replayed: false,
        execution_reused: false,
      },
      requestId: 'request-start',
    });

    const redetectButton = wrapper
      .findAll('el-button, el-button-stub')
      .find((button) => button.text().trim() === '重新检测');
    expect(redetectButton).toBeDefined();
    await redetectButton!.trigger('click');
    await flushPromises();

    expect(api.cancelWorkspacePreCommit).toHaveBeenCalledTimes(1);
    expect(api.startWorkspacePreCommit).toHaveBeenCalledWith(
      expect.objectContaining({ expected_index_version: nextIndexVersion }),
    );
    const cancelOrder = api.cancelWorkspacePreCommit.mock.invocationCallOrder[0]!;
    const refreshedWorkspaceOrder = api.queryWorkspace.mock.invocationCallOrder.find(
      (order) => order > cancelOrder,
    );
    const startOrder = api.startWorkspacePreCommit.mock.invocationCallOrder[0]!;
    expect(refreshedWorkspaceOrder).toBeDefined();
    expect(cancelOrder).toBeLessThan(refreshedWorkspaceOrder!);
    expect(refreshedWorkspaceOrder!).toBeLessThan(startOrder);

    wrapper.unmount();
    queryClient.clear();
  });

  it('recovers a route-pinned session without starting a new one', async () => {
    const { wrapper, queryClient } = await mountPage(undefined, 'precommit-a', {
      state: 'cancelled',
      phase: 'idle',
    });

    expect(api.queryWorkspacePreCommit).toHaveBeenCalledWith('tenant-a', 'precommit-a');
    expect(api.startWorkspacePreCommit).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain('失败重试');
    expect(wrapper.text()).not.toContain('没有活动 Pre-commit');

    wrapper.unmount();
    queryClient.clear();
  });

  it('rejects a routed Pre-commit that belongs to another Workspace scope', async () => {
    const { wrapper, queryClient } = await mountPage(undefined, 'precommit-a', {
      workspace_id: 'workspace-b',
    });

    expect(api.queryWorkspaceChangeList).not.toHaveBeenCalled();
    const scopeAlert = wrapper
      .findAllComponents(ApiProblemAlert)
      .find((alert) => (alert.props('error') as Error | undefined)?.message.includes('不属于'));
    expect(scopeAlert).toBeDefined();

    wrapper.unmount();
    queryClient.clear();
  });

  it('keeps a frozen candidate reviewable and committable while storage is unavailable', async () => {
    const { wrapper, queryClient } = await mountPage(
      'precommit-a',
      undefined,
      {
        state: 'ready',
        phase: 'idle',
        candidate_index_version: { revision: '3', digest: 'sha256:candidate-3' },
      },
      'unavailable',
    );

    const storageAlert = wrapper
      .findAll('el-alert, el-alert-stub')
      .find((alert) => alert.attributes('title') === 'StorageVolume 当前不可达');
    expect(storageAlert).toBeDefined();
    expect(storageAlert?.attributes('description')).toContain('中心保存的候选变化仍可审查和提交');
    expect(wrapper.text()).toContain('填写 Commit 信息');
    expect(wrapper.text()).not.toContain('重新检测');
    expect(api.queryWorkspaceChangeList).toHaveBeenCalledTimes(1);

    wrapper.unmount();
    queryClient.clear();
  });

  it('gates an Agent-backed retry while storage is unavailable', async () => {
    const { wrapper, queryClient } = await mountPage(
      undefined,
      'precommit-a',
      { state: 'cancelled', phase: 'idle' },
      'unavailable',
    );

    expect(wrapper.text()).not.toContain('失败重试');
    expect(api.restartWorkspacePreCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });
});
