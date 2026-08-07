import { VueQueryPlugin, QueryClient } from '@tanstack/vue-query';
import { flushPromises, shallowMount } from '@vue/test-utils';
import { ElMessageBox } from 'element-plus';
import { createPinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PlaygroundCommitPage from '@/pages/PlaygroundCommitPage.vue';

const api = vi.hoisted(() => ({
  cancelPlaygroundPreCommit: vi.fn(),
  commitPlayground: vi.fn(),
  queryApiVersion: vi.fn(),
  queryPlayground: vi.fn(),
  queryPlaygroundChangeList: vi.fn(),
  queryPlaygroundPreCommit: vi.fn(),
  restartPlaygroundPreCommit: vi.fn(),
  startPlaygroundPreCommit: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const headCommitId = 'a'.repeat(64);
const sourceIndexVersion = { revision: '2', digest: 'sha256:index' };

function playground(
  activePreCommitId?: string,
  indexVersion: { revision: string; digest: string } = sourceIndexVersion,
) {
  return {
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    playground_id: 'playground-a',
    storage_volume_id: 'volume-a',
    region: 'region-a',
    display_name: 'Playground A',
    head_commit_id: headCommitId,
    index_version: indexVersion,
    state: 'ready' as const,
    ...(activePreCommitId ? { active_precommit_id: activePreCommitId } : {}),
    created_at_unix_ms: '1',
    updated_at_unix_ms: '2',
  };
}

async function mountPage(
  activePreCommitId?: string,
  routedPreCommitId?: string,
  precommitOverrides: Record<string, unknown> = {},
) {
  api.queryApiVersion.mockResolvedValue({
    data: {
      api_versions: [1],
      agent_protocol_versions: [1],
      capabilities: ['resource_browser'],
    },
    requestId: 'request-version',
  });
  api.queryPlayground.mockResolvedValue({
    data: { playground: playground(activePreCommitId) },
    requestId: 'request-playground',
  });
  api.queryPlaygroundPreCommit.mockResolvedValue({
    data: {
      precommit: {
        tenant_id: 'tenant-a',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        playground_id: 'playground-a',
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
        created_at_unix_ms: '1',
        updated_at_unix_ms: '2',
        ...precommitOverrides,
      },
    },
    requestId: 'request-precommit',
  });
  api.queryPlaygroundChangeList.mockResolvedValue({
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
        path: '/tenants/:tenantId/projects/:projectId/artifacts/:artifactId/playgrounds/:playgroundId/commit',
        component: PlaygroundCommitPage,
      },
    ],
  });
  await router.push(
    `/tenants/tenant-a/projects/project-a/artifacts/artifact-a/playgrounds/playground-a/commit${routedPreCommitId ? `?precommit_id=${routedPreCommitId}` : ''}`,
  );
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = shallowMount(PlaygroundCommitPage, {
    global: {
      plugins: [createPinia(), [VueQueryPlugin, { queryClient }], router],
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

describe('Playground Commit page recovery', () => {
  it('does not start a Pre-commit when the page is opened without an active id', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryPlayground).toHaveBeenCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
      'playground-a',
    );
    expect(api.startPlaygroundPreCommit).not.toHaveBeenCalled();
    expect(api.queryPlaygroundPreCommit).not.toHaveBeenCalled();

    wrapper.unmount();
    queryClient.clear();
  });

  it('does not query frozen changes while a running Pre-commit has no candidate', async () => {
    const { wrapper, queryClient } = await mountPage('precommit-a');

    expect(api.queryPlaygroundPreCommit).toHaveBeenCalledWith('tenant-a', 'precommit-a');
    expect(api.queryPlaygroundChangeList).not.toHaveBeenCalled();
    expect(api.startPlaygroundPreCommit).not.toHaveBeenCalled();

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

    expect(api.queryPlaygroundChangeList).toHaveBeenCalledTimes(1);
    expect(api.queryPlaygroundChangeList).toHaveBeenLastCalledWith(
      expect.objectContaining({
        tenant_id: 'tenant-a',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        playground_id: 'playground-a',
        precommit_id: 'precommit-a',
      }),
    );

    api.queryPlaygroundPreCommit.mockResolvedValueOnce({
      data: {
        precommit: {
          tenant_id: 'tenant-a',
          project_id: 'project-a',
          artifact_id: 'artifact-a',
          playground_id: 'playground-a',
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

    expect(api.queryPlaygroundChangeList).toHaveBeenCalledTimes(2);
    const changeQueryKeys = queryClient
      .getQueryCache()
      .getAll()
      .filter(
        (query) => query.queryKey[0] === 'playground-changes' && query.state.data !== undefined,
      )
      .map((query) => query.queryKey);
    expect(changeQueryKeys).toHaveLength(2);
    expect(new Set(changeQueryKeys.map((queryKey) => JSON.stringify(queryKey))).size).toBe(2);

    wrapper.unmount();
    queryClient.clear();
  });

  it('refreshes the Playground after cancel and starts redetection from the newer revision', async () => {
    const nextIndexVersion = { revision: '3', digest: 'sha256:index-3' };
    const { wrapper, queryClient } = await mountPage('precommit-a', undefined, {
      state: 'ready',
      phase: 'idle',
      candidate_index_version: { revision: '3', digest: 'sha256:candidate-3' },
    });
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue(undefined as never);
    api.cancelPlaygroundPreCommit.mockResolvedValue({
      data: {
        precommit: { precommit_id: 'precommit-a', state: 'cancelled' },
        playground: playground(undefined, nextIndexVersion),
        replayed: false,
      },
      requestId: 'request-cancel',
    });
    api.queryPlayground.mockResolvedValue({
      data: { playground: playground(undefined, nextIndexVersion) },
      requestId: 'request-playground-refresh',
    });
    api.startPlaygroundPreCommit.mockResolvedValue({
      data: {
        precommit: { precommit_id: 'precommit-b', state: 'running' },
        playground: playground('precommit-b', nextIndexVersion),
        replayed: false,
      },
      requestId: 'request-start',
    });

    const redetectButton = wrapper
      .findAll('el-button, el-button-stub')
      .find((button) => button.text().trim() === '重新检测');
    expect(redetectButton).toBeDefined();
    await redetectButton!.trigger('click');
    await flushPromises();

    expect(api.cancelPlaygroundPreCommit).toHaveBeenCalledTimes(1);
    expect(api.startPlaygroundPreCommit).toHaveBeenCalledWith(
      expect.objectContaining({ expected_index_version: nextIndexVersion }),
    );
    const cancelOrder = api.cancelPlaygroundPreCommit.mock.invocationCallOrder[0]!;
    const refreshedPlaygroundOrder = api.queryPlayground.mock.invocationCallOrder.find(
      (order) => order > cancelOrder,
    );
    const startOrder = api.startPlaygroundPreCommit.mock.invocationCallOrder[0]!;
    expect(refreshedPlaygroundOrder).toBeDefined();
    expect(cancelOrder).toBeLessThan(refreshedPlaygroundOrder!);
    expect(refreshedPlaygroundOrder!).toBeLessThan(startOrder);

    wrapper.unmount();
    queryClient.clear();
  });

  it('recovers a route-pinned session without starting a new one', async () => {
    const { wrapper, queryClient } = await mountPage(undefined, 'precommit-a', {
      state: 'cancelled',
      phase: 'idle',
    });

    expect(api.queryPlaygroundPreCommit).toHaveBeenCalledWith('tenant-a', 'precommit-a');
    expect(api.startPlaygroundPreCommit).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain('失败重试');
    expect(wrapper.text()).not.toContain('没有活动 Pre-commit');

    wrapper.unmount();
    queryClient.clear();
  });

  it('rejects a routed Pre-commit that belongs to another Playground scope', async () => {
    const { wrapper, queryClient } = await mountPage(undefined, 'precommit-a', {
      playground_id: 'playground-b',
    });

    expect(api.queryPlaygroundChangeList).not.toHaveBeenCalled();
    const scopeAlert = wrapper
      .findAllComponents(ApiProblemAlert)
      .find((alert) => (alert.props('error') as Error | undefined)?.message.includes('不属于'));
    expect(scopeAlert).toBeDefined();

    wrapper.unmount();
    queryClient.clear();
  });
});
