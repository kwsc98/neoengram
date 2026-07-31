import { VueQueryPlugin, QueryClient } from '@tanstack/vue-query';
import { flushPromises, shallowMount } from '@vue/test-utils';
import { createPinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PlaygroundCommitPage from '@/pages/PlaygroundCommitPage.vue';

const api = vi.hoisted(() => ({
  cancelPlaygroundPreCommit: vi.fn(),
  commitPlayground: vi.fn(),
  queryPlayground: vi.fn(),
  queryPlaygroundChangeList: vi.fn(),
  queryPlaygroundPreCommit: vi.fn(),
  restartPlaygroundPreCommit: vi.fn(),
  startPlaygroundPreCommit: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

function playground(activePreCommitId?: string) {
  return {
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    playground_id: 'playground-a',
    storage_volume_id: 'volume-a',
    region: 'region-a',
    display_name: 'Playground A',
    head_commit_id: 'commit-a',
    index_version: { revision: '2', digest: 'sha256:index' },
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
        source_index_version: { revision: '2', digest: 'sha256:index' },
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

  it('recovers the active server session and queries its frozen changes', async () => {
    const { wrapper, queryClient } = await mountPage('precommit-a');

    expect(api.queryPlaygroundPreCommit).toHaveBeenCalledWith('tenant-a', 'precommit-a');
    expect(api.queryPlaygroundChangeList).toHaveBeenCalledWith(
      expect.objectContaining({
        tenant_id: 'tenant-a',
        project_id: 'project-a',
        artifact_id: 'artifact-a',
        playground_id: 'playground-a',
        precommit_id: 'precommit-a',
      }),
    );
    expect(api.startPlaygroundPreCommit).not.toHaveBeenCalled();

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
