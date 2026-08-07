import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import { ElButton, ElInput, ElOption, ElSelect } from 'element-plus';
import ElementPlus from 'element-plus';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ArtifactCommitSelect from '@/components/ArtifactCommitSelect.vue';

const api = vi.hoisted(() => ({ queryArtifactCommitGraph: vi.fn() }));

vi.mock('@/api/operations', () => api);

const headCommitId = 'a'.repeat(64);
const historicalCommitId = 'b'.repeat(64);
const headCommit = {
  commit_id: headCommitId,
  parent_commit_id: historicalCommitId,
  message: 'Current head',
  tag_names: [],
  created_at_unix_ms: '2',
};
const historicalCommit = {
  commit_id: historicalCommitId,
  message: 'Historical baseline',
  tag_names: ['v1'],
  created_at_unix_ms: '1',
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function mountSelector(
  props: Partial<InstanceType<typeof ArtifactCommitSelect>['$props']> = {},
) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = mount(ArtifactCommitSelect, {
    props: {
      tenantId: 'tenant-a',
      projectId: 'project-a',
      artifactId: 'artifact-a',
      headCommitId,
      modelValue: headCommitId,
      ...props,
    },
    global: { plugins: [ElementPlus, [VueQueryPlugin, { queryClient }]] },
  });
  await flushPromises();
  return { queryClient, wrapper };
}

afterEach(() => vi.clearAllMocks());

describe('ArtifactCommitSelect', () => {
  it('selects an authoritative historical Commit and loads later graph pages', async () => {
    api.queryArtifactCommitGraph
      .mockResolvedValueOnce({
        data: {
          graph: {
            graph_version: '2',
            head_commit_id: headCommitId,
            nodes: [headCommit],
            next_cursor: 'page-2',
          },
        },
        requestId: 'request-page-1',
      })
      .mockResolvedValueOnce({
        data: {
          graph: { graph_version: '2', head_commit_id: headCommitId, nodes: [historicalCommit] },
        },
        requestId: 'request-page-2',
      });
    const { queryClient, wrapper } = await mountSelector();

    expect(api.queryArtifactCommitGraph).toHaveBeenCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
    );
    expect(wrapper.findAllComponents(ElOption)).toHaveLength(1);

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '加载更多 Commit')!
      .trigger('click');
    await flushPromises();

    expect(api.queryArtifactCommitGraph).toHaveBeenLastCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
      'page-2',
    );
    expect(wrapper.findAllComponents(ElOption)).toHaveLength(2);
    wrapper.findComponent(ElSelect).vm.$emit('update:modelValue', historicalCommitId);
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual([historicalCommitId]);

    wrapper.unmount();
    queryClient.clear();
  });

  it('ignores an in-flight load-more response after the Artifact scope changes', async () => {
    const oldPage = deferred<unknown>();
    const newPage = deferred<unknown>();
    const newHeadCommitId = 'c'.repeat(64);
    const newHistoricalCommitId = 'd'.repeat(64);
    const newHeadCommit = {
      commit_id: newHeadCommitId,
      parent_commit_id: newHistoricalCommitId,
      message: 'New Artifact head',
      tag_names: [],
      created_at_unix_ms: '4',
    };
    const newHistoricalCommit = {
      commit_id: newHistoricalCommitId,
      message: 'New Artifact baseline',
      tag_names: [],
      created_at_unix_ms: '3',
    };
    api.queryArtifactCommitGraph.mockImplementation(
      (_tenantId: string, _projectId: string, artifactId: string, cursor?: string) => {
        if (artifactId === 'artifact-a' && cursor === 'page-a-2') return oldPage.promise;
        if (artifactId === 'artifact-b' && cursor === 'page-b-2') return newPage.promise;
        if (artifactId === 'artifact-b') {
          return Promise.resolve({
            data: {
              graph: {
                graph_version: '4',
                head_commit_id: newHeadCommitId,
                nodes: [newHeadCommit],
                next_cursor: 'page-b-2',
              },
            },
            requestId: 'request-b-page-1',
          });
        }
        return Promise.resolve({
          data: {
            graph: {
              graph_version: '2',
              head_commit_id: headCommitId,
              nodes: [headCommit],
              next_cursor: 'page-a-2',
            },
          },
          requestId: 'request-a-page-1',
        });
      },
    );
    const { queryClient, wrapper } = await mountSelector();

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '加载更多 Commit')!
      .trigger('click');
    expect(api.queryArtifactCommitGraph).toHaveBeenLastCalledWith(
      'tenant-a',
      'project-a',
      'artifact-a',
      'page-a-2',
    );

    await wrapper.setProps({
      projectId: 'project-b',
      artifactId: 'artifact-b',
      headCommitId: newHeadCommitId,
      modelValue: newHeadCommitId,
    });
    await flushPromises();
    expect(
      wrapper.findAllComponents(ElOption).map((option) => String(option.props('value') as unknown)),
    ).toEqual([newHeadCommitId]);

    await wrapper
      .findAll('button')
      .find((button) => button.text() === '加载更多 Commit')!
      .trigger('click');
    expect(api.queryArtifactCommitGraph).toHaveBeenLastCalledWith(
      'tenant-a',
      'project-b',
      'artifact-b',
      'page-b-2',
    );

    oldPage.resolve({
      data: {
        graph: { graph_version: '2', head_commit_id: headCommitId, nodes: [historicalCommit] },
      },
      requestId: 'request-a-page-2',
    });
    await flushPromises();

    expect(
      wrapper.findAllComponents(ElOption).map((option) => String(option.props('value') as unknown)),
    ).toEqual([newHeadCommitId]);
    expect(
      wrapper
        .findAllComponents(ElButton)
        .find((button) => button.text() === '加载更多 Commit')!
        .props('loading'),
    ).toBe(true);

    newPage.resolve({
      data: {
        graph: {
          graph_version: '4',
          head_commit_id: newHeadCommitId,
          nodes: [newHistoricalCommit],
        },
      },
      requestId: 'request-b-page-2',
    });
    await flushPromises();
    expect(
      wrapper.findAllComponents(ElOption).map((option) => String(option.props('value') as unknown)),
    ).toEqual([newHeadCommitId, newHistoricalCommitId]);

    wrapper.unmount();
    queryClient.clear();
  });

  it('keeps the Head read-only when Commit graph capability is unavailable', async () => {
    const { queryClient, wrapper } = await mountSelector({ allowHistory: false });

    expect(api.queryArtifactCommitGraph).not.toHaveBeenCalled();
    expect(wrapper.findComponent(ElInput).props('modelValue')).toBe(headCommitId);
    expect(wrapper.findComponent(ElInput).props('readonly')).toBe(true);

    wrapper.unmount();
    queryClient.clear();
  });

  it('represents an empty Artifact without querying or inventing a Commit', async () => {
    const { queryClient, wrapper } = await mountSelector({
      headCommitId: undefined,
      modelValue: '',
    });

    expect(api.queryArtifactCommitGraph).not.toHaveBeenCalled();
    expect(wrapper.findComponent(ElInput).props('modelValue')).toBe('空 Artifact');
    expect(wrapper.emitted('update:modelValue')).toBeUndefined();

    wrapper.unmount();
    queryClient.clear();
  });
});
