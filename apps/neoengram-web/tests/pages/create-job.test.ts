import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount, type VueWrapper } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { PlaygroundView } from '@/api/types';
import PlaygroundSelect from '@/components/PlaygroundSelect.vue';
import CreateJobPage from '@/pages/CreateJobPage.vue';

const api = vi.hoisted(() => ({
  createAddJob: vi.fn(),
  queryPlayground: vi.fn(),
  queryPlaygroundList: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const ElButtonStub = {
  emits: ['click'],
  template: '<button type="button" @click="$emit(\'click\')"><slot /></button>',
};
const ElFormItemStub = { template: '<div><slot /></div>' };
const ElInputStub = {
  props: ['modelValue'],
  emits: ['update:modelValue'],
  template:
    '<input :value="modelValue" @input="$emit(\'update:modelValue\', $event.target.value)" />',
};
const ElDatePickerStub = {
  props: ['modelValue'],
  emits: ['update:modelValue'],
  template:
    '<button data-testid="deadline" type="button" @click="$emit(\'update:modelValue\', new Date(modelValue.getTime() + 60000))">deadline</button>',
};

function playground(overrides: Partial<PlaygroundView> = {}): PlaygroundView {
  return {
    tenant_id: 'tenant-a',
    project_id: 'project-vision',
    artifact_id: 'road-scenes',
    playground_id: 'labeling',
    storage_volume_id: 'volume-a',
    region: 'cn-shanghai',
    display_name: 'Test playground',
    index_version: { revision: '7', digest: 'a'.repeat(64) },
    state: 'ready',
    created_at_unix_ms: '1785167000000',
    updated_at_unix_ms: '1785167600000',
    ...overrides,
  };
}

function addJobResult() {
  return {
    data: {
      job: { job_id: 'job-result' },
      replayed: false,
    },
    requestId: 'request-create-job',
  };
}

interface MountPageOptions {
  url?: string;
  queryPlaygroundImplementation?: (
    tenantId: string,
    projectId: string,
    artifactId: string,
    playgroundId: string,
  ) => Promise<unknown>;
}

async function mountPage(options: MountPageOptions = {}) {
  api.queryPlayground.mockImplementation(
    options.queryPlaygroundImplementation ??
      ((tenantId: string, projectId: string, artifactId: string, playgroundId: string) =>
        Promise.resolve({
          data: {
            playground: playground({
              tenant_id: tenantId,
              project_id: projectId,
              artifact_id: artifactId,
              playground_id: playgroundId,
            }),
          },
          requestId: 'request-query-playground',
        })),
  );
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/jobs/new',
        name: 'job-create',
        component: CreateJobPage,
      },
      {
        path: '/tenants/:tenantId/jobs/:jobId',
        name: 'job-detail',
        component: { template: '<div />' },
      },
      {
        path: '/playground',
        name: 'playground-detail',
        component: { template: '<div />' },
      },
    ],
  });
  await router.push(
    options.url ??
      '/tenants/tenant-a/jobs/new?project_id=project-vision&artifact_id=road-scenes&playground_id=labeling',
  );
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = shallowMount(CreateJobPage, {
    global: {
      plugins: [createPinia(), ElementPlus, [VueQueryPlugin, { queryClient }], router],
      stubs: {
        ElButton: ElButtonStub,
        ElDatePicker: ElDatePickerStub,
        ElFormItem: ElFormItemStub,
        ElInput: ElInputStub,
      },
    },
  });
  await flushPromises();
  return { wrapper, queryClient, router };
}

async function submit(wrapper: VueWrapper): Promise<void> {
  await wrapper.find('form').trigger('submit');
  await flushPromises();
}

function requests(): Array<Record<string, unknown>> {
  return (api.createAddJob.mock.calls as unknown[][]).map(
    ([request]) => request as Record<string, unknown>,
  );
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.clearAllMocks();
});

describe('Create Job mutation identity', () => {
  it('reuses the complete request after a transport failure and rotates only after success', async () => {
    const randomUUID = vi
      .spyOn(globalThis.crypto, 'randomUUID')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000001')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000002')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000003');
    api.createAddJob
      .mockRejectedValueOnce(new TypeError('transport interrupted'))
      .mockResolvedValueOnce(addJobResult());
    const { wrapper, queryClient, router } = await mountPage();

    await submit(wrapper);
    await submit(wrapper);

    const submitted = requests();
    expect(submitted).toHaveLength(2);
    expect(submitted[1]).toEqual(submitted[0]);
    expect(submitted[0]?.job_id).toBe('job-00000000-0000-4000-8000-000000000002');
    expect(randomUUID).toHaveBeenCalledTimes(3);
    expect(router.currentRoute.value.name).toBe('job-detail');

    wrapper.unmount();
    queryClient.clear();
  });

  it('generates a new Job identity when the mutation payload changes', async () => {
    vi.spyOn(globalThis.crypto, 'randomUUID')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000011')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000012')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000013')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000014');
    api.createAddJob
      .mockRejectedValueOnce(new TypeError('transport interrupted'))
      .mockResolvedValueOnce(addJobResult());
    const { wrapper, queryClient } = await mountPage();

    await submit(wrapper);
    await wrapper.get('[data-testid="deadline"]').trigger('click');
    await flushPromises();
    await submit(wrapper);

    const submitted = requests();
    expect(submitted).toHaveLength(2);
    expect(submitted[0]?.job_id).toBe('job-00000000-0000-4000-8000-000000000012');
    expect(submitted[1]?.job_id).toBe('job-00000000-0000-4000-8000-000000000013');
    expect(submitted[1]?.deadline_unix_ms).not.toBe(submitted[0]?.deadline_unix_ms);

    wrapper.unmount();
    queryClient.clear();
  });

  it('does not trust a query scope when the authoritative response has another identity', async () => {
    const { wrapper, queryClient } = await mountPage({
      url: '/tenants/tenant-a/jobs/new?project_id=forged-project&artifact_id=forged-artifact&playground_id=forged-playground',
      queryPlaygroundImplementation: () =>
        Promise.resolve({
          data: { playground: playground() },
          requestId: 'request-mismatched-playground',
        }),
    });

    await submit(wrapper);

    expect(api.createAddJob).not.toHaveBeenCalled();
    expect(wrapper.text()).not.toContain('forged-artifact');

    wrapper.unmount();
    queryClient.clear();
  });

  it('derives the request scope and IndexVersion from the selected authoritative Playground', async () => {
    const selected = playground({
      project_id: 'project-selected',
      artifact_id: 'artifact-selected',
      playground_id: 'playground-selected',
      index_version: { revision: '4', digest: 'b'.repeat(64) },
    });
    const refreshed = playground({
      ...selected,
      index_version: { revision: '5', digest: 'c'.repeat(64) },
    });
    api.createAddJob.mockResolvedValue(addJobResult());
    const { wrapper, queryClient } = await mountPage({
      url: '/tenants/tenant-a/jobs/new',
      queryPlaygroundImplementation: () =>
        Promise.resolve({
          data: { playground: refreshed },
          requestId: 'request-refreshed-playground',
        }),
    });

    wrapper.findComponent(PlaygroundSelect).vm.$emit('update:modelValue', selected);
    await flushPromises();
    await submit(wrapper);

    expect(requests()).toHaveLength(1);
    expect(requests()[0]).toMatchObject({
      project_id: 'project-selected',
      artifact_id: 'artifact-selected',
      playground_id: 'playground-selected',
      expected_index_version: { revision: '5', digest: 'c'.repeat(64) },
    });

    wrapper.unmount();
    queryClient.clear();
  });
});
