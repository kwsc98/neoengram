import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount, type VueWrapper } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import CreateJobPage from '@/pages/CreateJobPage.vue';

const api = vi.hoisted(() => ({ createAddJob: vi.fn() }));

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

function addJobResult() {
  return {
    data: {
      job: { job_id: 'job-result' },
      replayed: false,
    },
    requestId: 'request-create-job',
  };
}

async function mountPage() {
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
  await router.push('/tenants/tenant-a/jobs/new');
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = shallowMount(CreateJobPage, {
    global: {
      plugins: [createPinia(), ElementPlus, [VueQueryPlugin, { queryClient }], router],
      stubs: {
        ElButton: ElButtonStub,
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
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000002');
    api.createAddJob
      .mockRejectedValueOnce(new TypeError('transport interrupted'))
      .mockResolvedValueOnce(addJobResult());
    const { wrapper, queryClient, router } = await mountPage();

    await submit(wrapper);
    await submit(wrapper);

    const submitted = requests();
    expect(submitted).toHaveLength(2);
    expect(submitted[1]).toEqual(submitted[0]);
    expect(submitted[0]?.job_id).toBe('job-00000000-0000-4000-8000-000000000001');
    expect(randomUUID).toHaveBeenCalledTimes(2);
    expect(router.currentRoute.value.name).toBe('job-detail');

    wrapper.unmount();
    queryClient.clear();
  });

  it('generates a new Job identity when the mutation payload changes', async () => {
    vi.spyOn(globalThis.crypto, 'randomUUID')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000011')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000012')
      .mockReturnValueOnce('00000000-0000-4000-8000-000000000013');
    api.createAddJob
      .mockRejectedValueOnce(new TypeError('transport interrupted'))
      .mockResolvedValueOnce(addJobResult());
    const { wrapper, queryClient } = await mountPage();

    await submit(wrapper);
    await wrapper.findAll('input')[0]?.setValue('project-language');
    await flushPromises();
    await submit(wrapper);

    const submitted = requests();
    expect(submitted).toHaveLength(2);
    expect(submitted[0]?.job_id).toBe('job-00000000-0000-4000-8000-000000000011');
    expect(submitted[1]?.job_id).toBe('job-00000000-0000-4000-8000-000000000012');
    expect(submitted[1]?.project_id).toBe('project-language');

    wrapper.unmount();
    queryClient.clear();
  });
});
