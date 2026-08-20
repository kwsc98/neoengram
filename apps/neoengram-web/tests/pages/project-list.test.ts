import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import ElementPlus from 'element-plus';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ProjectListPage from '@/pages/ProjectListPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  createProject: vi.fn(),
  queryProjectList: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const project = {
  tenant_id: 'tenant-a',
  project_id: 'project-vision',
  display_name: '视觉数据',
  description: '道路场景数据',
  resource_version: '1',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '2',
};

async function mountPage() {
  api.queryProjectList.mockResolvedValue({
    data: { items: [project] },
    requestId: 'request-projects',
  });
  api.createProject.mockResolvedValue({
    data: { project: { ...project, project_id: 'project-lab' }, replayed: false },
    requestId: 'request-create',
  });
  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      permissions: ['project.read', 'project.create'],
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '2',
    },
  ];
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/tenants/:tenantId/projects', name: 'project-list', component: ProjectListPage },
      {
        path: '/tenants/:tenantId/artifacts',
        name: 'artifact-list',
        component: { template: '<div />' },
      },
    ],
  });
  await router.push('/tenants/tenant-a/projects');
  await router.isReady();
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = mount(ProjectListPage, {
    global: {
      plugins: [ElementPlus, pinia, [VueQueryPlugin, { queryClient }], router],
    },
  });
  await flushPromises();
  return { wrapper, queryClient, router };
}

afterEach(() => vi.clearAllMocks());

describe('Project catalog', () => {
  it('lists projects and exposes the create action for project.create users', async () => {
    const { wrapper, queryClient } = await mountPage();

    expect(api.queryProjectList).toHaveBeenCalledWith({ tenant_id: 'tenant-a', page_size: 50 });
    expect(wrapper.text()).toContain('视觉数据');
    expect(wrapper.findAll('button').some((button) => button.text() === '创建 Project')).toBe(true);

    wrapper.unmount();
    queryClient.clear();
  });

  it('creates a project from the dialog and reports the result', async () => {
    const { wrapper, queryClient } = await mountPage();
    await wrapper
      .findAll('button')
      .find((button) => button.text() === '创建 Project')!
      .trigger('click');
    const inputs = wrapper.findAll('input');
    await inputs[1]!.setValue('project-lab');
    await inputs[2]!.setValue('算法实验室');
    const createButtons = wrapper
      .findAll('button')
      .filter((button) => button.text() === '创建 Project');
    await createButtons[createButtons.length - 1]!.trigger('click');
    await flushPromises();

    expect(api.createProject.mock.calls[0]?.[0]).toEqual({
      tenant_id: 'tenant-a',
      project_id: 'project-lab',
      display_name: '算法实验室',
    });
    wrapper.unmount();
    queryClient.clear();
  });
});
