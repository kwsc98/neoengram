import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import { ElOption, ElSelect } from 'element-plus';
import ElementPlus from 'element-plus';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { WorkspaceView } from '@/api/types';
import WorkspaceSelect from '@/components/WorkspaceSelect.vue';

const api = vi.hoisted(() => ({ queryWorkspaceList: vi.fn() }));

vi.mock('@/api/operations', () => api);

function workspace(overrides: Partial<WorkspaceView> = {}): WorkspaceView {
  return {
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    workspace_id: 'workspace-a',
    storage_volume_id: 'volume-a',
    region: 'cn-shanghai',
    display_name: 'Workspace A',
    index_version: { revision: '7', digest: 'a'.repeat(64) },
    state: 'ready',
    storage_availability: 'ready',
    resource_version: '1',
    lifecycle: { state: 'active', generation: '1' },
    created_at_unix_ms: '1',
    updated_at_unix_ms: '1',
    ...overrides,
  };
}

afterEach(() => vi.clearAllMocks());

describe('WorkspaceSelect', () => {
  it('emits the complete WorkspaceView returned by the tenant-scoped list query', async () => {
    const option = workspace();
    api.queryWorkspaceList.mockResolvedValue({
      data: { items: [option] },
      requestId: 'request-workspaces',
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = mount(WorkspaceSelect, {
      props: { tenantId: 'tenant-a', modelValue: undefined },
      global: { plugins: [ElementPlus, [VueQueryPlugin, { queryClient }]] },
    });
    await flushPromises();

    expect(api.queryWorkspaceList).toHaveBeenCalledWith({ tenant_id: 'tenant-a', page_size: 50 });
    expect(wrapper.findAllComponents(ElOption)).toHaveLength(1);
    wrapper
      .findComponent(ElSelect)
      .vm.$emit('update:modelValue', 'project-a\u0000artifact-a\u0000workspace-a');
    await flushPromises();

    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual([option]);

    wrapper.unmount();
    queryClient.clear();
  });

  it('disables a Workspace whose lifecycle is ready but storage is unavailable', async () => {
    api.queryWorkspaceList.mockResolvedValue({
      data: { items: [workspace({ storage_availability: 'unavailable' })] },
      requestId: 'request-workspaces',
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = mount(WorkspaceSelect, {
      props: { tenantId: 'tenant-a', modelValue: undefined },
      global: { plugins: [ElementPlus, [VueQueryPlugin, { queryClient }]] },
    });
    await flushPromises();

    expect(wrapper.findComponent(ElOption).props('disabled')).toBe(true);
    expect(wrapper.findComponent(ElOption).text()).toContain('StorageVolume 当前不可达');

    wrapper.unmount();
    queryClient.clear();
  });

  it('explains an abnormal lifecycle before reporting a ready StorageVolume', async () => {
    api.queryWorkspaceList.mockResolvedValue({
      data: {
        items: [workspace({ state: 'abnormal', storage_availability: 'ready' })],
      },
      requestId: 'request-workspaces',
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = mount(WorkspaceSelect, {
      props: { tenantId: 'tenant-a', modelValue: undefined },
      global: { plugins: [ElementPlus, [VueQueryPlugin, { queryClient }]] },
    });
    await flushPromises();

    expect(wrapper.findComponent(ElOption).props('disabled')).toBe(true);
    expect(wrapper.findComponent(ElOption).text()).toContain('Workspace 生命周期异常');
    expect(wrapper.findComponent(ElOption).text()).not.toContain('存储可达');

    wrapper.unmount();
    queryClient.clear();
  });
});
