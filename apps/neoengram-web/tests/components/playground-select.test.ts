import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import { ElOption, ElSelect } from 'element-plus';
import ElementPlus from 'element-plus';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { PlaygroundView } from '@/api/types';
import PlaygroundSelect from '@/components/PlaygroundSelect.vue';

const api = vi.hoisted(() => ({ queryPlaygroundList: vi.fn() }));

vi.mock('@/api/operations', () => api);

function playground(overrides: Partial<PlaygroundView> = {}): PlaygroundView {
  return {
    tenant_id: 'tenant-a',
    project_id: 'project-a',
    artifact_id: 'artifact-a',
    playground_id: 'playground-a',
    storage_volume_id: 'volume-a',
    region: 'cn-shanghai',
    display_name: 'Playground A',
    index_version: { revision: '7', digest: 'a'.repeat(64) },
    state: 'ready',
    created_at_unix_ms: '1',
    updated_at_unix_ms: '1',
    ...overrides,
  };
}

afterEach(() => vi.clearAllMocks());

describe('PlaygroundSelect', () => {
  it('emits the complete PlaygroundView returned by the tenant-scoped list query', async () => {
    const option = playground();
    api.queryPlaygroundList.mockResolvedValue({
      data: { items: [option] },
      requestId: 'request-playgrounds',
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = mount(PlaygroundSelect, {
      props: { tenantId: 'tenant-a', modelValue: undefined },
      global: { plugins: [ElementPlus, [VueQueryPlugin, { queryClient }]] },
    });
    await flushPromises();

    expect(api.queryPlaygroundList).toHaveBeenCalledWith({ tenant_id: 'tenant-a', page_size: 50 });
    expect(wrapper.findAllComponents(ElOption)).toHaveLength(1);
    wrapper
      .findComponent(ElSelect)
      .vm.$emit('update:modelValue', 'project-a\u0000artifact-a\u0000playground-a');
    await flushPromises();

    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual([option]);

    wrapper.unmount();
    queryClient.clear();
  });
});
