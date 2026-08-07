import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import { ElOption, ElSelect } from 'element-plus';
import ElementPlus from 'element-plus';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ArtifactSelect from '@/components/ArtifactSelect.vue';

const api = vi.hoisted(() => ({ queryArtifactList: vi.fn() }));

vi.mock('@/api/operations', () => api);

const emptyArtifact = {
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'empty',
  display_name: 'Empty Artifact',
  initialization: { mode: 'empty' as const },
  resource_version: '1',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '1',
};
const nonEmptyArtifact = {
  ...emptyArtifact,
  artifact_id: 'non-empty',
  display_name: 'Non-empty Artifact',
  head_commit_id: 'a'.repeat(64),
};

async function mountSelector(allowNonEmpty: boolean) {
  api.queryArtifactList.mockResolvedValue({
    data: { items: [emptyArtifact, nonEmptyArtifact] },
    requestId: 'request-artifacts',
  });
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = mount(ArtifactSelect, {
    props: {
      tenantId: 'tenant-a',
      modelValue: undefined,
      allowNonEmpty,
    },
    global: { plugins: [ElementPlus, [VueQueryPlugin, { queryClient }]] },
  });
  await flushPromises();
  return { queryClient, wrapper };
}

afterEach(() => vi.clearAllMocks());

describe('ArtifactSelect', () => {
  it('disables non-empty Artifacts for the minimal catalog', async () => {
    const { queryClient, wrapper } = await mountSelector(false);
    const options = wrapper.findAllComponents(ElOption);

    expect(options).toHaveLength(2);
    expect(options[0]!.props('disabled')).toBe(false);
    expect(options[1]!.props('disabled')).toBe(true);

    wrapper.findComponent(ElSelect).vm.$emit('update:modelValue', 'project-a\u0000non-empty');
    await flushPromises();
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual([undefined]);

    wrapper.unmount();
    queryClient.clear();
  });

  it('allows non-empty Artifacts when the resource browser can materialize Commit data', async () => {
    const { queryClient, wrapper } = await mountSelector(true);
    const options = wrapper.findAllComponents(ElOption);

    expect(options[1]!.props('disabled')).toBe(false);
    wrapper.findComponent(ElSelect).vm.$emit('update:modelValue', 'project-a\u0000non-empty');
    await flushPromises();
    expect(wrapper.emitted('update:modelValue')?.at(-1)).toEqual([nonEmptyArtifact]);

    wrapper.unmount();
    queryClient.clear();
  });
});
