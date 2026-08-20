import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, mount } from '@vue/test-utils';
import { ElOption } from 'element-plus';
import ElementPlus from 'element-plus';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { StorageVolumeView } from '@/api/types';
import StorageVolumeFilter from '@/components/StorageVolumeFilter.vue';

const api = vi.hoisted(() => ({ queryStorageVolumeList: vi.fn() }));

vi.mock('@/api/operations', () => api);

function storageVolume(overrides: Partial<StorageVolumeView> = {}): StorageVolumeView {
  return {
    tenant_id: 'tenant-a',
    storage_volume_id: 'desktop-pvc',
    display_name: 'Desktop PVC',
    edge_cluster_id: 'desktop-cluster',
    region: 'local',
    backend_type: 'pvc',
    access_mode: 'read_write_once',
    allowed_delivery_modes: ['fuse', 'copy'],
    hardlink_policy: 'disabled',
    max_whole_file_bytes: '18446744073709551615',
    copy_reserve_bytes: '0',
    pvc_reference: { namespace: 'default', claim_name: 'desktop-pvc' },
    state: 'ready',
    resource_version: '1',
    lifecycle: { state: 'active', generation: '1' },
    created_at_unix_ms: '1',
    updated_at_unix_ms: '1',
    ...overrides,
  };
}

afterEach(() => vi.clearAllMocks());

describe('StorageVolumeFilter', () => {
  it.each([
    ['degraded', '存储降级，暂不可用于新放置'],
    ['unavailable', '存储不可达，暂不可用于新放置'],
  ] as const)('disables and explains a %s Volume', async (state, reason) => {
    api.queryStorageVolumeList.mockResolvedValue({
      data: { items: [storageVolume({ state })] },
      requestId: 'request-volumes',
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = mount(StorageVolumeFilter, {
      props: { tenantId: 'tenant-a', modelValue: '' },
      global: { plugins: [ElementPlus, [VueQueryPlugin, { queryClient }]] },
    });
    await flushPromises();

    const option = wrapper.findComponent(ElOption);
    expect(option.props('disabled')).toBe(true);
    expect(option.text()).toContain(reason);

    wrapper.unmount();
    queryClient.clear();
  });

  it('keeps a ready Volume selectable', async () => {
    api.queryStorageVolumeList.mockResolvedValue({
      data: { items: [storageVolume()] },
      requestId: 'request-volumes',
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = mount(StorageVolumeFilter, {
      props: { tenantId: 'tenant-a', modelValue: '' },
      global: { plugins: [ElementPlus, [VueQueryPlugin, { queryClient }]] },
    });
    await flushPromises();

    expect(wrapper.findComponent(ElOption).props('disabled')).toBe(false);
    expect(wrapper.text()).not.toContain('暂不可用于新放置');

    wrapper.unmount();
    queryClient.clear();
  });
});
