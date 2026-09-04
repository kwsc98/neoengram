import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount } from '@vue/test-utils';
import { createPinia, setActivePinia } from 'pinia';
import { createMemoryHistory, createRouter } from 'vue-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { QueryS3ObjectListRequest, S3AccessPointView, TenantView } from '@/api/types';
import ObjectStorageBrowserPage from '@/pages/ObjectStorageBrowserPage.vue';
import { useTenantsStore } from '@/stores/tenants';

const api = vi.hoisted(() => ({
  createS3DownloadUrl: vi.fn(),
  queryS3AccessPoint: vi.fn(),
  queryS3ObjectList: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const accessPoint: S3AccessPointView = {
  access_point_id: 'access-point-a',
  tenant_id: 'tenant-a',
  project_id: 'project-a',
  artifact_id: 'artifact-a',
  snapshot_id: 'snapshot-a',
  commit_id: 'a'.repeat(64),
  delivery_id: 'delivery-a',
  storage_volume_id: 'volume-a',
  edge_cluster_id: 'edge-a',
  bucket_name: 'bucket-a',
  endpoint: 'http://127.0.0.1:8084',
  region: 'region-a',
  state: 'active',
  policy_generation: '1',
  created_at_unix_ms: '1',
  updated_at_unix_ms: '1',
};

const ElBreadcrumbStub = { template: '<nav><slot /></nav>' };
const ElBreadcrumbItemStub = { template: '<span><slot /></span>' };
const ElButtonStub = {
  props: ['nativeType'],
  emits: ['click'],
  template: '<button :type="nativeType || \'button\'" @click="$emit(\'click\')"><slot /></button>',
};
const ElInputStub = {
  props: ['modelValue'],
  emits: ['update:modelValue'],
  template:
    '<input :value="modelValue" @input="$emit(\'update:modelValue\', $event.target.value)" />',
};
const PageHeadingStub = { template: '<header><slot name="actions" /></header>' };

async function mountPage(prefix = 'images/') {
  api.queryS3AccessPoint.mockResolvedValue({
    data: { access_point: accessPoint },
    requestId: 'request-access-point',
  });
  api.queryS3ObjectList.mockResolvedValue({
    data: { items: [] },
    requestId: 'request-object-list',
  });
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: '/tenants/:tenantId/object-storage/:accessPointId',
        name: 'object-storage-browser',
        component: ObjectStorageBrowserPage,
      },
      {
        path: '/tenants/:tenantId/object-storage',
        name: 'object-storage-list',
        component: { template: '<div />' },
      },
    ],
  });
  await router.push({
    name: 'object-storage-browser',
    params: { tenantId: 'tenant-a', accessPointId: accessPoint.access_point_id },
    query: prefix ? { prefix } : {},
  });
  await router.isReady();

  const pinia = createPinia();
  setActivePinia(pinia);
  useTenantsStore().items = [
    {
      tenant_id: 'tenant-a',
      display_name: 'Tenant A',
      resource_version: '1',
      created_at_unix_ms: '1',
      updated_at_unix_ms: '1',
      permissions: ['s3.access.read'],
    } satisfies TenantView,
  ];
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = shallowMount(ObjectStorageBrowserPage, {
    global: {
      plugins: [pinia, [VueQueryPlugin, { queryClient }], router],
      stubs: {
        ApiProblemAlert: true,
        ElAlert: true,
        ElBreadcrumb: ElBreadcrumbStub,
        ElBreadcrumbItem: ElBreadcrumbItemStub,
        ElButton: ElButtonStub,
        ElDrawer: true,
        ElEmpty: true,
        ElIcon: true,
        ElInput: ElInputStub,
        ElSkeleton: true,
        ElTable: true,
        ElTableColumn: true,
        PageCursor: true,
        PageHeading: PageHeadingStub,
        S3CredentialDialog: true,
      },
    },
  });
  await flushPromises();
  return { wrapper, queryClient, router };
}

function lastObjectListRequest(): QueryS3ObjectListRequest {
  const call = api.queryS3ObjectList.mock.calls.at(-1)?.[0] as QueryS3ObjectListRequest | undefined;
  if (!call) throw new Error('object list request is missing');
  return call;
}

afterEach(() => vi.clearAllMocks());

describe('Object storage browser root navigation', () => {
  it('returns to the empty root prefix from the Bucket breadcrumb', async () => {
    const { wrapper, queryClient, router } = await mountPage();

    const root = wrapper.get('.object-breadcrumb button');
    expect(root.text()).toBe(accessPoint.bucket_name);
    await root.trigger('click');
    await flushPromises();

    expect(router.currentRoute.value.query).toEqual({});
    expect(lastObjectListRequest().prefix).toBe('');

    wrapper.unmount();
    queryClient.clear();
  });

  it('returns to the empty root prefix after search is cleared and submitted', async () => {
    const { wrapper, queryClient, router } = await mountPage('manifests/');

    await wrapper.get('.object-search input').setValue('');
    await wrapper.get('.object-search').trigger('submit');
    await flushPromises();

    expect(router.currentRoute.value.query).toEqual({});
    expect(lastObjectListRequest().prefix).toBe('');

    wrapper.unmount();
    queryClient.clear();
  });
});
