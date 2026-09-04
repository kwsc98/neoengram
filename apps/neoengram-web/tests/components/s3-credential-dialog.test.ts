import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query';
import { flushPromises, shallowMount } from '@vue/test-utils';
import { createPinia } from 'pinia';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { CreateS3CredentialRequest, S3AccessPointView, S3CredentialView } from '@/api/types';
import S3CredentialDialog from '@/components/S3CredentialDialog.vue';

const api = vi.hoisted(() => ({
  createS3Credential: vi.fn(),
  queryS3CredentialList: vi.fn(),
  revokeS3Credential: vi.fn(),
}));

vi.mock('@/api/operations', () => api);

const ElDialogStub = {
  props: ['modelValue'],
  emits: ['update:modelValue'],
  template: '<section v-if="modelValue"><slot /><slot name="footer" /></section>',
};
const ElAlertStub = {
  props: ['title', 'description'],
  template:
    '<section><slot name="title"><strong>{{ title }}</strong></slot><span>{{ description }}</span><slot /></section>',
};
const ElButtonStub = {
  emits: ['click'],
  template: '<button type="button" @click="$emit(\'click\')"><slot /></button>',
};
const ApiProblemAlertStub = {
  props: ['error'],
  template: '<div data-testid="api-error">{{ error.message }}</div>',
};

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

const existingCredential: S3CredentialView = {
  credential_id: 'credential-a',
  access_point_id: accessPoint.access_point_id,
  access_key_id: 'NGS3EXISTING',
  state: 'active',
  expires_at_unix_ms: '1790000000000',
  created_at_unix_ms: '1',
};

async function mountDialog(
  props: Partial<InstanceType<typeof S3CredentialDialog>['$props']> = {},
  queryError?: Error,
) {
  if (queryError) api.queryS3CredentialList.mockRejectedValue(queryError);
  else {
    api.queryS3CredentialList.mockResolvedValue({
      data: { items: [] },
      requestId: 'request-credential-list',
    });
  }
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  const wrapper = shallowMount(S3CredentialDialog, {
    props: {
      modelValue: true,
      accessPoint,
      ...props,
    },
    global: {
      plugins: [createPinia(), [VueQueryPlugin, { queryClient }]],
      stubs: {
        ApiProblemAlert: ApiProblemAlertStub,
        ElAlert: ElAlertStub,
        ElButton: ElButtonStub,
        ElDialog: ElDialogStub,
        ElEmpty: true,
        ElSkeleton: true,
        ElTable: true,
        ElTableColumn: true,
        ElTag: true,
      },
    },
  });
  await flushPromises();
  return { wrapper, queryClient };
}

function rotateButton(wrapper: Awaited<ReturnType<typeof mountDialog>>['wrapper']) {
  const button = wrapper.findAll('button').find((candidate) => candidate.text() === '轮换凭证');
  if (!button) throw new Error('rotate credential button is missing');
  return button;
}

afterEach(() => vi.clearAllMocks());

describe('S3 credential dialog', () => {
  it('clears a one-time Secret when the dialog closes', async () => {
    const { wrapper, queryClient } = await mountDialog({
      initialCredential: {
        accessKeyId: 'NGS3NEW',
        secretAccessKey: 'one-time-secret',
        expiresAtUnixMs: '1790000000000',
      },
    });

    expect(wrapper.text()).toContain('Secret 只显示这一次');
    expect(wrapper.text()).toContain('one-time-secret');

    await wrapper.setProps({ modelValue: false });
    await wrapper.setProps({ modelValue: true, initialCredential: undefined });
    await flushPromises();

    expect(wrapper.text()).not.toContain('one-time-secret');

    wrapper.unmount();
    queryClient.clear();
  });

  it('reuses request identity after failure and explains a replay without Secret', async () => {
    api.createS3Credential.mockRejectedValueOnce(new Error('response lost')).mockResolvedValueOnce({
      data: { credential: existingCredential, replayed: true },
      requestId: 'request-credential-replay',
    });
    const { wrapper, queryClient } = await mountDialog();

    await rotateButton(wrapper).trigger('click');
    await flushPromises();
    await rotateButton(wrapper).trigger('click');
    await flushPromises();

    expect(api.createS3Credential).toHaveBeenCalledTimes(2);
    const firstRequest = api.createS3Credential.mock.calls[0]?.[0] as CreateS3CredentialRequest;
    const replayRequest = api.createS3Credential.mock.calls[1]?.[0] as CreateS3CredentialRequest;
    expect(replayRequest.request_id).toBe(firstRequest.request_id);
    expect(wrapper.text()).toContain('Secret 无法再次显示');
    expect(wrapper.text()).toContain('该请求已被幂等处理');

    wrapper.unmount();
    queryClient.clear();
  });

  it('keeps credential query errors distinct from an empty credential list', async () => {
    const { wrapper, queryClient } = await mountDialog({}, new Error('credential list failed'));

    expect(wrapper.get('[data-testid="api-error"]').text()).toBe('credential list failed');
    expect(wrapper.text()).not.toContain('暂无凭证');

    wrapper.unmount();
    queryClient.clear();
  });
});
