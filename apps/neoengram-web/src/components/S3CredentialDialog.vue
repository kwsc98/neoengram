<script setup lang="ts">
import { CopyDocument, Key, Refresh, WarningFilled } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, ref, watch } from 'vue';

import { createS3Credential, queryS3CredentialList, revokeS3Credential } from '@/api/operations';
import type {
  CreateS3CredentialResponse,
  RevokeS3CredentialRequest,
  S3AccessPointView,
  S3CredentialView,
} from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import { formatTime } from '@/utils/format';

export interface S3InitialCredential {
  accessKeyId: string;
  secretAccessKey: string;
  expiresAtUnixMs: string;
}

const props = defineProps<{
  modelValue: boolean;
  accessPoint: S3AccessPointView | undefined;
  initialCredential?: S3InitialCredential;
  initialSecretUnavailable?: boolean;
}>();
const emit = defineEmits<{ 'update:modelValue': [value: boolean] }>();

const queryClient = useQueryClient();
const secret = ref<S3InitialCredential>();
const secretUnavailable = ref(false);
const createError = ref<unknown>();
const revokeError = ref<unknown>();
const creating = ref(false);
const credentials = ref<S3CredentialView[]>([]);
const requestId = ref('');
const pendingRevokeRequest = ref<RevokeS3CredentialRequest>();

const accessPointId = computed(() => props.accessPoint?.access_point_id ?? '');
const tenantId = computed(() => props.accessPoint?.tenant_id ?? '');
const credentialQuery = useQuery({
  queryKey: computed(() => ['s3-credentials', tenantId.value, accessPointId.value]),
  queryFn: async () => {
    const result = await queryS3CredentialList({
      tenant_id: tenantId.value,
      access_point_id: accessPointId.value,
    });
    credentials.value = result.data.items;
    return result;
  },
  enabled: computed(() => props.modelValue && Boolean(tenantId.value && accessPointId.value)),
});
const revokeMutation = useMutation({
  mutationFn: revokeS3Credential,
  onSuccess: (result) => {
    credentials.value = result.data.items;
    void queryClient.invalidateQueries({
      queryKey: ['s3-credentials', tenantId.value, accessPointId.value],
    });
    ElMessage.success('凭证已撤销');
  },
});

watch(
  () => [props.modelValue, props.initialCredential, props.initialSecretUnavailable] as const,
  ([open, initial]) => {
    if (!open) {
      secret.value = undefined;
      secretUnavailable.value = false;
      requestId.value = '';
      pendingRevokeRequest.value = undefined;
      return;
    }
    createError.value = undefined;
    revokeError.value = undefined;
    secret.value = initial;
    secretUnavailable.value = props.initialSecretUnavailable ?? false;
    credentials.value = [];
    requestId.value = '';
    pendingRevokeRequest.value = undefined;
  },
  { immediate: true },
);

function close(): void {
  emit('update:modelValue', false);
}

function newRequestId(prefix: string): string {
  return `${prefix}-${globalThis.crypto.randomUUID()}`;
}

async function copy(value: string, label: string): Promise<void> {
  try {
    await globalThis.navigator.clipboard.writeText(value);
    ElMessage.success(`${label}已复制`);
  } catch {
    ElMessage.error('复制失败，请手动选择文本');
  }
}

async function rotate(): Promise<void> {
  if (!props.accessPoint || creating.value) return;
  if (credentials.value.filter((item) => item.state === 'active').length >= 2) {
    ElMessage.warning('最多同时保留两个 active 凭证，请先撤销旧凭证');
    return;
  }
  creating.value = true;
  createError.value = undefined;
  if (!requestId.value) requestId.value = newRequestId('s3-credential');
  try {
    const result = await createS3Credential({
      tenant_id: props.accessPoint.tenant_id,
      access_point_id: props.accessPoint.access_point_id,
      request_id: requestId.value,
    });
    if (result.data.secret_access_key) showSecret(result.data);
    else {
      secret.value = undefined;
      secretUnavailable.value = true;
    }
    await credentialQuery.refetch();
    requestId.value = '';
    if (result.data.secret_access_key) ElMessage.success('新凭证已创建');
    else ElMessage.warning('凭证请求已处理，但幂等重放不会再次返回 Secret');
  } catch (error) {
    createError.value = error;
  } finally {
    creating.value = false;
  }
}

function showSecret(result: CreateS3CredentialResponse): void {
  if (!result.secret_access_key) return;
  secretUnavailable.value = false;
  secret.value = {
    accessKeyId: result.credential.access_key_id,
    secretAccessKey: result.secret_access_key,
    expiresAtUnixMs: result.credential.expires_at_unix_ms,
  };
}

async function revoke(credential: S3CredentialView): Promise<void> {
  if (!props.accessPoint || revokeMutation.isPending.value) return;
  try {
    await ElMessageBox.confirm(
      `撤销 ${credential.access_key_id} 后，使用它的 S3 客户端将立即无法读取。`,
      '确认撤销凭证',
      { type: 'warning', confirmButtonText: '撤销', cancelButtonText: '取消' },
    );
  } catch {
    return;
  }
  const request: RevokeS3CredentialRequest = {
    tenant_id: props.accessPoint.tenant_id,
    access_point_id: props.accessPoint.access_point_id,
    credential_id: credential.credential_id,
    request_id: newRequestId('s3-revoke'),
  };
  pendingRevokeRequest.value = request;
  await executeRevoke(request);
}

async function executeRevoke(request: RevokeS3CredentialRequest): Promise<void> {
  if (revokeMutation.isPending.value) return;
  revokeError.value = undefined;
  try {
    await revokeMutation.mutateAsync(request);
    pendingRevokeRequest.value = undefined;
  } catch (error) {
    revokeError.value = error;
  }
}

async function retryRevoke(): Promise<void> {
  if (pendingRevokeRequest.value) await executeRevoke(pendingRevokeRequest.value);
}

function credentialTagType(state: S3CredentialView['state']): 'success' | 'danger' | 'info' {
  if (state === 'active') return 'success';
  if (state === 'revoked') return 'danger';
  return 'info';
}
</script>

<template>
  <el-dialog
    :model-value="modelValue"
    title="管理 S3 凭证"
    width="min(720px, calc(100vw - 28px))"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <template v-if="accessPoint">
      <div class="s3-dialog-context">
        <div>
          <span>Bucket</span>
          <code>{{ accessPoint.bucket_name }}</code>
        </div>
        <div>
          <span>Endpoint</span>
          <code>{{ accessPoint.endpoint }}</code>
        </div>
      </div>
      <el-alert v-if="secret" type="warning" :closable="false" class="s3-secret-alert">
        <template #title>Secret 只显示这一次，请立即复制并存放在安全位置。</template>
        <div class="s3-secret-grid">
          <label>
            <span>Access Key ID</span>
            <code>{{ secret.accessKeyId }}</code>
            <el-button
              text
              :icon="CopyDocument"
              title="复制 Access Key ID"
              @click="copy(secret!.accessKeyId, 'Access Key ID')"
            />
          </label>
          <label>
            <span>Secret Access Key</span>
            <code>{{ secret.secretAccessKey }}</code>
            <el-button
              text
              :icon="CopyDocument"
              title="复制 Secret Access Key"
              @click="copy(secret!.secretAccessKey, 'Secret Access Key')"
            />
          </label>
        </div>
        <small>有效期至 {{ formatTime(secret.expiresAtUnixMs) }}</small>
      </el-alert>
      <el-alert
        v-else-if="secretUnavailable"
        type="warning"
        :closable="false"
        class="s3-secret-alert"
        title="Secret 无法再次显示"
        description="该请求已被幂等处理。请撤销无法取回 Secret 的凭证，再创建一组新凭证。"
      />
      <ApiProblemAlert
        v-if="createError"
        :error="createError"
        :retrying="creating"
        @retry="rotate"
      />
      <ApiProblemAlert
        v-if="revokeError"
        :error="revokeError"
        :retrying="revokeMutation.isPending.value"
        @retry="retryRevoke"
      />
      <el-alert
        v-if="accessPoint.state === 'disabled'"
        type="warning"
        :closable="false"
        title="Access Point 已停用，所有凭证均已失效。"
      />
      <div class="s3-credential-toolbar">
        <span class="s3-dialog-label"><Key /> Access Key</span>
        <el-button
          type="primary"
          plain
          :icon="Refresh"
          :loading="creating"
          :disabled="accessPoint.state !== 'active'"
          @click="rotate"
        >
          轮换凭证
        </el-button>
      </div>
      <ApiProblemAlert
        v-if="credentialQuery.error.value"
        :error="credentialQuery.error.value"
        :retrying="credentialQuery.isFetching.value"
        @retry="credentialQuery.refetch"
      />
      <el-skeleton v-else-if="credentialQuery.isPending.value" :rows="3" animated />
      <template v-else>
        <el-table :data="credentials" size="small" class="s3-credential-table">
          <el-table-column label="Access Key ID" min-width="180">
            <template #default="scope"
              ><code>{{ scope.row.access_key_id }}</code></template
            >
          </el-table-column>
          <el-table-column label="状态" width="100">
            <template #default="scope">
              <el-tag :type="credentialTagType(scope.row.state)" effect="plain">{{
                scope.row.state
              }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="有效期" min-width="160">
            <template #default="scope">{{ formatTime(scope.row.expires_at_unix_ms) }}</template>
          </el-table-column>
          <el-table-column label="操作" width="90" align="right">
            <template #default="scope">
              <el-button
                v-if="scope.row.state === 'active'"
                text
                type="danger"
                :icon="WarningFilled"
                title="撤销凭证"
                @click="revoke(scope.row)"
              />
            </template>
          </el-table-column>
        </el-table>
        <el-empty v-if="credentials.length === 0" description="暂无凭证" :image-size="64" />
      </template>
    </template>
    <el-empty v-else description="Access Point 不存在" :image-size="64" />
    <template #footer><el-button @click="close">关闭</el-button></template>
  </el-dialog>
</template>

<style scoped>
.s3-dialog-context {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 14px;
}
.s3-dialog-context div {
  min-width: 0;
  padding: 10px 12px;
  background: var(--surface-soft);
}
.s3-dialog-context span,
.s3-secret-grid span {
  display: block;
  margin-bottom: 5px;
  color: var(--muted);
  font-size: 11px;
}
.s3-dialog-context code,
.s3-secret-grid code {
  display: block;
  overflow-wrap: anywhere;
}
.s3-secret-alert {
  margin-bottom: 14px;
}
.s3-secret-grid {
  display: grid;
  gap: 9px;
  margin: 10px 0 6px;
}
.s3-secret-grid label {
  position: relative;
  min-width: 0;
  padding-right: 34px;
}
.s3-secret-grid .el-button {
  position: absolute;
  top: 16px;
  right: 0;
}
.s3-credential-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 9px;
}
.s3-dialog-label {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  color: var(--muted);
  font-size: 12px;
}
.s3-dialog-label svg {
  width: 15px;
}
.s3-credential-table code {
  font-size: 12px;
}
@media (max-width: 600px) {
  .s3-dialog-context {
    grid-template-columns: 1fr;
  }
}
</style>
