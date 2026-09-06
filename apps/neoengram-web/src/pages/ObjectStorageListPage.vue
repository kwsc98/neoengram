<script setup lang="ts">
import {
  ArrowRight,
  Check,
  CopyDocument,
  Delete,
  FolderOpened,
  Key,
  Plus,
  Refresh,
  WarningFilled,
} from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage, ElMessageBox } from 'element-plus';
import { computed, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  createS3AccessPoint,
  deleteS3AccessPoint,
  disableS3AccessPoint,
  enableS3AccessPoint,
  queryS3AccessPointList,
  querySnapshotDeliveryList,
  querySnapshotList,
} from '@/api/operations';
import { isApiProblem } from '@/api/problem';
import type { S3AccessPointView, SnapshotView, UpdateS3AccessPointRequest } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import S3CredentialDialog from '@/components/S3CredentialDialog.vue';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

interface InitialCredential {
  accessKeyId: string;
  secretAccessKey: string;
  expiresAtUnixMs: string;
}

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const permissions = computed(() => tenants.byId(tenantId.value)?.permissions ?? []);
const canManage = computed(() =>
  (permissions.value as readonly string[]).includes('s3.access.manage'),
);

const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);
const createOpen = ref(false);
const createError = ref<unknown>();
const creating = ref(false);
const createRequestId = ref('');
const createForm = reactive({ snapshotId: '', bucketName: '' });
const credentialsOpen = ref(false);
const credentialAccessPoint = ref<S3AccessPointView>();
const initialCredential = ref<InitialCredential>();
const initialSecretUnavailable = ref(false);
const pendingAction = ref<string>();
const handledSnapshotContext = ref('');

const accessPointsQuery = useQuery({
  queryKey: computed(() => ['s3-access-points', tenantId.value, cursor.value ?? '']),
  queryFn: () =>
    queryS3AccessPointList({
      tenant_id: tenantId.value,
      page_size: 50,
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
});
const snapshotsQuery = useQuery({
  queryKey: computed(() => ['ready-snapshots', tenantId.value]),
  queryFn: async () => {
    const result = await querySnapshotList({ tenant_id: tenantId.value, page_size: 100 });
    const candidates = result.data.items.filter((snapshot) => snapshot.state === 'ready');
    const delivered = await Promise.all(
      candidates.map(async (snapshot) => {
        try {
          const deliveries = await querySnapshotDeliveryList({
            tenant_id: tenantId.value,
            snapshot_id: snapshot.snapshot_id,
            page_size: 100,
          });
          return deliveries.data.items.some(
            (delivery) =>
              delivery.delivery_id === snapshot.delivery_id && delivery.state === 'ready',
          )
            ? snapshot
            : undefined;
        } catch {
          return undefined;
        }
      }),
    );
    return {
      ...result,
      data: {
        ...result.data,
        items: delivered.filter((snapshot): snapshot is SnapshotView => Boolean(snapshot)),
      },
    };
  },
  enabled: computed(() => createOpen.value),
});
const createMutation = useMutation({ mutationFn: createS3AccessPoint });
const enableMutation = useMutation({ mutationFn: enableS3AccessPoint });
const disableMutation = useMutation({ mutationFn: disableS3AccessPoint });
const deleteMutation = useMutation({ mutationFn: deleteS3AccessPoint });
const accessPoints = computed(() => accessPointsQuery.data.value?.data.items ?? []);
const readySnapshots = computed(() => snapshotsQuery.data.value?.data.items ?? []);

watch(tenantId, () => {
  cursor.value = undefined;
  cursorHistory.value = [];
  createOpen.value = false;
  credentialsOpen.value = false;
  credentialAccessPoint.value = undefined;
  initialCredential.value = undefined;
  initialSecretUnavailable.value = false;
  handledSnapshotContext.value = '';
});

watch(
  [
    () => String(route.query.snapshotId ?? ''),
    accessPoints,
    () => accessPointsQuery.isSuccess.value,
  ],
  async ([requestedSnapshotId, currentAccessPoints, loaded]) => {
    if (!loaded || !requestedSnapshotId || handledSnapshotContext.value === requestedSnapshotId) {
      return;
    }
    handledSnapshotContext.value = requestedSnapshotId;
    const existing = currentAccessPoints.find(
      (accessPoint) => accessPoint.snapshot_id === requestedSnapshotId,
    );
    if (existing) {
      await openBrowser(existing);
    } else if (canManage.value) {
      openCreate(requestedSnapshotId);
    }
  },
  { immediate: true },
);

function openCreate(snapshotId = ''): void {
  Object.assign(createForm, { snapshotId, bucketName: '' });
  createError.value = undefined;
  createRequestId.value = `s3-access-point-${globalThis.crypto.randomUUID()}`;
  createMutation.reset();
  createOpen.value = true;
}

async function submitCreate(): Promise<void> {
  const bucketName = createForm.bucketName.trim().toLowerCase();
  if (!createForm.snapshotId) {
    createError.value = new Error('请选择已完成 Delivery 的 Ready Snapshot');
    return;
  }
  if (
    bucketName.length < 3 ||
    !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/.test(
      bucketName,
    )
  ) {
    createError.value = new Error('Bucket 必须是小写 DNS 兼容名称');
    return;
  }
  creating.value = true;
  createError.value = undefined;
  try {
    const result = await createMutation.mutateAsync({
      tenant_id: tenantId.value,
      snapshot_id: createForm.snapshotId,
      bucket_name: bucketName,
      request_id: createRequestId.value,
    });
    createOpen.value = false;
    const credential = result.data.secret_access_key
      ? {
          accessKeyId: result.data.access_key_id,
          secretAccessKey: result.data.secret_access_key,
          expiresAtUnixMs: result.data.credential_expires_at_unix_ms,
        }
      : undefined;
    openCredentials(result.data.access_point, credential, !result.data.secret_access_key);
    await queryClient.invalidateQueries({ queryKey: ['s3-access-points', tenantId.value] });
    ElMessage.success(
      result.data.request_replayed ? '已返回现有 Access Point' : 'S3 Access Point 已开启',
    );
  } catch (error) {
    createError.value = error;
  } finally {
    creating.value = false;
  }
}

function openCredentials(
  accessPoint: S3AccessPointView,
  credential?: InitialCredential,
  secretUnavailable = false,
): void {
  credentialAccessPoint.value = accessPoint;
  initialCredential.value = credential
    ? {
        accessKeyId: credential.accessKeyId,
        secretAccessKey: credential.secretAccessKey,
        expiresAtUnixMs: credential.expiresAtUnixMs,
      }
    : undefined;
  initialSecretUnavailable.value = secretUnavailable;
  credentialsOpen.value = true;
}

async function copy(value: string, label: string): Promise<void> {
  try {
    await globalThis.navigator.clipboard.writeText(value);
    ElMessage.success(`${label}已复制`);
  } catch {
    ElMessage.error('复制失败，请手动选择文本');
  }
}

async function updateState(
  accessPoint: S3AccessPointView,
  state: 'active' | 'disabled',
): Promise<void> {
  if (!canManage.value || pendingAction.value) return;
  if (state === 'disabled') {
    try {
      await ElMessageBox.confirm(
        `停用 ${accessPoint.bucket_name} 后，所有 S3 凭证会立即失效。`,
        '确认停用 Access Point',
        { type: 'warning', confirmButtonText: '停用', cancelButtonText: '取消' },
      );
    } catch {
      return;
    }
  }
  pendingAction.value = accessPoint.access_point_id;
  const request: UpdateS3AccessPointRequest = {
    tenant_id: tenantId.value,
    access_point_id: accessPoint.access_point_id,
    request_id: `s3-access-point-${state}-${globalThis.crypto.randomUUID()}`,
  };
  try {
    if (state === 'active') await enableMutation.mutateAsync(request);
    else await disableMutation.mutateAsync(request);
    await queryClient.invalidateQueries({ queryKey: ['s3-access-points', tenantId.value] });
    ElMessage.success(state === 'active' ? 'Access Point 已启用' : 'Access Point 已停用');
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '更新 Access Point 失败');
  } finally {
    pendingAction.value = undefined;
  }
}

async function deleteAccessPoint(accessPoint: S3AccessPointView): Promise<void> {
  if (!canManage.value || pendingAction.value) return;
  try {
    await ElMessageBox.confirm(
      `删除 ${accessPoint.bucket_name} 后，所有 S3 凭证会立即失效，且不能重新启用。`,
      '确认删除 Access Point',
      { type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消' },
    );
  } catch {
    return;
  }
  pendingAction.value = accessPoint.access_point_id;
  try {
    await deleteMutation.mutateAsync({
      tenant_id: tenantId.value,
      access_point_id: accessPoint.access_point_id,
      request_id: `s3-access-point-delete-${globalThis.crypto.randomUUID()}`,
    });
    await queryClient.invalidateQueries({ queryKey: ['s3-access-points', tenantId.value] });
    ElMessage.success('Access Point 已删除');
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '删除 Access Point 失败');
  } finally {
    pendingAction.value = undefined;
  }
}

async function openBrowser(accessPoint: S3AccessPointView): Promise<void> {
  await router.push({
    name: 'object-storage-browser',
    params: { tenantId: tenantId.value, accessPointId: accessPoint.access_point_id },
  });
}

function updateCredentialDialogVisibility(open: boolean): void {
  credentialsOpen.value = open;
  if (!open) {
    initialCredential.value = undefined;
    initialSecretUnavailable.value = false;
  }
}

function nextPage(): void {
  const next = accessPointsQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  cursor.value = cursorHistory.value.pop() || undefined;
}

function stateTagType(state: S3AccessPointView['state']): 'success' | 'warning' | 'danger' {
  if (state === 'active') return 'success';
  if (state === 'deleted') return 'danger';
  return 'warning';
}

function stateLabel(state: S3AccessPointView['state']): string {
  if (state === 'active') return '启用';
  if (state === 'deleted') return '已删除';
  return '已停用';
}
</script>

<template>
  <div class="page object-storage-page">
    <PageHeading title="对象存储" :description="`${tenantId} 的只读 S3 Access Point`">
      <template #actions>
        <el-button
          :icon="Refresh"
          :loading="accessPointsQuery.isFetching.value"
          @click="accessPointsQuery.refetch"
          >刷新</el-button
        >
        <el-button v-if="canManage" type="primary" :icon="Plus" @click="openCreate()"
          >开启 S3 访问</el-button
        >
      </template>
    </PageHeading>
    <section class="content-section resource-section">
      <ApiProblemAlert
        v-if="accessPointsQuery.error.value"
        :error="accessPointsQuery.error.value"
        :retrying="accessPointsQuery.isFetching.value"
        @retry="accessPointsQuery.refetch"
      />
      <el-skeleton v-else-if="accessPointsQuery.isPending.value" :rows="6" animated />
      <el-empty
        v-else-if="accessPoints.length === 0"
        description="暂无 S3 Access Point"
        :image-size="82"
      >
        <el-button v-if="canManage" type="primary" :icon="Plus" @click="openCreate()"
          >开启 S3 访问</el-button
        >
      </el-empty>
      <template v-else>
        <el-table :data="accessPoints" class="resource-table desktop-table">
          <el-table-column label="Bucket" min-width="220">
            <template #default="scope">
              <button class="resource-link" type="button" @click="openBrowser(scope.row)">
                <strong>{{ scope.row.bucket_name }}</strong
                ><code>{{ scope.row.access_point_id }}</code>
              </button>
            </template>
          </el-table-column>
          <el-table-column label="Snapshot / Commit" min-width="280">
            <template #default="scope"
              ><div class="table-placement">
                <strong>{{ scope.row.snapshot_id }}</strong
                ><code>{{ scope.row.commit_id }}</code>
              </div></template
            >
          </el-table-column>
          <el-table-column prop="region" label="Region" width="130" />
          <el-table-column label="Endpoint" min-width="210">
            <template #default="scope"
              ><button
                class="inline-copy"
                type="button"
                @click="copy(scope.row.endpoint, 'Endpoint')"
              >
                <code>{{ scope.row.endpoint }}</code
                ><CopyDocument /></button
            ></template>
          </el-table-column>
          <el-table-column label="状态" width="100">
            <template #default="scope"
              ><el-tag :type="stateTagType(scope.row.state)" effect="plain">{{
                stateLabel(scope.row.state)
              }}</el-tag></template
            >
          </el-table-column>
          <el-table-column label="创建时间" min-width="160"
            ><template #default="scope">{{
              formatTime(scope.row.created_at_unix_ms)
            }}</template></el-table-column
          >
          <el-table-column label="操作" width="180" align="right">
            <template #default="scope">
              <el-button
                text
                :icon="FolderOpened"
                title="打开对象"
                @click="openBrowser(scope.row)"
              />
              <el-button
                v-if="canManage"
                text
                :icon="Key"
                title="管理凭证"
                @click="openCredentials(scope.row)"
              />
              <el-button
                v-if="canManage && scope.row.state === 'active'"
                text
                type="danger"
                :icon="WarningFilled"
                title="停用 Access Point"
                :loading="pendingAction === scope.row.access_point_id"
                @click="updateState(scope.row, 'disabled')"
              />
              <el-button
                v-else-if="canManage && scope.row.state === 'disabled'"
                text
                type="success"
                :icon="Check"
                title="启用 Access Point"
                :loading="pendingAction === scope.row.access_point_id"
                @click="updateState(scope.row, 'active')"
              />
              <el-button
                v-if="canManage && scope.row.state !== 'deleted'"
                text
                type="danger"
                :icon="Delete"
                title="删除 Access Point"
                :loading="pendingAction === scope.row.access_point_id"
                @click="deleteAccessPoint(scope.row)"
              />
            </template>
          </el-table-column>
        </el-table>
        <div class="mobile-resource-list">
          <button
            v-for="accessPoint in accessPoints"
            :key="accessPoint.access_point_id"
            class="mobile-resource-item"
            type="button"
            @click="openBrowser(accessPoint)"
          >
            <span
              ><strong>{{ accessPoint.bucket_name }}</strong
              ><code>{{ accessPoint.snapshot_id }}</code
              ><small>{{ accessPoint.region }} · {{ accessPoint.endpoint }}</small></span
            >
            <span
              ><el-tag :type="stateTagType(accessPoint.state)" size="small" effect="plain">{{
                stateLabel(accessPoint.state)
              }}</el-tag
              ><ArrowRight
            /></span>
          </button>
        </div>
        <PageCursor
          :has-previous="cursorHistory.length > 0"
          :has-next="Boolean(accessPointsQuery.data.value?.data.next_cursor)"
          :loading="accessPointsQuery.isFetching.value"
          @previous="previousPage"
          @next="nextPage"
        />
      </template>
    </section>

    <el-dialog v-model="createOpen" title="开启 S3 访问" width="min(560px, calc(100vw - 28px))">
      <ApiProblemAlert
        v-if="createError && isApiProblem(createError)"
        :error="createError"
        :retrying="creating"
        @retry="submitCreate"
      />
      <el-alert
        v-else-if="createError"
        type="error"
        :closable="false"
        :title="createError instanceof Error ? createError.message : '创建失败'"
      />
      <ApiProblemAlert
        v-if="snapshotsQuery.error.value"
        :error="snapshotsQuery.error.value"
        :retrying="snapshotsQuery.isFetching.value"
        @retry="snapshotsQuery.refetch"
      />
      <el-form label-position="top" @submit.prevent="submitCreate">
        <el-form-item label="已交付 Snapshot" required>
          <el-select
            v-model="createForm.snapshotId"
            filterable
            placeholder="选择已完成只读交付的固定版本"
            :loading="snapshotsQuery.isPending.value"
          >
            <el-option
              v-for="snapshot in readySnapshots"
              :key="snapshot.snapshot_id"
              :label="`${snapshot.message} · ${snapshot.snapshot_id}`"
              :value="snapshot.snapshot_id"
            >
              <span>{{ snapshot.message }}</span
              ><code class="option-code">{{ snapshot.snapshot_id }}</code>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Bucket 名称" required>
          <el-input
            v-model="createForm.bucketName"
            placeholder="dataset-road-scenes"
            maxlength="63"
            @keyup.enter="submitCreate"
          />
          <small class="form-help">仅支持小写字母、数字、短横线和点，名称必须全局唯一。</small>
        </el-form-item>
      </el-form>
      <template #footer
        ><el-button @click="createOpen = false">取消</el-button
        ><el-button type="primary" :loading="creating" @click="submitCreate"
          >创建并生成凭证</el-button
        ></template
      >
    </el-dialog>
    <S3CredentialDialog
      :model-value="credentialsOpen"
      :access-point="credentialAccessPoint"
      v-bind="initialCredential ? { initialCredential } : {}"
      :initial-secret-unavailable="initialSecretUnavailable"
      @update:model-value="updateCredentialDialogVisibility"
    />
  </div>
</template>

<style scoped>
.object-storage-page {
  max-width: 1280px;
}
.inline-copy {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: 0;
  padding: 0;
  color: inherit;
  background: transparent;
  cursor: pointer;
  text-align: left;
}
.inline-copy:hover {
  color: var(--green);
}
.inline-copy svg {
  width: 14px;
  color: var(--muted);
}
.option-code {
  float: right;
  margin-left: 12px;
  color: var(--muted);
  font-size: 11px;
}
.form-help {
  display: block;
  margin-top: 5px;
  color: var(--muted);
  font-size: 12px;
}
@media (max-width: 900px) {
  .desktop-table {
    display: none;
  }
}
</style>
