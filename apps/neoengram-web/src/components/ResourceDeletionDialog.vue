<script setup lang="ts">
import { Delete } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, ref, watch } from 'vue';

import { createDeletion, queryDeletionImpact } from '@/api/operations';
import type { CreateDeletionRequest, DeletionOperationView, ResourceRef } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import { resourceRefId, resourceRefLabel } from '@/features/lifecycle';
import { formatBytes, formatCount, formatTime } from '@/utils/format';

const props = defineProps<{
  tenantId: string;
  resource: ResourceRef;
  resourceVersion: string;
  displayName?: string;
  disabled?: boolean;
}>();
const emit = defineEmits<{ deleted: [deletion: DeletionOperationView] }>();

const queryClient = useQueryClient();
const open = ref(false);
const cascade = ref(false);
const confirmManagedDataErase = ref(false);
const confirmation = ref('');
const pendingRequest = ref<CreateDeletionRequest>();
const resourceId = computed(() => resourceRefId(props.resource));
const resourceType = computed(() => resourceRefLabel(props.resource));
const requiresCascade = computed(() =>
  ['artifact', 'storage_volume'].includes(props.resource.type),
);
const isStorageVolume = computed(() => props.resource.type === 'storage_volume');

const impactQuery = useQuery({
  queryKey: computed(() => [
    'resource-deletion-impact',
    props.tenantId,
    JSON.stringify(props.resource),
    props.resourceVersion,
    cascade.value,
    confirmManagedDataErase.value,
  ]),
  queryFn: () =>
    queryDeletionImpact({
      tenant_id: props.tenantId,
      resource: props.resource,
      cascade: cascade.value,
      confirm_managed_data_erase: confirmManagedDataErase.value,
      expected_resource_version: props.resourceVersion,
    }),
  enabled: computed(() => open.value),
  staleTime: 0,
});
const deleteMutation = useMutation({ mutationFn: createDeletion });
const impact = computed(() => impactQuery.data.value?.data.impact);
const impactDigest = computed(() => impactQuery.data.value?.data.impact_digest ?? '');
const blocked = computed(() => Boolean(impact.value?.blockers.length));
const prerequisitesConfirmed = computed(
  () =>
    (!requiresCascade.value || cascade.value) &&
    (!isStorageVolume.value || confirmManagedDataErase.value),
);
const canSubmit = computed(
  () =>
    Boolean(impact.value) &&
    !impactQuery.isFetching.value &&
    !blocked.value &&
    prerequisitesConfirmed.value &&
    confirmation.value === resourceId.value &&
    Date.now() < Number(impact.value?.expires_at_unix_ms ?? 0),
);

watch([cascade, confirmManagedDataErase], () => {
  pendingRequest.value = undefined;
  deleteMutation.reset();
});

function show(): void {
  cascade.value = false;
  confirmManagedDataErase.value = false;
  confirmation.value = '';
  pendingRequest.value = undefined;
  deleteMutation.reset();
  open.value = true;
}

async function submit(): Promise<void> {
  if (!canSubmit.value || !impactDigest.value) return;
  pendingRequest.value ??= {
    tenant_id: props.tenantId,
    resource: props.resource,
    cascade: cascade.value,
    confirm_managed_data_erase: confirmManagedDataErase.value,
    expected_resource_version: props.resourceVersion,
    impact_digest: impactDigest.value,
    request_id: `resource-deletion-${globalThis.crypto.randomUUID()}`,
  };
  let result;
  try {
    result = await deleteMutation.mutateAsync(pendingRequest.value);
  } catch {
    return;
  }
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ['resource-deletions', props.tenantId] }),
    queryClient.invalidateQueries({ queryKey: ['artifacts', props.tenantId] }),
    queryClient.invalidateQueries({ queryKey: ['playgrounds', props.tenantId] }),
    queryClient.invalidateQueries({ queryKey: ['snapshots', props.tenantId] }),
    queryClient.invalidateQueries({ queryKey: ['storage-volumes', props.tenantId] }),
  ]);
  open.value = false;
  emit('deleted', result.data.deletion);
  ElMessage.success(result.data.replayed ? '已返回原删除任务' : '资源已停止访问并进入回收流程');
}
</script>

<template>
  <span class="deletion-trigger" @click.stop @keydown.stop>
    <el-tooltip content="删除资源" placement="top">
      <el-button
        text
        type="danger"
        :icon="Delete"
        :disabled="disabled"
        :aria-label="`删除 ${resourceId}`"
        @click="show"
      />
    </el-tooltip>
  </span>

  <el-dialog
    v-model="open"
    :title="`删除 ${resourceType}`"
    width="min(660px, calc(100vw - 32px))"
    :close-on-click-modal="false"
    destroy-on-close
    append-to-body
  >
    <ApiProblemAlert
      v-if="impactQuery.error.value"
      :error="impactQuery.error.value"
      :retrying="impactQuery.isFetching.value"
      @retry="impactQuery.refetch"
    />
    <ApiProblemAlert v-if="deleteMutation.error.value" :error="deleteMutation.error.value" />

    <el-skeleton v-if="impactQuery.isPending.value" :rows="5" animated />
    <template v-else-if="impact">
      <el-alert
        title="资源会立即停止访问，7 天内可从回收站整批恢复。"
        type="warning"
        :closable="false"
        show-icon
      />

      <dl class="impact-grid">
        <div>
          <dt>资源</dt>
          <dd>{{ displayName || resourceId }}</dd>
        </div>
        <div>
          <dt>受影响资源</dt>
          <dd>{{ formatCount(String(impact.targets.length)) }}</dd>
        </div>
        <div>
          <dt>活动任务</dt>
          <dd>{{ formatCount(impact.active_job_count) }}</dd>
        </div>
        <div>
          <dt>S3 凭证</dt>
          <dd>{{ formatCount(impact.active_s3_credential_count) }}</dd>
        </div>
        <div>
          <dt>预计文件</dt>
          <dd>{{ formatCount(impact.estimated_file_count) }}</dd>
        </div>
        <div>
          <dt>预计数据</dt>
          <dd>{{ formatBytes(impact.estimated_bytes) }}</dd>
        </div>
        <div>
          <dt>影响摘要有效至</dt>
          <dd>{{ formatTime(impact.expires_at_unix_ms) }}</dd>
        </div>
      </dl>

      <el-alert
        v-for="blocker in impact.blockers"
        :key="`${blocker.code}:${blocker.message}`"
        :title="blocker.message"
        :description="blocker.code"
        type="error"
        :closable="false"
        show-icon
      />

      <el-form label-position="top" class="dialog-form deletion-confirmation">
        <el-form-item v-if="requiresCascade">
          <el-checkbox v-model="cascade"> 级联处理该资源依赖的 Snapshot 和 Playground </el-checkbox>
        </el-form-item>
        <el-form-item v-if="isStorageVolume">
          <el-checkbox v-model="confirmManagedDataErase">
            我确认清理 NeoEngram 受管目录；PVC / NFS Export 本身不会被删除
          </el-checkbox>
        </el-form-item>
        <el-form-item :label="`输入 ${resourceId} 确认删除`" required>
          <el-input v-model="confirmation" autocomplete="off" :placeholder="resourceId" />
        </el-form-item>
      </el-form>
    </template>

    <template #footer>
      <el-button @click="open = false">取消</el-button>
      <el-button
        type="danger"
        :icon="Delete"
        :disabled="!canSubmit"
        :loading="deleteMutation.isPending.value"
        @click="submit"
      >
        停止访问并移入回收站
      </el-button>
    </template>
  </el-dialog>
</template>

<style scoped>
.deletion-trigger {
  display: inline-flex;
}

.impact-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px 20px;
  margin: 18px 0;
}

.impact-grid div {
  min-width: 0;
}

.impact-grid dt {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.impact-grid dd {
  margin: 4px 0 0;
  overflow-wrap: anywhere;
  font-weight: 600;
}

.deletion-confirmation {
  margin-top: 18px;
}

@media (max-width: 640px) {
  .impact-grid {
    grid-template-columns: 1fr;
  }
}
</style>
