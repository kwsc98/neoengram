<script setup lang="ts">
import {
  ArrowLeft,
  CopyDocument,
  Document,
  Download,
  Folder,
  Key,
  Refresh,
  Search,
} from '@element-plus/icons-vue';
import { useMutation, useQuery } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { createS3DownloadUrl, queryS3AccessPoint, queryS3ObjectList } from '@/api/operations';
import type { S3ObjectEntryView } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageCursor from '@/components/PageCursor.vue';
import PageHeading from '@/components/PageHeading.vue';
import S3CredentialDialog from '@/components/S3CredentialDialog.vue';
import { useTenantsStore } from '@/stores/tenants';
import { formatBytes, formatTime } from '@/utils/format';

interface BreadcrumbItem {
  label: string;
  prefix: string;
}

const route = useRoute();
const router = useRouter();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const accessPointId = computed(() => String(route.params.accessPointId ?? ''));
const permissions = computed(() => tenants.byId(tenantId.value)?.permissions ?? []);
const canManage = computed(() =>
  (permissions.value as readonly string[]).includes('s3.access.manage'),
);
const prefix = ref(String(route.query.prefix ?? ''));
const searchInput = ref(prefix.value);
const cursor = ref<string>();
const cursorHistory = ref<string[]>([]);
const selected = ref<S3ObjectEntryView>();
const detailOpen = ref(false);
const credentialsOpen = ref(false);
const downloadingKey = ref('');

const accessPointQuery = useQuery({
  queryKey: computed(() => ['s3-access-point', tenantId.value, accessPointId.value]),
  queryFn: () =>
    queryS3AccessPoint({ tenant_id: tenantId.value, access_point_id: accessPointId.value }),
});
const accessPoint = computed(() => accessPointQuery.data.value?.data.access_point);
const objectsQuery = useQuery({
  queryKey: computed(() => [
    's3-objects',
    tenantId.value,
    accessPointId.value,
    prefix.value,
    cursor.value ?? '',
  ]),
  enabled: computed(() => accessPoint.value?.state === 'active'),
  queryFn: () =>
    queryS3ObjectList({
      tenant_id: tenantId.value,
      access_point_id: accessPointId.value,
      prefix: prefix.value,
      delimiter: '/',
      page_size: 100,
      ...(cursor.value ? { cursor: cursor.value } : {}),
    }),
});
const downloadMutation = useMutation({ mutationFn: createS3DownloadUrl });
const entries = computed(() => {
  const response = objectsQuery.data.value?.data;
  if (!response) return [];
  const items = [...response.items];
  const knownPrefixes = new Set(
    items.filter((item) => item.entry_type === 'prefix').map((item) => item.key),
  );
  for (const value of response.common_prefixes ?? []) {
    const key = value.endsWith('/') ? value : `${value}/`;
    if (!knownPrefixes.has(key)) items.push({ key, entry_type: 'prefix' });
  }
  return items.sort((left, right) => {
    if (left.entry_type !== right.entry_type) return left.entry_type === 'prefix' ? -1 : 1;
    return left.key.localeCompare(right.key);
  });
});
const breadcrumbs = computed<BreadcrumbItem[]>(() => {
  const parts = prefix.value.split('/').filter(Boolean);
  const items: BreadcrumbItem[] = [
    { label: accessPoint.value?.bucket_name ?? 'Bucket', prefix: '' },
  ];
  let value = '';
  for (const part of parts) {
    value += `${part}/`;
    items.push({ label: part, prefix: value });
  }
  return items;
});

watch(
  () => route.query.prefix,
  (value) => {
    const next = String(value ?? '');
    prefix.value = next;
    searchInput.value = next;
    resetCursor();
  },
);
watch(accessPointId, () => {
  prefix.value = '';
  searchInput.value = '';
  resetCursor();
  credentialsOpen.value = false;
});

function resetCursor(): void {
  cursor.value = undefined;
  cursorHistory.value = [];
}

function displayName(entry: S3ObjectEntryView): string {
  const relative = entry.key.startsWith(prefix.value)
    ? entry.key.slice(prefix.value.length)
    : entry.key;
  return entry.entry_type === 'prefix' ? relative.replace(/\/$/, '') : relative;
}

function contentType(key: string): string {
  const extension = key.split('.').pop()?.toLowerCase();
  const known: Record<string, string> = {
    csv: 'text/csv',
    json: 'application/json',
    jsonl: 'application/x-ndjson',
    parquet: 'application/vnd.apache.parquet',
    png: 'image/png',
    jpg: 'image/jpeg',
    jpeg: 'image/jpeg',
    txt: 'text/plain',
    pdf: 'application/pdf',
    zip: 'application/zip',
  };
  return (extension && known[extension]) || 'application/octet-stream';
}

async function setPrefix(value: string): Promise<void> {
  const normalized = value.trim().replace(/^\/+/, '');
  if (!normalized) {
    resetCursor();
    await router.replace({ query: {} });
    return;
  }
  const segments = normalized.split('/');
  if (normalized.endsWith('/')) segments.pop();
  if (
    normalized.includes('\\') ||
    segments.some((segment) => !segment || segment === '.' || segment === '..')
  ) {
    ElMessage.error('Prefix 不是规范对象路径');
    return;
  }
  resetCursor();
  await router.replace({ query: normalized ? { prefix: normalized } : {} });
}

async function applySearch(): Promise<void> {
  await setPrefix(searchInput.value);
}

async function openEntry(entry: S3ObjectEntryView): Promise<void> {
  if (entry.entry_type === 'prefix') {
    await setPrefix(entry.key.endsWith('/') ? entry.key : `${entry.key}/`);
    return;
  }
  selected.value = entry;
  detailOpen.value = true;
}

async function back(): Promise<void> {
  await router.push({ name: 'object-storage-list', params: { tenantId: tenantId.value } });
}

async function copy(value: string, label: string): Promise<void> {
  try {
    await globalThis.navigator.clipboard.writeText(value);
    ElMessage.success(`${label}已复制`);
  } catch {
    ElMessage.error('复制失败，请手动选择文本');
  }
}

function s3Uri(entry: S3ObjectEntryView): string {
  return `s3://${accessPoint.value?.bucket_name ?? ''}/${entry.key}`;
}

async function download(entry: S3ObjectEntryView): Promise<void> {
  if (entry.entry_type !== 'object' || downloadingKey.value) return;
  downloadingKey.value = entry.key;
  try {
    const result = await downloadMutation.mutateAsync({
      tenant_id: tenantId.value,
      access_point_id: accessPointId.value,
      key: entry.key,
      expires_seconds: 300,
    });
    const link = globalThis.document.createElement('a');
    link.href = result.data.url;
    link.download = entry.key.split('/').pop() ?? 'download';
    link.rel = 'noopener';
    globalThis.document.body.append(link);
    link.click();
    link.remove();
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '下载地址创建失败');
  } finally {
    downloadingKey.value = '';
  }
}

function nextPage(): void {
  const next = objectsQuery.data.value?.data.next_cursor;
  if (!next) return;
  cursorHistory.value.push(cursor.value ?? '');
  cursor.value = next;
}

function previousPage(): void {
  cursor.value = cursorHistory.value.pop() || undefined;
}
</script>

<template>
  <div class="page object-browser-page">
    <PageHeading
      :title="accessPoint?.bucket_name ?? '对象浏览器'"
      :description="accessPoint?.endpoint ?? accessPointId"
    >
      <template #actions>
        <el-button :icon="ArrowLeft" @click="back">返回 Bucket</el-button>
        <el-button v-if="canManage && accessPoint" :icon="Key" @click="credentialsOpen = true"
          >凭证</el-button
        >
        <el-button
          :icon="Refresh"
          :loading="objectsQuery.isFetching.value"
          @click="objectsQuery.refetch"
          >刷新</el-button
        >
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="accessPointQuery.error.value"
      :error="accessPointQuery.error.value"
      :retrying="accessPointQuery.isFetching.value"
      @retry="accessPointQuery.refetch"
    />
    <el-skeleton v-if="accessPointQuery.isPending.value" :rows="4" animated />
    <template v-else-if="accessPoint">
      <el-alert
        v-if="accessPoint.state === 'disabled'"
        type="warning"
        :closable="false"
        title="Access Point 已停用"
        description="对象浏览和下载已关闭；重新启用后需要创建新凭证。"
      />
      <section class="object-toolbar" aria-label="对象路径与搜索">
        <el-breadcrumb separator="/" class="object-breadcrumb">
          <el-breadcrumb-item v-for="item in breadcrumbs" :key="item.prefix">
            <button type="button" @click="setPrefix(item.prefix)">{{ item.label }}</button>
          </el-breadcrumb-item>
        </el-breadcrumb>
        <form class="object-search" @submit.prevent="applySearch">
          <el-input
            v-model="searchInput"
            :prefix-icon="Search"
            clearable
            placeholder="按对象前缀搜索"
            aria-label="对象前缀"
          />
          <el-button type="primary" native-type="submit" :icon="Search">查询</el-button>
        </form>
      </section>
      <section class="content-section object-list-section">
        <ApiProblemAlert
          v-if="objectsQuery.error.value"
          :error="objectsQuery.error.value"
          :retrying="objectsQuery.isFetching.value"
          @retry="objectsQuery.refetch"
        />
        <el-skeleton
          v-else-if="objectsQuery.isPending.value && accessPoint.state === 'active'"
          :rows="7"
          animated
        />
        <el-empty
          v-else-if="accessPoint.state === 'disabled'"
          description="当前 Bucket 不可访问"
          :image-size="76"
        />
        <el-empty
          v-else-if="entries.length === 0"
          :description="prefix ? '当前前缀下没有对象' : 'Bucket 为空'"
          :image-size="76"
        />
        <template v-else>
          <el-table :data="entries" class="object-table">
            <el-table-column label="名称" min-width="310">
              <template #default="scope">
                <button class="object-name" type="button" @click="openEntry(scope.row)">
                  <el-icon
                    ><Folder v-if="scope.row.entry_type === 'prefix'" /><Document v-else
                  /></el-icon>
                  <span>{{ displayName(scope.row) }}</span>
                </button>
              </template>
            </el-table-column>
            <el-table-column label="大小" width="130"
              ><template #default="scope">{{
                scope.row.entry_type === 'object' ? formatBytes(scope.row.size_bytes) : '—'
              }}</template></el-table-column
            >
            <el-table-column label="类型" min-width="180"
              ><template #default="scope">{{
                scope.row.entry_type === 'prefix' ? '目录' : contentType(scope.row.key)
              }}</template></el-table-column
            >
            <el-table-column label="修改时间" min-width="170"
              ><template #default="scope">{{
                formatTime(scope.row.last_modified_unix_ms)
              }}</template></el-table-column
            >
            <el-table-column label="ETag" min-width="190"
              ><template #default="scope"
                ><code v-if="scope.row.etag">{{ scope.row.etag }}</code
                ><span v-else>—</span></template
              ></el-table-column
            >
            <el-table-column label="操作" width="120" align="right">
              <template #default="scope">
                <template v-if="scope.row.entry_type === 'object'">
                  <el-button
                    text
                    :icon="CopyDocument"
                    title="复制 S3 URI"
                    @click.stop="copy(s3Uri(scope.row), 'S3 URI')"
                  />
                  <el-button
                    text
                    :icon="Download"
                    title="下载对象"
                    :loading="downloadingKey === scope.row.key"
                    @click.stop="download(scope.row)"
                  />
                </template>
              </template>
            </el-table-column>
          </el-table>
          <PageCursor
            :has-previous="cursorHistory.length > 0"
            :has-next="Boolean(objectsQuery.data.value?.data.next_cursor)"
            :loading="objectsQuery.isFetching.value"
            @previous="previousPage"
            @next="nextPage"
          />
        </template>
      </section>
    </template>

    <el-drawer v-model="detailOpen" title="对象详情" size="min(440px, 92vw)">
      <template v-if="selected">
        <div class="object-detail-heading">
          <Document /><strong>{{ displayName(selected) }}</strong>
        </div>
        <dl class="object-detail-list">
          <div>
            <dt>Key</dt>
            <dd>
              <code>{{ selected.key }}</code>
            </dd>
          </div>
          <div>
            <dt>S3 URI</dt>
            <dd>
              <button type="button" class="detail-copy" @click="copy(s3Uri(selected), 'S3 URI')">
                <code>{{ s3Uri(selected) }}</code
                ><CopyDocument />
              </button>
            </dd>
          </div>
          <div>
            <dt>大小</dt>
            <dd>{{ formatBytes(selected.size_bytes) }}</dd>
          </div>
          <div>
            <dt>Content-Type</dt>
            <dd>{{ contentType(selected.key) }}</dd>
          </div>
          <div>
            <dt>Last-Modified</dt>
            <dd>{{ formatTime(selected.last_modified_unix_ms) }}</dd>
          </div>
          <div>
            <dt>ETag</dt>
            <dd>
              <code>{{ selected.etag ?? '—' }}</code>
            </dd>
          </div>
        </dl>
        <el-button
          type="primary"
          :icon="Download"
          :loading="downloadingKey === selected.key"
          @click="download(selected)"
          >下载对象</el-button
        >
      </template>
    </el-drawer>
    <S3CredentialDialog v-model="credentialsOpen" :access-point="accessPoint" />
  </div>
</template>

<style scoped>
.object-browser-page {
  max-width: 1280px;
}
.object-toolbar {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(320px, 480px);
  align-items: center;
  gap: 18px;
  margin-bottom: 14px;
  padding: 12px 14px;
  border: 1px solid var(--line);
  background: var(--surface);
}
.object-breadcrumb {
  min-width: 0;
  overflow: hidden;
}
.object-breadcrumb button {
  border: 0;
  padding: 2px 0;
  color: #355149;
  background: transparent;
  cursor: pointer;
}
.object-breadcrumb button:hover {
  color: var(--green);
}
.object-search {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 8px;
}
.object-list-section {
  padding: 0;
  overflow: hidden;
}
.object-name {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 9px;
  border: 0;
  padding: 1px 0;
  color: #244d40;
  background: transparent;
  cursor: pointer;
  text-align: left;
}
.object-name:hover span {
  text-decoration: underline;
}
.object-name .el-icon {
  flex: 0 0 auto;
  color: #44816d;
  font-size: 18px;
}
.object-table code {
  font-size: 11px;
}
.object-detail-heading {
  display: flex;
  align-items: center;
  gap: 9px;
  padding-bottom: 14px;
  border-bottom: 1px solid var(--line);
}
.object-detail-heading svg {
  width: 22px;
  color: var(--green);
}
.object-detail-list {
  display: grid;
  gap: 1px;
  margin: 14px 0;
  background: var(--line);
}
.object-detail-list div {
  min-width: 0;
  padding: 12px;
  background: #fff;
}
.object-detail-list dt {
  margin-bottom: 5px;
  color: var(--muted);
  font-size: 11px;
}
.object-detail-list dd {
  margin: 0;
  overflow-wrap: anywhere;
}
.detail-copy {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  border: 0;
  padding: 0;
  color: inherit;
  background: transparent;
  cursor: pointer;
  text-align: left;
}
.detail-copy svg {
  flex: 0 0 auto;
  width: 15px;
  color: var(--muted);
}
@media (max-width: 760px) {
  .object-toolbar {
    grid-template-columns: 1fr;
  }
  .object-search {
    grid-template-columns: 1fr;
  }
}
</style>
