<script setup lang="ts">
import { Back, Box, Check, Files, Plus, RefreshRight } from '@element-plus/icons-vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { ElMessage } from 'element-plus';
import { computed, reactive, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { commitPlayground, queryPlayground } from '@/api/operations';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { useTenantsStore } from '@/stores/tenants';
import { formatTime } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const queryClient = useQueryClient();
const tenants = useTenantsStore();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const playgroundId = computed(() => String(route.params.playgroundId ?? ''));
const playgroundQuery = useQuery({
  queryKey: computed(() => [
    'playground',
    tenantId.value,
    projectId.value,
    artifactId.value,
    playgroundId.value,
  ]),
  queryFn: () =>
    queryPlayground(tenantId.value, projectId.value, artifactId.value, playgroundId.value),
});
const playground = computed(() => playgroundQuery.data.value?.data.playground);
const commitOpen = ref(false);
const commitError = ref('');
const tagInput = ref('');
const commitForm = reactive({
  commitRequestId: '',
  message: '',
  description: '',
  tagNames: [] as string[],
});
const canCommit = computed(
  () =>
    (tenants.byId(tenantId.value)?.permissions.includes('commit.create') ?? false) &&
    playground.value?.state === 'ready',
);
const commitMutation = useMutation({ mutationFn: commitPlayground });
const tagPattern = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$/;

function showCommit(): void {
  commitForm.commitRequestId = `commit-request-${globalThis.crypto.randomUUID()}`;
  commitForm.message = '';
  commitForm.description = '';
  commitForm.tagNames = [];
  tagInput.value = '';
  commitError.value = '';
  commitOpen.value = true;
}

function addTag(): boolean {
  const tagName = tagInput.value.trim();
  if (!tagName) return true;
  if (!tagPattern.test(tagName) || tagName.startsWith('refs/')) {
    commitError.value = 'Tag 必须以字母或数字开头，且不能包含 refs/ 前缀';
    return false;
  }
  if (commitForm.tagNames.includes(tagName)) {
    commitError.value = `Tag ${tagName} 已添加`;
    return false;
  }
  if (commitForm.tagNames.length >= 20) {
    commitError.value = '一次最多创建 20 个 Tag';
    return false;
  }
  commitForm.tagNames.push(tagName);
  tagInput.value = '';
  commitError.value = '';
  return true;
}

function removeTag(tagName: string): void {
  commitForm.tagNames = commitForm.tagNames.filter((item) => item !== tagName);
}

async function submitCommit(): Promise<void> {
  commitError.value = '';
  const current = playground.value;
  if (!current || !commitForm.message.trim()) {
    commitError.value = '请输入 Commit message';
    return;
  }
  if (!addTag()) return;
  const tagNames = commitForm.tagNames.map((tagName) => tagName.trim());
  if (
    tagNames.length > 20 ||
    new Set(tagNames).size !== tagNames.length ||
    tagNames.some((tagName) => !tagPattern.test(tagName) || tagName.startsWith('refs/'))
  ) {
    commitError.value = 'Tag 必须是合法且不重复的名称，不能包含 refs/ 前缀';
    return;
  }
  try {
    const result = await commitMutation.mutateAsync({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      playground_id: playgroundId.value,
      commit_request_id: commitForm.commitRequestId,
      expected_index_version: current.index_version,
      message: commitForm.message.trim(),
      ...(commitForm.description.trim() ? { description: commitForm.description.trim() } : {}),
      ...(tagNames.length ? { tag_names: tagNames } : {}),
    });
    commitOpen.value = false;
    await Promise.all([
      playgroundQuery.refetch(),
      queryClient.invalidateQueries({
        queryKey: ['artifact-commits', tenantId.value, projectId.value, artifactId.value],
      }),
      queryClient.invalidateQueries({
        queryKey: ['artifact', tenantId.value, projectId.value, artifactId.value],
      }),
    ]);
    ElMessage.success(
      result.data.replayed
        ? `已重放 Commit ${result.data.commit.commit_id}`
        : `Commit ${result.data.commit.commit_id} 已创建`,
    );
  } catch (error) {
    commitError.value = error instanceof Error ? error.message : '提交 Playground 失败';
  }
}

async function openHeadCommit(): Promise<void> {
  if (!playground.value?.head_commit_id) return;
  await router.push({
    name: 'artifact-detail',
    params: { tenantId: tenantId.value, projectId: projectId.value, artifactId: artifactId.value },
    query: { tab: 'commits', commit_id: playground.value.head_commit_id },
  });
}
</script>

<template>
  <div class="page">
    <PageHeading
      :title="playground?.display_name ?? playgroundId"
      :description="`${projectId} / ${artifactId} / ${playgroundId}`"
    >
      <template #actions>
        <el-button v-if="canCommit" type="primary" :icon="Check" @click="showCommit">
          Commit
        </el-button>
        <el-button
          :icon="Back"
          @click="router.push({ name: 'playground-list', params: { tenantId } })"
        >
          返回列表
        </el-button>
        <el-button
          :icon="RefreshRight"
          :loading="playgroundQuery.isFetching.value"
          @click="playgroundQuery.refetch"
          >刷新</el-button
        >
      </template>
    </PageHeading>
    <ApiProblemAlert
      v-if="playgroundQuery.error.value"
      :error="playgroundQuery.error.value"
      :retrying="playgroundQuery.isFetching.value"
      @retry="playgroundQuery.refetch"
    />
    <template v-if="playground">
      <section class="resource-summary">
        <div>
          <span>状态</span><el-tag effect="plain">{{ playground.state }}</el-tag>
        </div>
        <div>
          <span>Region</span><strong>{{ playground.region }}</strong>
        </div>
        <div>
          <span>Index revision</span><strong>{{ playground.index_version.revision }}</strong>
        </div>
        <div>
          <span>更新时间</span><strong>{{ formatTime(playground.updated_at_unix_ms) }}</strong>
        </div>
      </section>
      <section class="content-section">
        <div class="section-heading section-heading--inline">
          <div>
            <h2>资源信息</h2>
            <p>公开 PlaygroundView</p>
          </div>
          <div class="section-actions">
            <el-button
              v-if="playground.head_commit_id"
              text
              type="primary"
              :icon="Files"
              @click="openHeadCommit"
            >
              查看 Head Commit
            </el-button>
            <el-button
              text
              type="primary"
              :icon="Box"
              @click="
                router.push({
                  name: 'artifact-detail',
                  params: { tenantId, projectId, artifactId },
                })
              "
              >查看 Artifact</el-button
            >
          </div>
        </div>
        <dl class="definition-grid definition-grid--scope">
          <div>
            <dt>Tenant</dt>
            <dd>{{ playground.tenant_id }}</dd>
          </div>
          <div>
            <dt>Project</dt>
            <dd>{{ playground.project_id }}</dd>
          </div>
          <div>
            <dt>Artifact</dt>
            <dd>{{ playground.artifact_id }}</dd>
          </div>
          <div>
            <dt>Playground</dt>
            <dd>{{ playground.playground_id }}</dd>
          </div>
          <div>
            <dt>StorageVolume</dt>
            <dd>
              <code>{{ playground.storage_volume_id }}</code>
            </dd>
          </div>
          <div>
            <dt>Base commit</dt>
            <dd>
              <code>{{ playground.base_commit_id ?? '—' }}</code>
            </dd>
          </div>
          <div>
            <dt>Head commit</dt>
            <dd>
              <code>{{ playground.head_commit_id ?? '—' }}</code>
            </dd>
          </div>
          <div>
            <dt>Index digest</dt>
            <dd>
              <code>{{ playground.index_version.digest }}</code>
            </dd>
          </div>
          <div>
            <dt>创建时间</dt>
            <dd>{{ formatTime(playground.created_at_unix_ms) }}</dd>
          </div>
        </dl>
      </section>
    </template>
    <div v-else-if="playgroundQuery.isPending.value" class="page-loading">
      <el-skeleton :rows="8" animated />
    </div>

    <el-dialog
      v-model="commitOpen"
      title="Commit Playground"
      width="min(600px, calc(100vw - 32px))"
    >
      <ApiProblemAlert v-if="commitMutation.error.value" :error="commitMutation.error.value" />
      <el-alert v-if="commitError" :title="commitError" type="error" :closable="false" />
      <section v-if="playground" class="commit-context">
        <span>Index revision</span>
        <strong>{{ playground.index_version.revision }}</strong>
        <span>当前 Head</span>
        <code>{{ playground.head_commit_id ?? '尚无 Commit' }}</code>
      </section>
      <el-form label-position="top" class="dialog-form">
        <el-form-item label="Commit message" required>
          <el-input
            v-model="commitForm.message"
            placeholder="这次变更的简短标题"
            maxlength="256"
            show-word-limit
          />
        </el-form-item>
        <el-form-item label="详细描述">
          <el-input
            v-model="commitForm.description"
            type="textarea"
            :rows="4"
            placeholder="记录变更背景和内容"
            maxlength="2048"
            show-word-limit
          />
        </el-form-item>
        <el-form-item label="Tags">
          <div class="tag-editor">
            <div class="tag-editor__input">
              <el-input
                v-model="tagInput"
                aria-label="Tags"
                placeholder="输入 Tag 名称后按 Enter"
                maxlength="128"
                @keyup.enter.prevent="addTag"
              />
              <el-tooltip content="添加 Tag" placement="top">
                <el-button :icon="Plus" aria-label="添加 Tag" @click="addTag" />
              </el-tooltip>
            </div>
            <div v-if="commitForm.tagNames.length" class="tag-editor__values">
              <el-tag
                v-for="tagName in commitForm.tagNames"
                :key="tagName"
                closable
                @close="removeTag(tagName)"
              >
                {{ tagName }}
              </el-tag>
            </div>
          </div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="commitOpen = false">取消</el-button>
        <el-button type="primary" :loading="commitMutation.isPending.value" @click="submitCommit">
          创建 Commit
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>
