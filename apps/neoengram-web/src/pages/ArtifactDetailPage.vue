<script setup lang="ts">
import { ArrowRight, RefreshRight } from '@element-plus/icons-vue';
import { useQuery } from '@tanstack/vue-query';
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import {
  queryArtifact,
  queryArtifactCommitGraph,
  queryPlaygroundList,
  querySnapshotList,
} from '@/api/operations';
import type { CommitNode } from '@/api/types';
import ApiProblemAlert from '@/components/ApiProblemAlert.vue';
import PageHeading from '@/components/PageHeading.vue';
import { formatBytes, formatCount, formatTime, shortId } from '@/utils/format';

const route = useRoute();
const router = useRouter();
const tenantId = computed(() => String(route.params.tenantId ?? ''));
const projectId = computed(() => String(route.params.projectId ?? ''));
const artifactId = computed(() => String(route.params.artifactId ?? ''));
const allowedTabs = ['overview', 'commits', 'playgrounds', 'snapshots'];
const initialTab = String(route.query.tab ?? 'overview');
const activeTab = ref(allowedTabs.includes(initialTab) ? initialTab : 'overview');
const commitNodes = ref<CommitNode[]>([]);
const nextCommitCursor = ref<string>();
const loadingMoreCommits = ref(false);

const artifactQuery = useQuery({
  queryKey: computed(() => ['artifact', tenantId.value, projectId.value, artifactId.value]),
  queryFn: () => queryArtifact(tenantId.value, projectId.value, artifactId.value),
});
const artifact = computed(() => artifactQuery.data.value?.data.artifact);

const commitQuery = useQuery({
  queryKey: computed(() => ['artifact-commits', tenantId.value, projectId.value, artifactId.value]),
  queryFn: () => queryArtifactCommitGraph(tenantId.value, projectId.value, artifactId.value),
  enabled: computed(() => activeTab.value === 'commits'),
});
const playgroundQuery = useQuery({
  queryKey: computed(() => [
    'playgrounds',
    tenantId.value,
    projectId.value,
    artifactId.value,
    'artifact-detail',
  ]),
  queryFn: () =>
    queryPlaygroundList({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      page_size: 100,
    }),
  enabled: computed(() => activeTab.value === 'playgrounds'),
});
const snapshotQuery = useQuery({
  queryKey: computed(() => [
    'snapshots',
    tenantId.value,
    projectId.value,
    artifactId.value,
    'artifact-detail',
  ]),
  queryFn: () =>
    querySnapshotList({
      tenant_id: tenantId.value,
      project_id: projectId.value,
      artifact_id: artifactId.value,
      page_size: 100,
    }),
  enabled: computed(() => activeTab.value === 'snapshots'),
});

watch(
  () => commitQuery.data.value,
  (result) => {
    if (!result) return;
    commitNodes.value = [...result.data.graph.nodes];
    nextCommitCursor.value = result.data.graph.next_cursor;
  },
  { immediate: true },
);

watch(
  () => route.query.tab,
  (tab) => {
    const value = String(tab ?? 'overview');
    activeTab.value = allowedTabs.includes(value) ? value : 'overview';
  },
);

async function changeTab(tab: string | number): Promise<void> {
  const value = String(tab);
  await router.replace({ query: value === 'overview' ? {} : { tab: value } });
}

async function loadMoreCommits(): Promise<void> {
  if (!nextCommitCursor.value) return;
  loadingMoreCommits.value = true;
  try {
    const result = await queryArtifactCommitGraph(
      tenantId.value,
      projectId.value,
      artifactId.value,
      nextCommitCursor.value,
    );
    commitNodes.value.push(...result.data.graph.nodes);
    nextCommitCursor.value = result.data.graph.next_cursor;
  } finally {
    loadingMoreCommits.value = false;
  }
}

async function openPlayground(playgroundId: string): Promise<void> {
  await router.push({
    name: 'playground-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      playgroundId,
    },
  });
}

async function openSnapshot(commitId: string): Promise<void> {
  await router.push({
    name: 'snapshot-detail',
    params: {
      tenantId: tenantId.value,
      projectId: projectId.value,
      artifactId: artifactId.value,
      commitId,
    },
  });
}
</script>

<template>
  <div class="page">
    <PageHeading
      :title="artifact?.display_name ?? artifactId"
      :description="`${projectId} / ${artifactId}`"
    >
      <template #actions>
        <el-button
          :icon="RefreshRight"
          :loading="artifactQuery.isFetching.value"
          @click="artifactQuery.refetch"
        >
          刷新
        </el-button>
      </template>
    </PageHeading>

    <ApiProblemAlert
      v-if="artifactQuery.error.value"
      :error="artifactQuery.error.value"
      :retrying="artifactQuery.isFetching.value"
      @retry="artifactQuery.refetch"
    />

    <section v-if="artifact" class="content-section resource-detail-shell">
      <el-tabs :model-value="activeTab" @tab-change="changeTab">
        <el-tab-pane label="Overview" name="overview">
          <dl class="definition-grid definition-grid--scope">
            <div>
              <dt>Tenant</dt>
              <dd>{{ artifact.tenant_id }}</dd>
            </div>
            <div>
              <dt>Project</dt>
              <dd>{{ artifact.project_id }}</dd>
            </div>
            <div>
              <dt>Artifact ID</dt>
              <dd>
                <code>{{ artifact.artifact_id }}</code>
              </dd>
            </div>
            <div>
              <dt>Resource version</dt>
              <dd>{{ artifact.resource_version }}</dd>
            </div>
            <div>
              <dt>Default ref</dt>
              <dd>
                <code>{{ artifact.default_ref }}</code>
              </dd>
            </div>
            <div>
              <dt>创建时间</dt>
              <dd>{{ formatTime(artifact.created_at_unix_ms) }}</dd>
            </div>
            <div>
              <dt>更新时间</dt>
              <dd>{{ formatTime(artifact.updated_at_unix_ms) }}</dd>
            </div>
            <div class="definition-grid__wide">
              <dt>描述</dt>
              <dd>{{ artifact.description ?? '—' }}</dd>
            </div>
          </dl>
        </el-tab-pane>

        <el-tab-pane label="Commits" name="commits">
          <ApiProblemAlert
            v-if="commitQuery.error.value"
            :error="commitQuery.error.value"
            :retrying="commitQuery.isFetching.value"
            @retry="commitQuery.refetch"
          />
          <el-skeleton v-if="commitQuery.isPending.value" :rows="6" animated />
          <template v-else-if="commitQuery.data.value">
            <div class="ref-strip">
              <span>Graph version {{ commitQuery.data.value.data.graph.graph_version }}</span>
              <el-tag
                v-for="refTip in commitQuery.data.value.data.graph.refs"
                :key="refTip.name"
                effect="plain"
              >
                {{ refTip.name.replace('refs/heads/', '') }} · {{ shortId(refTip.commit_id, 14) }}
              </el-tag>
            </div>
            <ol class="commit-tree" aria-label="Commit 图">
              <li v-for="node in commitNodes" :key="node.commit_id" class="commit-node">
                <span class="commit-node__dot" />
                <div class="commit-node__body">
                  <div class="commit-node__heading">
                    <strong>{{ node.message }}</strong>
                    <time>{{ formatTime(node.created_at_unix_ms) }}</time>
                  </div>
                  <div class="commit-node__meta">
                    <code>{{ node.commit_id }}</code>
                    <span v-if="node.parent_commit_id">
                      parent <code>{{ node.parent_commit_id }}</code>
                    </span>
                    <el-tag v-for="name in node.ref_names" :key="name" size="small" effect="plain">
                      {{ name.replace('refs/heads/', '').replace('refs/tags/', '') }}
                    </el-tag>
                  </div>
                </div>
              </li>
            </ol>
            <el-button
              v-if="nextCommitCursor"
              :loading="loadingMoreCommits"
              @click="loadMoreCommits"
            >
              加载更多历史
            </el-button>
          </template>
        </el-tab-pane>

        <el-tab-pane label="Playgrounds" name="playgrounds">
          <ApiProblemAlert
            v-if="playgroundQuery.error.value"
            :error="playgroundQuery.error.value"
            :retrying="playgroundQuery.isFetching.value"
            @retry="playgroundQuery.refetch"
          />
          <el-skeleton v-if="playgroundQuery.isPending.value" :rows="5" animated />
          <el-empty
            v-else-if="!playgroundQuery.data.value?.data.items.length"
            description="此 Artifact 暂无 Playground"
          />
          <div v-else class="relation-list">
            <button
              v-for="playground in playgroundQuery.data.value?.data.items"
              :key="playground.playground_id"
              type="button"
              @click="openPlayground(playground.playground_id)"
            >
              <span>
                <strong>{{ playground.display_name }}</strong>
                <code>{{ playground.playground_id }}</code>
              </span>
              <span class="relation-list__aside">
                <el-tag effect="plain">{{ playground.state }}</el-tag
                ><ArrowRight />
              </span>
            </button>
          </div>
        </el-tab-pane>

        <el-tab-pane label="Snapshots" name="snapshots">
          <ApiProblemAlert
            v-if="snapshotQuery.error.value"
            :error="snapshotQuery.error.value"
            :retrying="snapshotQuery.isFetching.value"
            @retry="snapshotQuery.refetch"
          />
          <el-skeleton v-if="snapshotQuery.isPending.value" :rows="5" animated />
          <el-empty
            v-else-if="!snapshotQuery.data.value?.data.items.length"
            description="此 Artifact 暂无 Snapshot"
          />
          <div v-else class="relation-list">
            <button
              v-for="snapshot in snapshotQuery.data.value?.data.items"
              :key="snapshot.commit_id"
              type="button"
              @click="openSnapshot(snapshot.commit_id)"
            >
              <span>
                <strong>{{ snapshot.message }}</strong>
                <code>{{ snapshot.commit_id }}</code>
              </span>
              <span class="relation-list__aside">
                <small>
                  {{ formatCount(snapshot.logical_file_count) }} files ·
                  {{ formatBytes(snapshot.logical_size_bytes) }}
                </small>
                <ArrowRight />
              </span>
            </button>
          </div>
        </el-tab-pane>
      </el-tabs>
    </section>

    <div v-else-if="artifactQuery.isPending.value" class="page-loading">
      <el-skeleton :rows="8" animated />
    </div>
  </div>
</template>
