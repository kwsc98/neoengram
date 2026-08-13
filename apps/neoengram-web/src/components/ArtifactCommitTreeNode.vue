<script setup lang="ts">
import { Files } from '@element-plus/icons-vue';

import type { CommitTreeItem } from '@/utils/commit-tree';
import { commitTagNames } from '@/utils/commit';
import { formatTime } from '@/utils/format';

defineOptions({ name: 'ArtifactCommitTreeNode' });

defineProps<{
  item: CommitTreeItem;
  headCommitId: string | undefined;
  depth: number;
}>();

const emit = defineEmits<{
  select: [commitId: string];
}>();
</script>

<template>
  <li class="commit-branch" :class="{ 'commit-branch--has-children': item.children.length > 0 }">
    <div
      class="commit-node"
      :class="{
        'commit-node--head': item.node.commit_id === headCommitId,
        'commit-node--tip': item.isTip,
      }"
      :data-commit-id="item.node.commit_id"
      :data-parent-commit-id="item.node.parent_commit_id"
      :data-depth="depth"
    >
      <span class="commit-node__rail" aria-hidden="true">
        <span class="commit-node__dot" />
      </span>
      <article class="commit-node__body">
        <div class="commit-node__heading">
          <span class="commit-node__title">
            <strong>{{ item.node.message }}</strong>
            <span class="commit-node__labels">
              <el-tag
                v-if="item.node.commit_id === headCommitId"
                size="small"
                type="success"
                effect="dark"
              >
                默认 HEAD
              </el-tag>
              <el-tag v-if="item.isTip" size="small" effect="plain">分支末端</el-tag>
              <el-tag v-if="!item.node.parent_commit_id" size="small" type="info" effect="plain">
                根提交
              </el-tag>
            </span>
          </span>
          <span class="commit-node__actions">
            <time>{{ formatTime(item.node.created_at_unix_ms) }}</time>
            <el-button
              text
              type="primary"
              :icon="Files"
              @click="emit('select', item.node.commit_id)"
            >
              详情与 Diff
            </el-button>
          </span>
        </div>
        <p v-if="item.node.description" class="commit-node__description">
          {{ item.node.description }}
        </p>
        <div class="commit-node__meta">
          <span
            >Commit <code>{{ item.node.commit_id }}</code></span
          >
          <span v-if="item.node.parent_commit_id">
            Parent <code>{{ item.node.parent_commit_id }}</code>
          </span>
          <el-tag
            v-if="item.node.parent_commit_id && !item.parentLoaded"
            size="small"
            type="warning"
            effect="plain"
          >
            父提交未加载
          </el-tag>
          <el-tag
            v-for="tagName in commitTagNames(item.node.tag_names)"
            :key="tagName"
            size="small"
            effect="plain"
          >
            {{ tagName }}
          </el-tag>
        </div>
      </article>
    </div>

    <ol
      v-if="item.children.length"
      class="commit-tree__children"
      :class="{ 'commit-tree__children--linear': item.children.length === 1 }"
    >
      <ArtifactCommitTreeNode
        v-for="child in item.children"
        :key="child.node.commit_id"
        :item="child"
        :head-commit-id="headCommitId"
        :depth="depth + 1"
        @select="emit('select', $event)"
      />
    </ol>
  </li>
</template>
