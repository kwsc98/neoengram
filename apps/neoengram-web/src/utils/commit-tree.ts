import type { CommitNode } from '@/api/types';

export interface CommitTreeItem {
  node: CommitNode;
  children: CommitTreeItem[];
  parentLoaded: boolean;
  isTip: boolean;
}

export interface CommitTreeView {
  roots: CommitTreeItem[];
  loadedCount: number;
  tipCount: number;
}

function compareCanonicalU64Descending(left: string, right: string): number {
  if (left.length !== right.length) return right.length - left.length;
  return right.localeCompare(left);
}

function compareTreeItems(left: CommitTreeItem, right: CommitTreeItem): number {
  const byCreatedAt = compareCanonicalU64Descending(
    left.node.created_at_unix_ms,
    right.node.created_at_unix_ms,
  );
  return byCreatedAt || left.node.commit_id.localeCompare(right.node.commit_id);
}

function hasParentCycle(item: CommitTreeItem, byId: ReadonlyMap<string, CommitTreeItem>): boolean {
  const visited = new Set([item.node.commit_id]);
  let parentId = item.node.parent_commit_id;

  while (parentId) {
    if (visited.has(parentId)) return true;
    visited.add(parentId);
    parentId = byId.get(parentId)?.node.parent_commit_id;
  }
  return false;
}

export function buildCommitTree(nodes: readonly CommitNode[]): CommitTreeView {
  const byId = new Map<string, CommitTreeItem>();

  for (const node of nodes) {
    if (byId.has(node.commit_id)) continue;
    byId.set(node.commit_id, {
      node,
      children: [],
      parentLoaded: false,
      isTip: true,
    });
  }

  const roots: CommitTreeItem[] = [];
  for (const item of byId.values()) {
    const parent = item.node.parent_commit_id ? byId.get(item.node.parent_commit_id) : undefined;
    if (!parent || hasParentCycle(item, byId)) {
      roots.push(item);
      continue;
    }

    item.parentLoaded = true;
    parent.children.push(item);
    parent.isTip = false;
  }

  for (const item of byId.values()) item.children.sort(compareTreeItems);
  roots.sort(compareTreeItems);

  return {
    roots,
    loadedCount: byId.size,
    tipCount: [...byId.values()].filter((item) => item.isTip).length,
  };
}
