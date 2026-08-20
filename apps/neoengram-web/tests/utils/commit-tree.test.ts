import { describe, expect, it } from 'vitest';

import type { CommitNode } from '@/api/types';
import { buildCommitTree } from '@/utils/commit-tree';

function commit(
  commitId: string,
  parentCommitId: string | undefined,
  createdAtUnixMs: string,
): CommitNode {
  return {
    commit_id: commitId,
    ...(parentCommitId ? { parent_commit_id: parentCommitId } : {}),
    message: commitId,
    tag_names: [],
    data_layout: 'fast_cdc',
    created_at_unix_ms: createdAtUnixMs,
  };
}

describe('buildCommitTree', () => {
  it('keeps a child visible while its parent is on a later page', () => {
    const tree = buildCommitTree([commit('child', 'parent', '2')]);

    expect(tree.loadedCount).toBe(1);
    expect(tree.tipCount).toBe(1);
    expect(tree.roots).toHaveLength(1);
    expect(tree.roots[0]?.node.commit_id).toBe('child');
    expect(tree.roots[0]?.parentLoaded).toBe(false);
  });

  it('deduplicates a Commit repeated across overlapping pages', () => {
    const repeated = commit('child', 'parent', '2');
    const tree = buildCommitTree([repeated, repeated, commit('parent', undefined, '1')]);

    expect(tree.loadedCount).toBe(2);
    expect(tree.tipCount).toBe(1);
    expect(tree.roots).toHaveLength(1);
    expect(tree.roots[0]?.children.map((item) => item.node.commit_id)).toEqual(['child']);
  });

  it('breaks malformed parent cycles into finite roots', () => {
    const tree = buildCommitTree([
      commit('commit-a', 'commit-b', '2'),
      commit('commit-b', 'commit-a', '1'),
    ]);

    expect(tree.loadedCount).toBe(2);
    expect(tree.tipCount).toBe(2);
    expect(tree.roots.map((item) => item.node.commit_id)).toEqual(['commit-a', 'commit-b']);
    expect(tree.roots.every((item) => item.children.length === 0)).toBe(true);
  });
});
