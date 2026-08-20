import { describe, expect, it } from 'vitest';

import {
  supportsArtifactCatalog,
  supportsArtifactCommitGraph,
  supportsCommitLayoutSelection,
  supportsPlaygroundBrowser,
  supportsPlaygroundMaterialize,
  supportsPlaygroundPreCommit,
  supportsSnapshotMaterialize,
  supportsSnapshotDelivery,
  supportsSnapshotDeliveryMode,
} from '@/features/capabilities';

describe('server capability gates', () => {
  it('exposes the Artifact catalog without enabling an aggregate browser', () => {
    expect(supportsArtifactCatalog(['artifact_catalog'])).toBe(true);
    expect(supportsArtifactCommitGraph(['artifact_catalog'])).toBe(false);
    expect(supportsSnapshotMaterialize(['artifact_catalog'])).toBe(false);
  });

  it('rejects unsupported aggregate capabilities', () => {
    const removed = ['aggregate_browser'];
    expect(supportsArtifactCatalog(removed)).toBe(false);
    expect(supportsArtifactCommitGraph(removed)).toBe(false);
    expect(supportsPlaygroundMaterialize(removed)).toBe(false);
    expect(supportsPlaygroundBrowser(removed)).toBe(false);
    expect(supportsPlaygroundPreCommit(removed)).toBe(false);
    expect(supportsSnapshotMaterialize(removed)).toBe(false);
  });

  it('gates workspace surfaces independently when the server advertises granular capabilities', () => {
    expect(supportsArtifactCommitGraph(['artifact_commit_graph'])).toBe(true);
    expect(supportsPlaygroundMaterialize(['playground_materialize'])).toBe(true);
    expect(supportsPlaygroundBrowser(['playground_materialize'])).toBe(false);
    expect(supportsPlaygroundBrowser(['playground_browser'])).toBe(true);
    expect(supportsPlaygroundPreCommit(['playground_precommit'])).toBe(true);
    expect(supportsPlaygroundMaterialize(['playground_precommit'])).toBe(false);
    expect(supportsSnapshotMaterialize(['snapshot_materialize'])).toBe(true);
  });

  it('keeps both resource families hidden when neither capability is declared', () => {
    expect(supportsArtifactCatalog(['managed_add'])).toBe(false);
    expect(supportsArtifactCommitGraph(undefined)).toBe(false);
    expect(supportsPlaygroundMaterialize(undefined)).toBe(false);
    expect(supportsPlaygroundBrowser(undefined)).toBe(false);
    expect(supportsPlaygroundPreCommit(undefined)).toBe(false);
    expect(supportsSnapshotMaterialize(undefined)).toBe(false);
  });

  it('gates Commit layout selection and each Snapshot delivery mode independently', () => {
    expect(supportsCommitLayoutSelection(['commit_layout_selection_v2'])).toBe(true);
    expect(supportsCommitLayoutSelection(['snapshot_delivery_fuse_v2'])).toBe(false);

    expect(supportsSnapshotDeliveryMode(['snapshot_delivery_fuse_v2'], 'fuse')).toBe(true);
    expect(supportsSnapshotDeliveryMode(['snapshot_delivery_fuse_v2'], 'copy')).toBe(false);
    expect(supportsSnapshotDeliveryMode(['snapshot_delivery_copy_v2'], 'copy')).toBe(true);
    expect(supportsSnapshotDeliveryMode(['snapshot_delivery_copy_v2'], 'hardlink')).toBe(false);
    expect(supportsSnapshotDeliveryMode(['snapshot_delivery_hardlink_v2'], 'hardlink')).toBe(true);
    expect(supportsSnapshotDeliveryMode(['snapshot_delivery_hardlink_v2'], 'fuse')).toBe(false);

    expect(
      supportsSnapshotDelivery([
        'snapshot_delivery_fuse_v2',
        'snapshot_delivery_copy_v2',
        'snapshot_delivery_hardlink_v2',
      ]),
    ).toBe(true);
    expect(supportsSnapshotDelivery(['commit_layout_selection_v2'])).toBe(false);
    expect(supportsSnapshotDelivery(undefined)).toBe(false);
  });
});
