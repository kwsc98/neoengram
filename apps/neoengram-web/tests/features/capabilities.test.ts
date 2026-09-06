import { describe, expect, it } from 'vitest';

import {
  supportsArtifactCatalog,
  supportsArtifactCommitDiff,
  supportsArtifactCommitGraph,
  supportsCommitMaterializationV2,
  supportsCommitLayoutSelection,
  supportsWorkspaceBrowser,
  supportsWorkspaceMaterialize,
  supportsWorkspacePreCommit,
  supportsSnapshotMaterialize,
  supportsSnapshotDelivery,
  supportsSnapshotDeliveryMode,
} from '@/features/capabilities';

describe('server capability gates', () => {
  it('exposes the Artifact catalog without enabling an aggregate browser', () => {
    expect(supportsArtifactCatalog(['artifact_catalog'])).toBe(true);
    expect(supportsArtifactCommitGraph(['artifact_catalog'])).toBe(false);
    expect(supportsArtifactCommitDiff(['artifact_catalog'])).toBe(false);
    expect(supportsCommitMaterializationV2(['artifact_catalog'])).toBe(false);
    expect(supportsSnapshotMaterialize(['artifact_catalog'])).toBe(false);
  });

  it('rejects unsupported aggregate capabilities', () => {
    const removed = ['aggregate_browser'];
    expect(supportsArtifactCatalog(removed)).toBe(false);
    expect(supportsArtifactCommitGraph(removed)).toBe(false);
    expect(supportsWorkspaceMaterialize(removed)).toBe(false);
    expect(supportsWorkspaceBrowser(removed)).toBe(false);
    expect(supportsWorkspacePreCommit(removed)).toBe(false);
    expect(supportsSnapshotMaterialize(removed)).toBe(false);
  });

  it('gates workspace surfaces independently when the server advertises granular capabilities', () => {
    expect(supportsArtifactCommitGraph(['artifact_commit_graph'])).toBe(true);
    expect(supportsArtifactCommitDiff(['artifact_commit_graph'])).toBe(false);
    expect(supportsArtifactCommitDiff(['artifact_commit_diff'])).toBe(true);
    expect(supportsCommitMaterializationV2(['artifact_commit_diff'])).toBe(false);
    expect(supportsCommitMaterializationV2(['commit_materialization_v2'])).toBe(true);
    expect(supportsWorkspaceMaterialize(['workspace_materialize'])).toBe(true);
    expect(supportsWorkspaceBrowser(['workspace_materialize'])).toBe(false);
    expect(supportsWorkspaceBrowser(['workspace_browser'])).toBe(true);
    expect(supportsWorkspacePreCommit(['workspace_precommit'])).toBe(true);
    expect(supportsWorkspaceMaterialize(['workspace_precommit'])).toBe(false);
    expect(supportsSnapshotMaterialize(['commit_materialization_v2'])).toBe(true);
  });

  it('keeps both resource families hidden when neither capability is declared', () => {
    expect(supportsArtifactCatalog(['managed_add'])).toBe(false);
    expect(supportsArtifactCommitGraph(undefined)).toBe(false);
    expect(supportsArtifactCommitDiff(undefined)).toBe(false);
    expect(supportsCommitMaterializationV2(undefined)).toBe(false);
    expect(supportsWorkspaceMaterialize(undefined)).toBe(false);
    expect(supportsWorkspaceBrowser(undefined)).toBe(false);
    expect(supportsWorkspacePreCommit(undefined)).toBe(false);
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
