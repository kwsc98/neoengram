import { describe, expect, it } from 'vitest';

import {
  supportsArtifactCatalog,
  supportsArtifactCommitGraph,
  supportsPlaygroundBrowser,
  supportsPlaygroundMaterialize,
  supportsPlaygroundPreCommit,
  supportsResourceBrowser,
  supportsSnapshotMaterialize,
} from '@/features/capabilities';

describe('server capability gates', () => {
  it('exposes the Artifact catalog without enabling the full resource browser', () => {
    expect(supportsArtifactCatalog(['artifact_catalog'])).toBe(true);
    expect(supportsArtifactCommitGraph(['artifact_catalog'])).toBe(false);
    expect(supportsResourceBrowser(['artifact_catalog'])).toBe(false);
    expect(supportsSnapshotMaterialize(['artifact_catalog'])).toBe(false);
  });

  it('keeps resource_browser backward compatible with the Artifact catalog', () => {
    expect(supportsArtifactCatalog(['resource_browser'])).toBe(true);
    expect(supportsArtifactCommitGraph(['resource_browser'])).toBe(true);
    expect(supportsResourceBrowser(['resource_browser'])).toBe(true);
    expect(supportsPlaygroundMaterialize(['resource_browser'])).toBe(true);
    expect(supportsPlaygroundBrowser(['resource_browser'])).toBe(true);
    expect(supportsPlaygroundPreCommit(['resource_browser'])).toBe(true);
    expect(supportsSnapshotMaterialize(['resource_browser'])).toBe(false);
  });

  it('gates workspace surfaces independently when the server advertises granular capabilities', () => {
    expect(supportsArtifactCommitGraph(['artifact_commit_graph'])).toBe(true);
    expect(supportsResourceBrowser(['artifact_commit_graph'])).toBe(false);
    expect(supportsPlaygroundMaterialize(['playground_materialize'])).toBe(true);
    expect(supportsPlaygroundBrowser(['playground_materialize'])).toBe(false);
    expect(supportsPlaygroundBrowser(['playground_browser'])).toBe(true);
    expect(supportsPlaygroundPreCommit(['playground_precommit'])).toBe(true);
    expect(supportsPlaygroundMaterialize(['playground_precommit'])).toBe(false);
    expect(supportsSnapshotMaterialize(['snapshot_materialize'])).toBe(true);
    expect(supportsResourceBrowser(['snapshot_materialize'])).toBe(false);
  });

  it('keeps both resource families hidden when neither capability is declared', () => {
    expect(supportsArtifactCatalog(['managed_add'])).toBe(false);
    expect(supportsArtifactCommitGraph(undefined)).toBe(false);
    expect(supportsResourceBrowser(undefined)).toBe(false);
    expect(supportsPlaygroundMaterialize(undefined)).toBe(false);
    expect(supportsPlaygroundBrowser(undefined)).toBe(false);
    expect(supportsPlaygroundPreCommit(undefined)).toBe(false);
    expect(supportsSnapshotMaterialize(undefined)).toBe(false);
  });
});
