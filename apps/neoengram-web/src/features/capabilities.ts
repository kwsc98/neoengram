export function supportsArtifactCatalog(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('artifact_catalog') ?? false;
}

export function supportsArtifactCommitGraph(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('artifact_commit_graph') ?? false;
}

export function supportsArtifactCommitDiff(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('artifact_commit_diff') ?? false;
}

export function supportsCommitMaterializationV2(
  capabilities: readonly string[] | undefined,
): boolean {
  return capabilities?.includes('commit_materialization_v2') ?? false;
}

export function supportsWorkspaceMaterialize(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('workspace_materialize') ?? false;
}

export function supportsWorkspaceBrowser(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('workspace_browser') ?? false;
}

export function supportsWorkspacePreCommit(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('workspace_precommit') ?? false;
}

export function supportsSnapshotMaterialize(capabilities: readonly string[] | undefined): boolean {
  return supportsCommitMaterializationV2(capabilities);
}

export function supportsCommitLayoutSelection(
  capabilities: readonly string[] | undefined,
): boolean {
  return capabilities?.includes('commit_layout_selection_v2') ?? false;
}

export type SnapshotDeliveryCapabilityMode = 'fuse' | 'copy' | 'hardlink';

export function supportsSnapshotDeliveryMode(
  capabilities: readonly string[] | undefined,
  mode: SnapshotDeliveryCapabilityMode,
): boolean {
  return capabilities?.includes(`snapshot_delivery_${mode}_v2`) ?? false;
}

export function supportsSnapshotDelivery(capabilities: readonly string[] | undefined): boolean {
  return (['fuse', 'copy', 'hardlink'] as const).some((mode) =>
    supportsSnapshotDeliveryMode(capabilities, mode),
  );
}

export function supportsS3ReadonlyAccessPoint(
  capabilities: readonly string[] | undefined,
): boolean {
  return capabilities?.includes('s3_readonly_access_point') ?? false;
}

export function supportsResourceLifecycle(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('resource_lifecycle_v1') ?? false;
}
