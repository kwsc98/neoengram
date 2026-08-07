export function supportsArtifactCatalog(capabilities: readonly string[] | undefined): boolean {
  return (
    capabilities?.includes('artifact_catalog') ||
    capabilities?.includes('resource_browser') ||
    false
  );
}

/**
 * `resource_browser` was the original all-or-nothing capability. Keep it as
 * an alias while allowing the workspace surface to be rolled out in pieces.
 */
export function supportsResourceBrowser(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('resource_browser') ?? false;
}

export function supportsArtifactCommitGraph(capabilities: readonly string[] | undefined): boolean {
  return Boolean(
    capabilities?.includes('artifact_commit_graph') || capabilities?.includes('resource_browser'),
  );
}

export function supportsPlaygroundMaterialize(
  capabilities: readonly string[] | undefined,
): boolean {
  return Boolean(
    capabilities?.includes('playground_materialize') || capabilities?.includes('resource_browser'),
  );
}

export function supportsPlaygroundBrowser(capabilities: readonly string[] | undefined): boolean {
  return Boolean(
    capabilities?.includes('playground_browser') || capabilities?.includes('resource_browser'),
  );
}

export function supportsPlaygroundPreCommit(capabilities: readonly string[] | undefined): boolean {
  return Boolean(
    capabilities?.includes('playground_precommit') || capabilities?.includes('resource_browser'),
  );
}

export function supportsSnapshotMaterialize(capabilities: readonly string[] | undefined): boolean {
  return capabilities?.includes('snapshot_materialize') ?? false;
}
