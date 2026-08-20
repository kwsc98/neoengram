/// Current canonical repository and WorkspaceIndex format identity.
pub const WORKSPACE_INDEX_FORMAT_VERSION: u32 = 9;

/// Maximum number of mutations carried by one bounded [`IndexDeltaPage`](super::IndexDeltaPage).
pub const MAX_INDEX_MUTATIONS_PER_PAGE: usize = 4_096;
