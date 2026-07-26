/// Current canonical Index snapshot format version.
pub const INDEX_FORMAT_VERSION: u32 = 8;

/// Maximum number of mutations carried by one bounded [`IndexDeltaPage`](super::IndexDeltaPage).
pub const MAX_INDEX_MUTATIONS_PER_PAGE: usize = 4_096;
