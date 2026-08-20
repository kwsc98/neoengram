#[cfg(unix)]
use std::fs::File;
use std::path::Path;

use crate::AgentDaemonResult;

/// Flushes an atomically updated directory entry where the platform exposes a portable barrier.
#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> AgentDaemonResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Rust cannot portably open a directory for synchronization on Windows.
#[cfg(not(unix))]
pub(crate) fn sync_directory(_path: &Path) -> AgentDaemonResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_sync_has_a_supported_platform_path() {
        let directory = tempfile::tempdir().unwrap();
        sync_directory(directory.path()).unwrap();
    }
}
