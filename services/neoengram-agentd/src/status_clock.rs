use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{durability::sync_directory, AgentDaemonError, AgentDaemonResult};

const STATUS_CLOCK_SCHEMA_VERSION: u16 = 1;
const STATUS_CLOCK_FILE: &str = "bootstrap-status-clock.json";
const MAX_STATUS_CLOCK_BYTES: u64 = 1024;

/// Crash-safe monotonic timestamp source for signed bootstrap status requests.
#[derive(Debug)]
pub(crate) struct StatusTimestampClock {
    state_dir: PathBuf,
    last_signed_at_unix_ms: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusClockDocument {
    schema_version: u16,
    last_signed_at_unix_ms: u64,
}

impl StatusTimestampClock {
    pub(crate) fn open(state_dir: impl AsRef<Path>) -> AgentDaemonResult<Self> {
        let state_dir = state_dir.as_ref().to_path_buf();
        let path = state_dir.join(STATUS_CLOCK_FILE);
        let last_signed_at_unix_ms = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink()
                    || !metadata.is_file()
                    || metadata.len() > MAX_STATUS_CLOCK_BYTES
                {
                    return Err(status_clock_error(
                        "status timestamp watermark is not a bounded regular file",
                    ));
                }
                let mut bytes = Vec::with_capacity(metadata.len() as usize);
                File::open(&path)?
                    .take(MAX_STATUS_CLOCK_BYTES + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MAX_STATUS_CLOCK_BYTES {
                    return Err(status_clock_error(
                        "status timestamp watermark exceeds the size limit",
                    ));
                }
                let document: StatusClockDocument = serde_json::from_slice(&bytes)
                    .map_err(|_| status_clock_error("status timestamp watermark is invalid"))?;
                if document.schema_version != STATUS_CLOCK_SCHEMA_VERSION
                    || document.last_signed_at_unix_ms == 0
                {
                    return Err(status_clock_error(
                        "status timestamp watermark has an unsupported identity",
                    ));
                }
                Some(document.last_signed_at_unix_ms)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            state_dir,
            last_signed_at_unix_ms,
        })
    }

    /// Reserves and durably commits the next timestamp before it can be sent to the server.
    pub(crate) fn next(&mut self, wall_clock_unix_ms: u64) -> AgentDaemonResult<u64> {
        let next = match self.last_signed_at_unix_ms {
            Some(previous) => previous
                .checked_add(1)
                .ok_or_else(|| status_clock_error("status timestamp watermark overflowed"))?
                .max(wall_clock_unix_ms),
            None => wall_clock_unix_ms.max(1),
        };
        write_atomic(&self.state_dir, next)?;
        self.last_signed_at_unix_ms = Some(next);
        Ok(next)
    }
}

fn write_atomic(state_dir: &Path, last_signed_at_unix_ms: u64) -> AgentDaemonResult<()> {
    let destination = state_dir.join(STATUS_CLOCK_FILE);
    reject_special_existing_path(&destination)?;
    let temporary = state_dir.join(format!(".{STATUS_CLOCK_FILE}.{}.tmp", std::process::id()));
    reject_special_existing_path(&temporary)?;
    let bytes = serde_json::to_vec(&StatusClockDocument {
        schema_version: STATUS_CLOCK_SCHEMA_VERSION,
        last_signed_at_unix_ms,
    })
    .map_err(|_| status_clock_error("failed to encode status timestamp watermark"))?;
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temporary)?;
    secure_file(&file)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, &destination)?;
    sync_directory(state_dir)?;
    Ok(())
}

fn reject_special_existing_path(path: &Path) -> AgentDaemonResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(
            status_clock_error("status timestamp watermark path is not a regular file"),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn secure_file(file: &File) -> AgentDaemonResult<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn secure_file(_file: &File) -> AgentDaemonResult<()> {
    Ok(())
}

fn status_clock_error(message: impl Into<String>) -> AgentDaemonError {
    AgentDaemonError::StatusClock(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_strictly_monotonic_across_same_millisecond_and_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let mut clock = StatusTimestampClock::open(directory.path()).unwrap();
        assert_eq!(clock.next(1_000).unwrap(), 1_000);
        assert_eq!(clock.next(1_000).unwrap(), 1_001);
        assert_eq!(clock.next(999).unwrap(), 1_002);
    }

    #[test]
    fn restart_advances_the_durable_watermark_even_when_clock_moves_back() {
        let directory = tempfile::tempdir().unwrap();
        let mut first = StatusTimestampClock::open(directory.path()).unwrap();
        assert_eq!(first.next(5_000).unwrap(), 5_000);
        drop(first);

        let mut restarted = StatusTimestampClock::open(directory.path()).unwrap();
        assert_eq!(restarted.next(4_900).unwrap(), 5_001);
    }

    #[test]
    fn invalid_or_overflowed_watermark_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join(STATUS_CLOCK_FILE),
            br#"{"schema_version":1,"last_signed_at_unix_ms":1,"extra":true}"#,
        )
        .unwrap();
        assert!(StatusTimestampClock::open(directory.path()).is_err());

        fs::write(
            directory.path().join(STATUS_CLOCK_FILE),
            serde_json::to_vec(&StatusClockDocument {
                schema_version: STATUS_CLOCK_SCHEMA_VERSION,
                last_signed_at_unix_ms: u64::MAX,
            })
            .unwrap(),
        )
        .unwrap();
        let mut clock = StatusTimestampClock::open(directory.path()).unwrap();
        assert!(clock.next(1).is_err());
    }
}
