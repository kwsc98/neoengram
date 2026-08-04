use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::ValueEnum;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{durability::sync_directory, AgentDaemonError, AgentDaemonResult};

const HEALTH_SCHEMA_VERSION: u16 = 1;
const HEALTH_FILE: &str = "runtime-health.json";
const HEALTH_LEASE_FILE: &str = "runtime-health.lock";
const MAX_HEALTH_BYTES: u64 = 4 * 1024;
const MAX_HEALTH_LEASE_BYTES: u64 = 1024;
const LIVE_STALE_AFTER_MS: u64 = 60_000;
const CLOCK_SKEW_MS: u64 = 60_000;
const LEASE_ACQUIRE_ATTEMPTS: usize = 20;
const LEASE_ACQUIRE_RETRY_DELAY: Duration = Duration::from_millis(10);
const RUN_ID_PREFIX: &str = "health-run-";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum HealthMode {
    Startup,
    Live,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeHealthPhase {
    Starting,
    Bootstrapping,
    PendingApproval,
    ApprovedWaitingCertificate,
    SessionReady,
    Rejected,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeHealthDocument {
    schema_version: u16,
    process_id: u32,
    run_id: String,
    phase: RuntimeHealthPhase,
    updated_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeHealthLeaseDocument {
    schema_version: u16,
    process_id: u32,
    run_id: String,
}

pub fn check_health(state_dir: impl AsRef<Path>, mode: HealthMode) -> AgentDaemonResult<()> {
    let state_dir = state_dir.as_ref();
    let mut lease_file = open_existing_regular_file(
        &state_dir.join(HEALTH_LEASE_FILE),
        MAX_HEALTH_LEASE_BYTES,
        "health lease",
    )?;
    let lease_before: RuntimeHealthLeaseDocument =
        read_bounded_json_from_file(&mut lease_file, MAX_HEALTH_LEASE_BYTES, "health lease")?;
    validate_identity(
        lease_before.schema_version,
        lease_before.process_id,
        &lease_before.run_id,
    )?;
    require_active_lease(&lease_file)?;

    let document: RuntimeHealthDocument = read_bounded_json(
        &state_dir.join(HEALTH_FILE),
        MAX_HEALTH_BYTES,
        "health state",
    )?;
    let lease_after: RuntimeHealthLeaseDocument =
        read_bounded_json_from_file(&mut lease_file, MAX_HEALTH_LEASE_BYTES, "health lease")?;
    validate_identity(
        document.schema_version,
        document.process_id,
        &document.run_id,
    )?;
    validate_identity(
        lease_after.schema_version,
        lease_after.process_id,
        &lease_after.run_id,
    )?;
    require_active_lease(&lease_file)?;
    if lease_before != lease_after
        || document.process_id != lease_after.process_id
        || document.run_id != lease_after.run_id
    {
        return Err(AgentDaemonError::Health(
            "health state does not belong to the active lease".into(),
        ));
    }

    validate_live_document(&document, now_unix_ms()?)?;
    if document.phase == RuntimeHealthPhase::Starting {
        return Err(AgentDaemonError::Health(
            "Agent startup initialization has not completed".into(),
        ));
    }

    match mode {
        HealthMode::Startup | HealthMode::Live => Ok(()),
        HealthMode::Ready if document.phase == RuntimeHealthPhase::SessionReady => Ok(()),
        HealthMode::Ready => Err(AgentDaemonError::Health(format!(
            "Agent is not ready while runtime phase is {:?}",
            document.phase
        ))),
    }
}

fn read_bounded_json<T>(path: &Path, limit: u64, description: &str) -> AgentDaemonResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    let mut file = open_existing_regular_file(path, limit, description)?;
    read_bounded_json_from_file(&mut file, limit, description)
}

fn read_bounded_json_from_file<T>(
    file: &mut File,
    limit: u64,
    description: &str,
) -> AgentDaemonResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    let metadata = file.metadata().map_err(|error| {
        AgentDaemonError::Health(format!("{description} metadata is unavailable: {error}"))
    })?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.seek(SeekFrom::Start(0)).map_err(|error| {
        AgentDaemonError::Health(format!("{description} cannot be read: {error}"))
    })?;
    Read::by_ref(file)
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            AgentDaemonError::Health(format!("{description} cannot be read: {error}"))
        })?;
    if bytes.len() as u64 > limit {
        return Err(AgentDaemonError::Health(format!(
            "{description} exceeds the size limit"
        )));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| AgentDaemonError::Health(format!("{description} is invalid")))
}

fn require_active_lease(lease_file: &File) -> AgentDaemonResult<()> {
    match FileExt::try_lock_exclusive(lease_file) {
        Ok(()) => {
            let _ = FileExt::unlock(lease_file);
            Err(AgentDaemonError::Health(
                "health lease is not owned by an active Agent process".into(),
            ))
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
        Err(error) => Err(AgentDaemonError::Health(format!(
            "health lease ownership cannot be verified: {error}"
        ))),
    }
}

#[derive(Debug)]
pub(crate) struct HealthReporter {
    state_dir: PathBuf,
    process_id: u32,
    run_id: String,
    phase: RuntimeHealthPhase,
    lease_file: File,
}

impl HealthReporter {
    pub(crate) fn acquire(state_dir: PathBuf) -> AgentDaemonResult<Self> {
        prepare_state_dir(&state_dir)?;
        let lease_path = state_dir.join(HEALTH_LEASE_FILE);
        reject_special_existing_path(&lease_path, "health lease")?;
        let mut lease_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lease_path)?;
        secure_file(&lease_file)?;
        acquire_lease(&lease_file)?;

        let process_id = std::process::id();
        let run_id = format!("{RUN_ID_PREFIX}{}", Uuid::new_v4().simple());
        let lease = RuntimeHealthLeaseDocument {
            schema_version: HEALTH_SCHEMA_VERSION,
            process_id,
            run_id: run_id.clone(),
        };
        let bytes = serde_json::to_vec(&lease)
            .map_err(|error| AgentDaemonError::Health(error.to_string()))?;
        lease_file.set_len(0)?;
        lease_file.seek(SeekFrom::Start(0))?;
        lease_file.write_all(&bytes)?;
        lease_file.sync_all()?;
        sync_directory(&state_dir)?;

        let reporter = Self {
            state_dir,
            process_id,
            run_id,
            phase: RuntimeHealthPhase::Starting,
            lease_file,
        };
        reporter.refresh()?;
        Ok(reporter)
    }

    pub(crate) fn set_phase(&mut self, phase: RuntimeHealthPhase) -> AgentDaemonResult<()> {
        self.phase = phase;
        self.refresh()
    }

    pub(crate) fn refresh(&self) -> AgentDaemonResult<()> {
        let document = RuntimeHealthDocument {
            schema_version: HEALTH_SCHEMA_VERSION,
            process_id: self.process_id,
            run_id: self.run_id.clone(),
            phase: self.phase,
            updated_at_unix_ms: now_unix_ms()?,
        };
        write_atomic(&self.state_dir, &document)
    }
}

impl Drop for HealthReporter {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lease_file);
    }
}

fn validate_live_document(
    document: &RuntimeHealthDocument,
    now_unix_ms: u64,
) -> AgentDaemonResult<()> {
    if document.updated_at_unix_ms > now_unix_ms.saturating_add(CLOCK_SKEW_MS) {
        return Err(AgentDaemonError::Health(
            "health state timestamp is in the future".into(),
        ));
    }
    if now_unix_ms.saturating_sub(document.updated_at_unix_ms) > LIVE_STALE_AFTER_MS {
        return Err(AgentDaemonError::Health("health state is stale".into()));
    }
    Ok(())
}

fn validate_identity(schema_version: u16, process_id: u32, run_id: &str) -> AgentDaemonResult<()> {
    let valid_run_id = run_id.strip_prefix(RUN_ID_PREFIX).is_some_and(|value| {
        value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
    });
    if schema_version != HEALTH_SCHEMA_VERSION || process_id == 0 || !valid_run_id {
        return Err(AgentDaemonError::Health(
            "health state has an unsupported identity".into(),
        ));
    }
    Ok(())
}

fn prepare_state_dir(state_dir: &Path) -> AgentDaemonResult<()> {
    match fs::symlink_metadata(state_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(AgentDaemonError::Health(
                "health state directory is not a regular directory".into(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(state_dir)?;
            let metadata = fs::symlink_metadata(state_dir)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(AgentDaemonError::Health(
                    "health state directory is not a regular directory".into(),
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }
    secure_directory(state_dir)?;
    Ok(())
}

fn acquire_lease(file: &File) -> AgentDaemonResult<()> {
    for attempt in 0..LEASE_ACQUIRE_ATTEMPTS {
        match FileExt::try_lock_exclusive(file) {
            Ok(()) => return Ok(()),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && attempt + 1 < LEASE_ACQUIRE_ATTEMPTS =>
            {
                thread::sleep(LEASE_ACQUIRE_RETRY_DELAY);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(AgentDaemonError::Health(
                    "another Agent process already owns the health lease".into(),
                ));
            }
            Err(error) => {
                return Err(AgentDaemonError::Health(format!(
                    "health lease cannot be acquired: {error}"
                )));
            }
        }
    }
    unreachable!("health lease acquisition loop always returns")
}

fn open_existing_regular_file(
    path: &Path,
    limit: u64,
    description: &str,
) -> AgentDaemonResult<File> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        AgentDaemonError::Health(format!("{description} is unavailable: {error}"))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit {
        return Err(AgentDaemonError::Health(format!(
            "{description} is not a bounded regular file"
        )));
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| {
            AgentDaemonError::Health(format!("{description} cannot be opened: {error}"))
        })
}

fn write_atomic(state_dir: &Path, document: &RuntimeHealthDocument) -> AgentDaemonResult<()> {
    let destination = state_dir.join(HEALTH_FILE);
    reject_special_existing_path(&destination, "health state")?;
    let temporary = state_dir.join(format!(
        ".{HEALTH_FILE}.{}.{}.tmp",
        document.process_id, document.run_id
    ));
    reject_special_existing_path(&temporary, "temporary health state")?;
    let bytes = serde_json::to_vec(document)
        .map_err(|error| AgentDaemonError::Health(error.to_string()))?;
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

fn reject_special_existing_path(path: &Path, description: &str) -> AgentDaemonResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(
            AgentDaemonError::Health(format!("{description} path is not a regular file")),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn secure_directory(path: &Path) -> AgentDaemonResult<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn secure_directory(_path: &Path) -> AgentDaemonResult<()> {
    Ok(())
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

fn now_unix_ms() -> AgentDaemonResult<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AgentDaemonError::Health("system clock precedes the Unix epoch".into()))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| AgentDaemonError::Health("system clock is out of range".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_and_live_require_an_active_non_starting_reporter() {
        let directory = tempfile::tempdir().unwrap();
        let mut reporter = HealthReporter::acquire(directory.path().to_path_buf()).unwrap();
        assert!(check_health(directory.path(), HealthMode::Startup).is_err());
        assert!(check_health(directory.path(), HealthMode::Live).is_err());

        reporter
            .set_phase(RuntimeHealthPhase::ApprovedWaitingCertificate)
            .unwrap();
        check_health(directory.path(), HealthMode::Startup).unwrap();
        check_health(directory.path(), HealthMode::Live).unwrap();
        assert!(check_health(directory.path(), HealthMode::Ready).is_err());

        reporter
            .set_phase(RuntimeHealthPhase::SessionReady)
            .unwrap();
        check_health(directory.path(), HealthMode::Ready).unwrap();

        drop(reporter);
        assert!(check_health(directory.path(), HealthMode::Startup).is_err());
        assert!(check_health(directory.path(), HealthMode::Live).is_err());
    }

    #[test]
    fn replacement_reporter_immediately_invalidates_the_previous_run() {
        let directory = tempfile::tempdir().unwrap();
        let mut first = HealthReporter::acquire(directory.path().to_path_buf()).unwrap();
        first
            .set_phase(RuntimeHealthPhase::PendingApproval)
            .unwrap();
        check_health(directory.path(), HealthMode::Live).unwrap();
        drop(first);

        let second = HealthReporter::acquire(directory.path().to_path_buf()).unwrap();
        assert!(check_health(directory.path(), HealthMode::Startup).is_err());
        assert!(check_health(directory.path(), HealthMode::Live).is_err());
        drop(second);
    }

    #[test]
    fn mismatched_health_and_lease_identity_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let mut reporter = HealthReporter::acquire(directory.path().to_path_buf()).unwrap();
        reporter
            .set_phase(RuntimeHealthPhase::PendingApproval)
            .unwrap();
        let path = directory.path().join(HEALTH_FILE);
        let mut document: RuntimeHealthDocument =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        document.process_id = document.process_id.saturating_add(1);
        fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
        assert!(check_health(directory.path(), HealthMode::Live).is_err());

        document.process_id = reporter.process_id;
        document.run_id = format!("{RUN_ID_PREFIX}{}", Uuid::new_v4().simple());
        fs::write(path, serde_json::to_vec(&document).unwrap()).unwrap();
        assert!(check_health(directory.path(), HealthMode::Live).is_err());
    }

    #[test]
    fn stale_and_unknown_health_documents_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let reporter = HealthReporter::acquire(directory.path().to_path_buf()).unwrap();
        let stale = RuntimeHealthDocument {
            schema_version: HEALTH_SCHEMA_VERSION,
            process_id: reporter.process_id,
            run_id: reporter.run_id.clone(),
            phase: RuntimeHealthPhase::PendingApproval,
            updated_at_unix_ms: now_unix_ms()
                .unwrap()
                .saturating_sub(LIVE_STALE_AFTER_MS + 1),
        };
        fs::write(
            directory.path().join(HEALTH_FILE),
            serde_json::to_vec(&stale).unwrap(),
        )
        .unwrap();
        assert!(check_health(directory.path(), HealthMode::Live).is_err());

        fs::write(
            directory.path().join(HEALTH_FILE),
            format!(
                r#"{{"schema_version":1,"process_id":{},"run_id":"{}","phase":"pending_approval","updated_at_unix_ms":1,"extra":true}}"#,
                reporter.process_id, reporter.run_id
            ),
        )
        .unwrap();
        assert!(check_health(directory.path(), HealthMode::Startup).is_err());
    }
}
