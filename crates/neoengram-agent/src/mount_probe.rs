use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    sync::atomic::{AtomicU64, Ordering},
};

use neoengram_core::ContentDigest;
use neoengram_protocol::{
    AgentBootstrapProbe, AgentMountIdentityDigest, Extensions, MountAccessMode, ResourceHealth,
    UnixMillis, VolumeMarkerId,
};

use crate::{AgentError, AgentErrorCode, AgentResult};

/// Fixed marker name provisioned at the root of every managed data Volume.
pub const VOLUME_MARKER_FILE_NAME: &str = ".neoengram-volume-marker";

#[cfg(unix)]
static PROBE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Immutable inputs for a local mount-readiness observation.
#[derive(Clone, PartialEq, Eq)]
pub struct FilesystemMountProbeConfig {
    pub mount_root: PathBuf,
    pub expected_volume_marker: VolumeMarkerId,
    pub desired_access_mode: MountAccessMode,
    pub hard_minimum_free_bytes: u64,
    pub ready_minimum_free_bytes: u64,
}

impl std::fmt::Debug for FilesystemMountProbeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FilesystemMountProbeConfig")
            .field(
                "mount_root_configured",
                &!self.mount_root.as_os_str().is_empty(),
            )
            .field("expected_volume_marker", &self.expected_volume_marker)
            .field("desired_access_mode", &self.desired_access_mode)
            .field("free_space_thresholds", &"configured")
            .finish()
    }
}

impl FilesystemMountProbeConfig {
    pub fn validate(&self) -> AgentResult<()> {
        if !self.mount_root.is_absolute()
            || self
                .mount_root
                .components()
                .any(|component| component == Component::ParentDir)
        {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "mount root must be an absolute normalized path",
            ));
        }
        if self.desired_access_mode != MountAccessMode::ReadWrite {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "0.0.1 mount readiness probes require read-write access",
            ));
        }
        if self.ready_minimum_free_bytes < self.hard_minimum_free_bytes {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "ready free-space threshold must not be below the hard minimum",
            ));
        }
        Ok(())
    }

    /// Probes Linux mount state. Other platforms return a sanitized unsupported observation.
    #[must_use]
    pub fn probe(&self) -> FilesystemMountObservation {
        if self.validate().is_err() {
            return FilesystemMountObservation::unavailable(MountProbeCondition::InvalidConfig);
        }
        probe_platform(self)
    }
}

/// Explicitly opted-in probe inputs for a local development directory.
///
/// Unlike [`FilesystemMountProbeConfig`], this probe treats the configured directory itself as
/// the storage boundary. Callers must additionally constrain the Agent transport to a local
/// development endpoint before using it.
#[derive(Clone, PartialEq, Eq)]
pub struct DevelopmentDirectoryMountProbeConfig {
    pub directory_root: PathBuf,
    pub expected_volume_marker: VolumeMarkerId,
    pub volume_descriptor_digest: ContentDigest,
    pub hard_minimum_free_bytes: u64,
    pub ready_minimum_free_bytes: u64,
}

impl std::fmt::Debug for DevelopmentDirectoryMountProbeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DevelopmentDirectoryMountProbeConfig")
            .field(
                "directory_root_configured",
                &!self.directory_root.as_os_str().is_empty(),
            )
            .field("expected_volume_marker", &self.expected_volume_marker)
            .field("free_space_thresholds", &"configured")
            .finish()
    }
}

impl DevelopmentDirectoryMountProbeConfig {
    pub fn validate(&self) -> AgentResult<()> {
        self.filesystem_config().validate()
    }

    /// Probes an ordinary directory for local development use only.
    #[must_use]
    pub fn probe(&self) -> FilesystemMountObservation {
        if self.validate().is_err() {
            return FilesystemMountObservation::unavailable(MountProbeCondition::InvalidConfig);
        }
        probe_development_directory_platform(self)
    }

    fn filesystem_config(&self) -> FilesystemMountProbeConfig {
        FilesystemMountProbeConfig {
            mount_root: self.directory_root.clone(),
            expected_volume_marker: self.expected_volume_marker.clone(),
            desired_access_mode: MountAccessMode::ReadWrite,
            hard_minimum_free_bytes: self.hard_minimum_free_bytes,
            ready_minimum_free_bytes: self.ready_minimum_free_bytes,
        }
    }
}

/// Local probe details. The fingerprint remains internal and is redacted from formatting.
#[derive(Clone, PartialEq, Eq)]
pub struct FilesystemMountObservation {
    pub observed_volume_marker: Option<VolumeMarkerId>,
    pub marker_matches: bool,
    pub mount_boundary_detected: bool,
    pub access_mode: Option<MountAccessMode>,
    pub rename_supported: bool,
    pub fsync_supported: bool,
    pub health: ResourceHealth,
    pub available_bytes: Option<u64>,
    pub mount_identity_digest: Option<AgentMountIdentityDigest>,
    pub condition: MountProbeCondition,
}

impl std::fmt::Debug for FilesystemMountObservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FilesystemMountObservation")
            .field("observed_volume_marker", &self.observed_volume_marker)
            .field("marker_matches", &self.marker_matches)
            .field("mount_boundary_detected", &self.mount_boundary_detected)
            .field("access_mode", &self.access_mode)
            .field("rename_supported", &self.rename_supported)
            .field("fsync_supported", &self.fsync_supported)
            .field("health", &self.health)
            .field("available_bytes_present", &self.available_bytes.is_some())
            .field(
                "mount_identity_digest",
                &self.mount_identity_digest.map(|_| "[REDACTED]"),
            )
            .field("condition", &self.condition)
            .finish()
    }
}

impl FilesystemMountObservation {
    const fn unavailable(condition: MountProbeCondition) -> Self {
        Self {
            observed_volume_marker: None,
            marker_matches: false,
            mount_boundary_detected: false,
            access_mode: None,
            rename_supported: false,
            fsync_supported: false,
            health: ResourceHealth::Unavailable,
            available_bytes: None,
            mount_identity_digest: None,
            condition,
        }
    }

    /// Converts local evidence to the bounded bootstrap DTO without physical storage details.
    pub fn to_bootstrap_probe(
        &self,
        observed_at_unix_ms: UnixMillis,
    ) -> AgentResult<AgentBootstrapProbe> {
        let mount_identity_digest = self.mount_identity_digest.ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::MountUnavailable,
                "mount identity is unavailable for bootstrap",
            )
        })?;
        Ok(AgentBootstrapProbe {
            observed_volume_marker: self.observed_volume_marker.clone(),
            marker_matches: self.marker_matches,
            mount_boundary_detected: self.mount_boundary_detected,
            mount_identity_digest,
            access_mode: self.access_mode,
            rename_supported: self.rename_supported,
            fsync_supported: self.fsync_supported,
            health: self.health,
            observed_at_unix_ms,
            extensions: Extensions::new(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountProbeCondition {
    Ready,
    LowFreeSpace,
    BelowHardFreeSpaceMinimum,
    MarkerUnavailable,
    MarkerMismatch,
    NotMountBoundary,
    AccessModeUnavailable,
    AccessModeMismatch,
    ReadWriteProbeFailed,
    FilesystemMetadataUnavailable,
    UnsupportedPlatform,
    InvalidConfig,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Clone, PartialEq, Eq)]
struct MountInfoEntry {
    mount_point: PathBuf,
    mount_options: String,
    filesystem_type: String,
    source: String,
}

#[cfg(any(target_os = "linux", test))]
impl std::fmt::Debug for MountInfoEntry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountInfoEntry")
            .field("mount_point", &"[REDACTED]")
            .field("mount_options", &"[REDACTED]")
            .field("filesystem_type", &"[REDACTED]")
            .field("source", &"[REDACTED]")
            .finish()
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadWriteProbeEvidence {
    rename_supported: bool,
    fsync_supported: bool,
    succeeded: bool,
}

#[cfg(target_os = "linux")]
fn probe_platform(config: &FilesystemMountProbeConfig) -> FilesystemMountObservation {
    let root_metadata = match fs::symlink_metadata(&config.mount_root) {
        Ok(metadata) if !metadata.file_type().is_symlink() && metadata.is_dir() => metadata,
        _ => {
            return FilesystemMountObservation::unavailable(MountProbeCondition::NotMountBoundary);
        }
    };
    drop(root_metadata);
    let root = match fs::canonicalize(&config.mount_root) {
        Ok(root) => root,
        Err(_) => {
            return FilesystemMountObservation::unavailable(MountProbeCondition::NotMountBoundary);
        }
    };
    let mountinfo = match fs::read_to_string("/proc/self/mountinfo") {
        Ok(mountinfo) => mountinfo,
        Err(_) => {
            return FilesystemMountObservation::unavailable(MountProbeCondition::NotMountBoundary);
        }
    };
    let entries = match parse_mountinfo(&mountinfo) {
        Ok(entries) => entries,
        Err(()) => {
            return FilesystemMountObservation::unavailable(MountProbeCondition::NotMountBoundary);
        }
    };
    let Some(entry) = entries.into_iter().find(|entry| entry.mount_point == root) else {
        return FilesystemMountObservation::unavailable(MountProbeCondition::NotMountBoundary);
    };
    finish_probe(config, &root, entry)
}

#[cfg(not(target_os = "linux"))]
fn probe_platform(_config: &FilesystemMountProbeConfig) -> FilesystemMountObservation {
    FilesystemMountObservation::unavailable(MountProbeCondition::UnsupportedPlatform)
}

#[cfg(unix)]
fn probe_development_directory_platform(
    config: &DevelopmentDirectoryMountProbeConfig,
) -> FilesystemMountObservation {
    let configured_metadata = match fs::symlink_metadata(&config.directory_root) {
        Ok(metadata) if !metadata.file_type().is_symlink() && metadata.is_dir() => metadata,
        _ => {
            return FilesystemMountObservation::unavailable(MountProbeCondition::NotMountBoundary);
        }
    };
    let root = match fs::canonicalize(&config.directory_root) {
        Ok(root) => root,
        Err(_) => {
            return FilesystemMountObservation::unavailable(MountProbeCondition::NotMountBoundary);
        }
    };
    let metadata = match fs::symlink_metadata(&root) {
        Ok(metadata)
            if !metadata.file_type().is_symlink()
                && metadata.is_dir()
                && metadata.dev() == configured_metadata.dev()
                && metadata.ino() == configured_metadata.ino() =>
        {
            metadata
        }
        _ => {
            return FilesystemMountObservation::unavailable(MountProbeCondition::NotMountBoundary);
        }
    };
    let filesystem = match rustix::fs::statvfs(&root) {
        Ok(filesystem) => filesystem,
        Err(_) => {
            return FilesystemMountObservation::unavailable(
                MountProbeCondition::FilesystemMetadataUnavailable,
            );
        }
    };
    let fragment_size = if filesystem.f_frsize == 0 {
        filesystem.f_bsize
    } else {
        filesystem.f_frsize
    };
    let available_bytes = filesystem.f_bavail.saturating_mul(fragment_size);
    let mount_identity_digest = development_directory_identity_digest(
        &root,
        &metadata,
        &config.expected_volume_marker,
        &config.volume_descriptor_digest,
    );
    let filesystem_config = config.filesystem_config();
    finish_probe_with_filesystem_evidence(
        &filesystem_config,
        &root,
        Some(MountAccessMode::ReadWrite),
        Some(available_bytes),
        Some(mount_identity_digest),
    )
}

#[cfg(not(unix))]
fn probe_development_directory_platform(
    _config: &DevelopmentDirectoryMountProbeConfig,
) -> FilesystemMountObservation {
    FilesystemMountObservation::unavailable(MountProbeCondition::UnsupportedPlatform)
}

#[cfg(target_os = "linux")]
fn finish_probe(
    config: &FilesystemMountProbeConfig,
    root: &Path,
    entry: MountInfoEntry,
) -> FilesystemMountObservation {
    let filesystem = rustix::fs::statvfs(root).ok();
    let mount_identity_digest = filesystem
        .as_ref()
        .map(|filesystem| mount_identity_digest(&entry, filesystem.f_fsid));
    let available_bytes = filesystem.as_ref().map(|filesystem| {
        let fragment_size = if filesystem.f_frsize == 0 {
            filesystem.f_bsize
        } else {
            filesystem.f_frsize
        };
        filesystem.f_bavail.saturating_mul(fragment_size)
    });
    finish_probe_with_filesystem_evidence(
        config,
        root,
        mount_access_mode(&entry.mount_options),
        available_bytes,
        mount_identity_digest,
    )
}

#[cfg(unix)]
fn finish_probe_with_filesystem_evidence(
    config: &FilesystemMountProbeConfig,
    root: &Path,
    access_mode: Option<MountAccessMode>,
    available_bytes: Option<u64>,
    mount_identity_digest: Option<AgentMountIdentityDigest>,
) -> FilesystemMountObservation {
    let observed_volume_marker = match read_marker(root) {
        Ok(marker) => marker,
        Err(()) => {
            return FilesystemMountObservation {
                observed_volume_marker: None,
                marker_matches: false,
                mount_boundary_detected: true,
                access_mode,
                rename_supported: false,
                fsync_supported: false,
                health: ResourceHealth::Unavailable,
                available_bytes,
                mount_identity_digest,
                condition: MountProbeCondition::MarkerUnavailable,
            };
        }
    };
    if observed_volume_marker != config.expected_volume_marker {
        return FilesystemMountObservation {
            observed_volume_marker: Some(observed_volume_marker),
            marker_matches: false,
            mount_boundary_detected: true,
            access_mode,
            rename_supported: false,
            fsync_supported: false,
            health: ResourceHealth::Unavailable,
            available_bytes,
            mount_identity_digest,
            condition: MountProbeCondition::MarkerMismatch,
        };
    }

    let Some(access_mode) = access_mode else {
        return FilesystemMountObservation {
            observed_volume_marker: Some(observed_volume_marker),
            marker_matches: true,
            mount_boundary_detected: true,
            access_mode: None,
            rename_supported: false,
            fsync_supported: false,
            health: ResourceHealth::Unavailable,
            available_bytes,
            mount_identity_digest,
            condition: MountProbeCondition::AccessModeUnavailable,
        };
    };
    let (Some(available_bytes), Some(mount_identity_digest)) =
        (available_bytes, mount_identity_digest)
    else {
        return FilesystemMountObservation {
            observed_volume_marker: Some(observed_volume_marker),
            marker_matches: true,
            mount_boundary_detected: true,
            access_mode: Some(access_mode),
            rename_supported: false,
            fsync_supported: false,
            health: ResourceHealth::Unavailable,
            available_bytes: None,
            mount_identity_digest: None,
            condition: MountProbeCondition::FilesystemMetadataUnavailable,
        };
    };

    if config.desired_access_mode == MountAccessMode::ReadWrite
        && access_mode != MountAccessMode::ReadWrite
    {
        return FilesystemMountObservation {
            observed_volume_marker: Some(observed_volume_marker),
            marker_matches: true,
            mount_boundary_detected: true,
            access_mode: Some(access_mode),
            rename_supported: false,
            fsync_supported: false,
            health: ResourceHealth::Degraded,
            available_bytes: Some(available_bytes),
            mount_identity_digest: Some(mount_identity_digest),
            condition: MountProbeCondition::AccessModeMismatch,
        };
    }
    let write_evidence = if access_mode == MountAccessMode::ReadWrite {
        run_read_write_probe(root)
    } else {
        ReadWriteProbeEvidence {
            rename_supported: false,
            fsync_supported: false,
            succeeded: true,
        }
    };
    if !write_evidence.succeeded {
        return FilesystemMountObservation {
            observed_volume_marker: Some(observed_volume_marker),
            marker_matches: true,
            mount_boundary_detected: true,
            access_mode: Some(access_mode),
            rename_supported: write_evidence.rename_supported,
            fsync_supported: write_evidence.fsync_supported,
            health: ResourceHealth::Unavailable,
            available_bytes: Some(available_bytes),
            mount_identity_digest: Some(mount_identity_digest),
            condition: MountProbeCondition::ReadWriteProbeFailed,
        };
    }
    if available_bytes < config.hard_minimum_free_bytes {
        return FilesystemMountObservation {
            observed_volume_marker: Some(observed_volume_marker),
            marker_matches: true,
            mount_boundary_detected: true,
            access_mode: Some(access_mode),
            rename_supported: write_evidence.rename_supported,
            fsync_supported: write_evidence.fsync_supported,
            health: ResourceHealth::Unavailable,
            available_bytes: Some(available_bytes),
            mount_identity_digest: Some(mount_identity_digest),
            condition: MountProbeCondition::BelowHardFreeSpaceMinimum,
        };
    }
    if available_bytes < config.ready_minimum_free_bytes {
        return FilesystemMountObservation {
            observed_volume_marker: Some(observed_volume_marker),
            marker_matches: true,
            mount_boundary_detected: true,
            access_mode: Some(access_mode),
            rename_supported: write_evidence.rename_supported,
            fsync_supported: write_evidence.fsync_supported,
            health: ResourceHealth::Degraded,
            available_bytes: Some(available_bytes),
            mount_identity_digest: Some(mount_identity_digest),
            condition: MountProbeCondition::LowFreeSpace,
        };
    }
    FilesystemMountObservation {
        observed_volume_marker: Some(observed_volume_marker),
        marker_matches: true,
        mount_boundary_detected: true,
        access_mode: Some(access_mode),
        rename_supported: write_evidence.rename_supported,
        fsync_supported: write_evidence.fsync_supported,
        health: ResourceHealth::Ready,
        available_bytes: Some(available_bytes),
        mount_identity_digest: Some(mount_identity_digest),
        condition: MountProbeCondition::Ready,
    }
}

#[cfg(unix)]
fn read_marker(root: &Path) -> Result<VolumeMarkerId, ()> {
    use rustix::fs::{openat, Mode, OFlags, CWD};

    let root = openat(
        CWD,
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ())?;
    let marker = openat(
        &root,
        VOLUME_MARKER_FILE_NAME,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| ())?;
    let mut marker = File::from(marker);
    let metadata = marker.metadata().map_err(|_| ())?;
    if !metadata.is_file() || metadata.len() > 256 {
        return Err(());
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut marker)
        .take(257)
        .read_to_end(&mut bytes)
        .map_err(|_| ())?;
    if bytes.len() > 256 {
        return Err(());
    }
    let value = String::from_utf8(bytes).map_err(|_| ())?;
    let value = value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(&value);
    VolumeMarkerId::new(value).map_err(|_| ())
}

#[cfg(any(target_os = "linux", test))]
fn mount_access_mode(options: &str) -> Option<MountAccessMode> {
    let mut read_only = false;
    let mut read_write = false;
    for option in options.split(',') {
        match option {
            "ro" => read_only = true,
            "rw" => read_write = true,
            _ => {}
        }
    }
    match (read_only, read_write) {
        (true, false) => Some(MountAccessMode::ReadOnly),
        (false, true) => Some(MountAccessMode::ReadWrite),
        _ => None,
    }
}

#[cfg(any(target_os = "linux", test))]
fn mount_identity_digest(entry: &MountInfoEntry, fsid: u64) -> AgentMountIdentityDigest {
    let stable_source = (!entry.source.starts_with("/dev/")).then_some(entry.source.as_str());
    let mut material = Vec::with_capacity(
        entry.filesystem_type.len()
            + stable_source.map_or(0, str::len)
            + std::mem::size_of::<u64>()
            + 32,
    );
    material.extend_from_slice(b"neoengram-mount-identity-v1\0");
    material.extend_from_slice(entry.filesystem_type.as_bytes());
    material.push(0);
    if let Some(source) = stable_source {
        material.extend_from_slice(source.as_bytes());
    }
    material.push(0);
    material.extend_from_slice(&fsid.to_le_bytes());
    AgentMountIdentityDigest::new(ContentDigest::hash(material))
}

#[cfg(unix)]
fn development_directory_identity_digest(
    root: &Path,
    metadata: &fs::Metadata,
    marker: &VolumeMarkerId,
    volume_descriptor_digest: &ContentDigest,
) -> AgentMountIdentityDigest {
    let path = root.as_os_str().as_bytes();
    let marker = marker.as_str().as_bytes();
    let mut material = Vec::with_capacity(path.len() + marker.len() + 96);
    material.extend_from_slice(b"neoengram-development-directory-identity-v1\0");
    material.extend_from_slice(&(path.len() as u64).to_le_bytes());
    material.extend_from_slice(path);
    material.extend_from_slice(&metadata.dev().to_le_bytes());
    material.extend_from_slice(&metadata.ino().to_le_bytes());
    material.extend_from_slice(&(marker.len() as u64).to_le_bytes());
    material.extend_from_slice(marker);
    material.extend_from_slice(volume_descriptor_digest.as_bytes());
    AgentMountIdentityDigest::new(ContentDigest::hash(material))
}

#[cfg(unix)]
fn run_read_write_probe(root: &Path) -> ReadWriteProbeEvidence {
    let sequence = PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    run_read_write_probe_with_sequence(root, sequence)
}

#[cfg(unix)]
fn run_read_write_probe_with_sequence(root: &Path, sequence: u64) -> ReadWriteProbeEvidence {
    let prefix = format!(".neoengram-rw-probe-{}-{sequence}", std::process::id());
    let pending = root.join(format!("{prefix}.pending"));
    let committed = root.join(format!("{prefix}.committed"));
    let mut pending_owned = false;
    let mut committed_owned = false;
    let mut evidence = ReadWriteProbeEvidence {
        rename_supported: false,
        fsync_supported: false,
        succeeded: false,
    };
    let result = (|| -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&pending)?;
        pending_owned = true;
        file.write_all(b"neoengram mount readiness probe\n")?;
        file.sync_all()?;
        drop(file);
        rename_no_replace(&pending, &committed)?;
        pending_owned = false;
        committed_owned = true;
        evidence.rename_supported = true;
        sync_directory(root)?;
        fs::remove_file(&committed)?;
        committed_owned = false;
        sync_directory(root)?;
        evidence.fsync_supported = true;
        evidence.succeeded = true;
        Ok(())
    })();
    if result.is_err() {
        if pending_owned {
            let _ = fs::remove_file(&pending);
        } else if committed_owned {
            let _ = fs::remove_file(&committed);
        }
    }
    evidence
}

#[cfg(unix)]
fn rename_no_replace(source: &Path, destination: &Path) -> io::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        source,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(io::Error::from)
}

#[cfg(unix)]
fn sync_directory(root: &Path) -> io::Result<()> {
    match File::open(root)?.sync_all() {
        Ok(()) => Ok(()),
        #[cfg(not(target_os = "linux"))]
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::InvalidInput | io::ErrorKind::Unsupported
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_mountinfo(input: &str) -> Result<Vec<MountInfoEntry>, ()> {
    input.lines().map(parse_mountinfo_line).collect()
}

#[cfg(any(target_os = "linux", test))]
fn parse_mountinfo_line(line: &str) -> Result<MountInfoEntry, ()> {
    let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
    let separator = fields.iter().position(|field| *field == "-").ok_or(())?;
    if separator < 6
        || fields.len() <= separator + 3
        || !valid_major_minor(fields[2])
        || fields[separator + 1].is_empty()
    {
        return Err(());
    }
    Ok(MountInfoEntry {
        mount_point: PathBuf::from(decode_mountinfo_field(fields[4])?),
        mount_options: fields[5].to_owned(),
        filesystem_type: fields[separator + 1].to_owned(),
        source: decode_mountinfo_field(fields[separator + 2])?,
    })
}

#[cfg(any(target_os = "linux", test))]
fn valid_major_minor(value: &str) -> bool {
    let Some((major, minor)) = value.split_once(':') else {
        return false;
    };
    !major.is_empty()
        && !minor.is_empty()
        && major.bytes().all(|byte| byte.is_ascii_digit())
        && minor.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(any(target_os = "linux", test))]
fn decode_mountinfo_field(value: &str) -> Result<String, ()> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        let escape = bytes.get(index + 1..index + 4).ok_or(())?;
        let byte = match escape {
            b"040" => b' ',
            b"011" => b'\t',
            b"012" => b'\n',
            b"134" => b'\\',
            _ => return Err(()),
        };
        decoded.push(byte);
        index += 4;
    }
    String::from_utf8(decoded).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_linux_mountinfo_and_decodes_escaped_fields() {
        let entries = parse_mountinfo(
            "36 25 0:32 / /volume\\040data rw,relatime shared:7 - nfs4 server:/export\\040a rw,vers=4.1\n",
        )
        .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].mount_point, Path::new("/volume data"));
        assert_eq!(entries[0].source, "server:/export a");
        assert_eq!(
            mount_access_mode(&entries[0].mount_options),
            Some(MountAccessMode::ReadWrite)
        );
    }

    #[test]
    fn mount_identity_ignores_ephemeral_major_minor_numbers() {
        let first =
            parse_mountinfo("36 25 8:1 / /volume rw,relatime shared:7 - ext4 /dev/pvc-a rw\n")
                .unwrap()
                .pop()
                .unwrap();
        let reattached =
            parse_mountinfo("52 25 259:7 / /volume rw,relatime shared:9 - ext4 /dev/pvc-a rw\n")
                .unwrap()
                .pop()
                .unwrap();
        let device_renamed =
            parse_mountinfo("61 25 259:9 / /volume rw,relatime shared:11 - ext4 /dev/nvme7n1 rw\n")
                .unwrap()
                .pop()
                .unwrap();

        assert_eq!(
            mount_identity_digest(&first, 0x1234),
            mount_identity_digest(&reattached, 0x1234)
        );
        assert_eq!(
            mount_identity_digest(&first, 0x1234),
            mount_identity_digest(&device_renamed, 0x1234)
        );
        assert_ne!(
            mount_identity_digest(&first, 0x1234),
            mount_identity_digest(&reattached, 0x5678)
        );
    }

    #[test]
    fn mount_identity_binds_stable_network_source() {
        let first = parse_mountinfo(
            "36 25 0:32 / /volume rw,relatime shared:7 - nfs4 server:/export-a rw\n",
        )
        .unwrap()
        .pop()
        .unwrap();
        let other_export = parse_mountinfo(
            "52 25 0:41 / /volume rw,relatime shared:9 - nfs4 server:/export-b rw\n",
        )
        .unwrap()
        .pop()
        .unwrap();

        assert_ne!(
            mount_identity_digest(&first, 0x1234),
            mount_identity_digest(&other_export, 0x1234)
        );
    }

    #[test]
    fn rejects_malformed_or_ambiguous_mountinfo() {
        assert!(parse_mountinfo("36 25 bad / /volume rw - nfs source rw\n").is_err());
        assert!(parse_mountinfo("36 25 0:32 / /volume rw nfs source rw\n").is_err());
        assert_eq!(mount_access_mode("ro,rw"), None);
    }

    #[test]
    fn mount_info_debug_redacts_physical_storage_details() {
        let entry = MountInfoEntry {
            mount_point: PathBuf::from("/sensitive/pvc-mount-a"),
            mount_options: "rw,secret-option".to_owned(),
            filesystem_type: "sensitive-filesystem".to_owned(),
            source: "sensitive-server:/private-export".to_owned(),
        };

        let debug = format!("{entry:?}");

        assert!(debug.contains("[REDACTED]"));
        for sensitive in [
            "/sensitive/pvc-mount-a",
            "secret-option",
            "sensitive-filesystem",
            "sensitive-server:/private-export",
        ] {
            assert!(!debug.contains(sensitive));
        }
    }

    #[cfg(any(
        target_vendor = "apple",
        target_os = "linux",
        target_os = "android",
        target_os = "redox"
    ))]
    #[test]
    fn read_write_probe_leaves_no_files() {
        let root = tempfile::tempdir().unwrap();
        let evidence = run_read_write_probe(root.path());
        assert_eq!(
            evidence,
            ReadWriteProbeEvidence {
                rename_supported: true,
                fsync_supported: true,
                succeeded: true,
            }
        );
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[cfg(any(
        target_vendor = "apple",
        target_os = "linux",
        target_os = "android",
        target_os = "redox"
    ))]
    #[test]
    fn read_write_probe_preserves_a_preexisting_pending_file() {
        let root = tempfile::tempdir().unwrap();
        let sequence = PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let prefix = format!(".neoengram-rw-probe-{}-{sequence}", std::process::id());
        let pending = root.path().join(format!("{prefix}.pending"));
        let committed = root.path().join(format!("{prefix}.committed"));
        fs::write(&pending, b"preexisting pending contents").unwrap();

        let evidence = run_read_write_probe_with_sequence(root.path(), sequence);

        assert!(!evidence.succeeded);
        assert_eq!(fs::read(&pending).unwrap(), b"preexisting pending contents");
        assert!(!committed.exists());
    }

    #[cfg(any(
        target_vendor = "apple",
        target_os = "linux",
        target_os = "android",
        target_os = "redox"
    ))]
    #[test]
    fn read_write_probe_preserves_a_preexisting_committed_file() {
        let root = tempfile::tempdir().unwrap();
        let sequence = PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let prefix = format!(".neoengram-rw-probe-{}-{sequence}", std::process::id());
        let pending = root.path().join(format!("{prefix}.pending"));
        let committed = root.path().join(format!("{prefix}.committed"));
        fs::write(&committed, b"preexisting committed contents").unwrap();

        let evidence = run_read_write_probe_with_sequence(root.path(), sequence);

        assert!(!evidence.succeeded);
        assert!(!pending.exists());
        assert_eq!(
            fs::read(&committed).unwrap(),
            b"preexisting committed contents"
        );
    }

    #[cfg(unix)]
    #[test]
    fn marker_must_be_regular_and_exact() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(VOLUME_MARKER_FILE_NAME), "marker-a\n").unwrap();
        assert_eq!(
            read_marker(root.path()).unwrap(),
            VolumeMarkerId::new("marker-a").unwrap()
        );
        fs::write(root.path().join(VOLUME_MARKER_FILE_NAME), " marker-a\n").unwrap();
        assert!(read_marker(root.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn marker_open_rejects_symlinks_and_oversize_files() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("marker-target");
        fs::write(&target, "volume-a\n").unwrap();
        symlink(&target, root.path().join(VOLUME_MARKER_FILE_NAME)).unwrap();
        assert!(read_marker(root.path()).is_err());

        fs::remove_file(root.path().join(VOLUME_MARKER_FILE_NAME)).unwrap();
        fs::write(root.path().join(VOLUME_MARKER_FILE_NAME), "a".repeat(257)).unwrap();
        assert!(read_marker(root.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn development_directory_probe_accepts_an_opted_in_ordinary_directory() {
        let root = tempfile::tempdir().unwrap();
        fs::write(
            root.path().join(VOLUME_MARKER_FILE_NAME),
            "volume-development\n",
        )
        .unwrap();
        let config = DevelopmentDirectoryMountProbeConfig {
            directory_root: root.path().to_path_buf(),
            expected_volume_marker: VolumeMarkerId::new("volume-development").unwrap(),
            volume_descriptor_digest: ContentDigest::hash(b"development-volume-descriptor"),
            hard_minimum_free_bytes: 0,
            ready_minimum_free_bytes: 0,
        };

        let first = config.probe();
        let second = config.probe();

        assert_eq!(first.condition, MountProbeCondition::Ready);
        assert_eq!(first.health, ResourceHealth::Ready);
        assert!(first.marker_matches);
        assert!(first.mount_boundary_detected);
        assert_eq!(first.access_mode, Some(MountAccessMode::ReadWrite));
        assert!(first.rename_supported);
        assert!(first.fsync_supported);
        assert!(first.available_bytes.is_some());
        assert_eq!(first.mount_identity_digest, second.mount_identity_digest);
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);

        let mut other_descriptor = config;
        other_descriptor.volume_descriptor_digest = ContentDigest::hash(b"other-descriptor");
        assert_ne!(
            first.mount_identity_digest,
            other_descriptor.probe().mount_identity_digest
        );
    }

    #[cfg(unix)]
    #[test]
    fn development_directory_probe_rejects_marker_mismatch() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(VOLUME_MARKER_FILE_NAME), "volume-other\n").unwrap();
        let config = DevelopmentDirectoryMountProbeConfig {
            directory_root: root.path().to_path_buf(),
            expected_volume_marker: VolumeMarkerId::new("volume-expected").unwrap(),
            volume_descriptor_digest: ContentDigest::hash(b"development-volume-descriptor"),
            hard_minimum_free_bytes: 0,
            ready_minimum_free_bytes: 0,
        };

        let observation = config.probe();

        assert_eq!(observation.condition, MountProbeCondition::MarkerMismatch);
        assert_eq!(observation.health, ResourceHealth::Unavailable);
        assert!(!observation.marker_matches);
        assert!(observation.mount_boundary_detected);
    }

    #[cfg(unix)]
    #[test]
    fn development_directory_probe_rejects_a_symlink_root() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let actual = parent.path().join("actual");
        let link = parent.path().join("link");
        fs::create_dir(&actual).unwrap();
        fs::write(actual.join(VOLUME_MARKER_FILE_NAME), "volume-development\n").unwrap();
        symlink(&actual, &link).unwrap();
        let config = DevelopmentDirectoryMountProbeConfig {
            directory_root: link,
            expected_volume_marker: VolumeMarkerId::new("volume-development").unwrap(),
            volume_descriptor_digest: ContentDigest::hash(b"development-volume-descriptor"),
            hard_minimum_free_bytes: 0,
            ready_minimum_free_bytes: 0,
        };

        let observation = config.probe();

        assert_eq!(observation.condition, MountProbeCondition::NotMountBoundary);
        assert_eq!(observation.health, ResourceHealth::Unavailable);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn production_filesystem_probe_remains_unsupported_off_linux() {
        let config = FilesystemMountProbeConfig {
            mount_root: PathBuf::from("/volume"),
            expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
            desired_access_mode: MountAccessMode::ReadWrite,
            hard_minimum_free_bytes: 0,
            ready_minimum_free_bytes: 0,
        };

        assert_eq!(
            config.probe().condition,
            MountProbeCondition::UnsupportedPlatform
        );
    }

    #[test]
    fn free_space_thresholds_are_ordered() {
        let config = FilesystemMountProbeConfig {
            mount_root: PathBuf::from("/volume"),
            expected_volume_marker: VolumeMarkerId::new("marker-a").unwrap(),
            desired_access_mode: MountAccessMode::ReadWrite,
            hard_minimum_free_bytes: 2,
            ready_minimum_free_bytes: 1,
        };
        assert_eq!(
            config.validate().unwrap_err().code(),
            AgentErrorCode::InvalidState
        );
    }

    #[test]
    fn mount_root_rejects_parent_components() {
        let config = FilesystemMountProbeConfig {
            mount_root: PathBuf::from("/volume/../other"),
            expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
            desired_access_mode: MountAccessMode::ReadWrite,
            hard_minimum_free_bytes: 1,
            ready_minimum_free_bytes: 2,
        };
        assert_eq!(
            config.validate().unwrap_err().code(),
            AgentErrorCode::ScopeMismatch
        );
    }

    #[test]
    fn p0_mount_probe_rejects_read_only_intent() {
        let config = FilesystemMountProbeConfig {
            mount_root: PathBuf::from("/volume"),
            expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
            desired_access_mode: MountAccessMode::ReadOnly,
            hard_minimum_free_bytes: 1,
            ready_minimum_free_bytes: 2,
        };
        assert_eq!(
            config.validate().unwrap_err().code(),
            AgentErrorCode::InvalidState
        );
    }

    #[test]
    fn mount_probe_config_debug_redacts_paths_and_capacity_thresholds() {
        let config = FilesystemMountProbeConfig {
            mount_root: PathBuf::from("/sensitive/pvc-mount-b"),
            expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
            desired_access_mode: MountAccessMode::ReadWrite,
            hard_minimum_free_bytes: 7_310_047,
            ready_minimum_free_bytes: 9_820_061,
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("mount_root_configured: true"));
        assert!(debug.contains("free_space_thresholds: \"configured\""));
        assert!(!debug.contains("/sensitive/pvc-mount-b"));
        assert!(!debug.contains("7310047"));
        assert!(!debug.contains("9820061"));
    }

    #[test]
    fn debug_redacts_mount_identity_digest() {
        let digest = AgentMountIdentityDigest::new(ContentDigest::from_bytes([0xab; 32]));
        let observation = FilesystemMountObservation {
            observed_volume_marker: Some(VolumeMarkerId::new("marker-a").unwrap()),
            marker_matches: true,
            mount_boundary_detected: true,
            access_mode: Some(MountAccessMode::ReadWrite),
            rename_supported: true,
            fsync_supported: true,
            health: ResourceHealth::Ready,
            available_bytes: Some(123_456_789),
            mount_identity_digest: Some(digest),
            condition: MountProbeCondition::Ready,
        };
        let debug = format!("{observation:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(&digest.as_digest().to_string()));
        assert!(!debug.contains("123456789"));
        assert!(debug.contains("available_bytes_present: true"));
    }

    #[test]
    fn bootstrap_mapping_is_bounded_and_explicit() {
        let ready = observation(
            Some("volume-a"),
            true,
            true,
            Some(MountAccessMode::ReadWrite),
            true,
            true,
            ResourceHealth::Ready,
            MountProbeCondition::Ready,
        );
        let ready_wire = ready.to_bootstrap_probe(UnixMillis::new(100)).unwrap();
        assert!(ready_wire.marker_matches);
        assert!(ready_wire.mount_boundary_detected);
        assert!(ready_wire.rename_supported);
        assert!(ready_wire.fsync_supported);
        assert_eq!(ready_wire.health, ResourceHealth::Ready);
        assert_eq!(
            ready_wire.mount_identity_digest.as_digest().as_bytes(),
            &[0x42; 32]
        );
        assert!(ready_wire.extensions.is_empty());

        let read_only = observation(
            Some("volume-a"),
            true,
            true,
            Some(MountAccessMode::ReadOnly),
            false,
            false,
            ResourceHealth::Degraded,
            MountProbeCondition::AccessModeMismatch,
        )
        .to_bootstrap_probe(UnixMillis::new(101))
        .unwrap();
        assert_eq!(read_only.access_mode, Some(MountAccessMode::ReadOnly));
        assert!(!read_only.rename_supported);
        assert!(!read_only.fsync_supported);

        let marker_mismatch = observation(
            Some("volume-b"),
            false,
            true,
            Some(MountAccessMode::ReadWrite),
            false,
            false,
            ResourceHealth::Unavailable,
            MountProbeCondition::MarkerMismatch,
        )
        .to_bootstrap_probe(UnixMillis::new(102))
        .unwrap();
        assert!(!marker_mismatch.marker_matches);
        assert!(marker_mismatch.mount_boundary_detected);

        let rw_failure = observation(
            Some("volume-a"),
            true,
            true,
            Some(MountAccessMode::ReadWrite),
            true,
            false,
            ResourceHealth::Unavailable,
            MountProbeCondition::ReadWriteProbeFailed,
        )
        .to_bootstrap_probe(UnixMillis::new(103))
        .unwrap();
        assert!(rw_failure.rename_supported);
        assert!(!rw_failure.fsync_supported);
        assert_eq!(rw_failure.health, ResourceHealth::Unavailable);

        let no_identity = FilesystemMountObservation::unavailable(
            MountProbeCondition::FilesystemMetadataUnavailable,
        );
        assert_eq!(
            no_identity
                .to_bootstrap_probe(UnixMillis::new(104))
                .unwrap_err()
                .code(),
            AgentErrorCode::MountUnavailable
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn observation(
        marker: Option<&str>,
        marker_matches: bool,
        mount_boundary_detected: bool,
        access_mode: Option<MountAccessMode>,
        rename_supported: bool,
        fsync_supported: bool,
        health: ResourceHealth,
        condition: MountProbeCondition,
    ) -> FilesystemMountObservation {
        FilesystemMountObservation {
            observed_volume_marker: marker.map(|marker| VolumeMarkerId::new(marker).unwrap()),
            marker_matches,
            mount_boundary_detected,
            access_mode,
            rename_supported,
            fsync_supported,
            health,
            available_bytes: Some(999_999),
            mount_identity_digest: Some(AgentMountIdentityDigest::new(ContentDigest::from_bytes(
                [0x42; 32],
            ))),
            condition,
        }
    }
}
