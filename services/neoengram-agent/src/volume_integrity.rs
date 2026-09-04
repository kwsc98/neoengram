//! Read-only integrity verification for the Managed Volume object store.
//!
//! Agent startup validates the SQLite inventory, but that check cannot see bytes that an
//! operator removed directly from a mounted Volume.  This scanner walks the published CAS,
//! hashes every ordinary file, and compares the result with the Agent-local placement evidence.
//! It deliberately does not repair, delete, or update any state; callers decide whether a
//! reported issue should trigger re-materialization or a Central reconciliation pass.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use neoengram_domain::{
    core::ObjectId,
    protocol::{
        materialization::{
            IntegrityScanReport, ObjectPlacement, ObjectPlacementState, PlacementHealthObservation,
            PlacementHealthState, VolumeIntegrityScan, VolumeIntegrityScanState,
        },
        ArtifactId, DecimalU64, IntegrityScanId, MountGeneration, ObjectNamespaceId,
        PlacementGeneration, StorageVolumeId, TenantId, UnixMillis,
    },
};

use crate::{AgentError, AgentErrorCode, AgentResult, LocalPlacementInventory};

const METADATA_DIRECTORY: &str = ".neoengram";
const OBJECTS_DIRECTORY: &str = "objects";
const TENANTS_DIRECTORY: &str = "tenants";
const ARTIFACTS_DIRECTORY: &str = "artifacts";
const OBJECT_TEMP_DIRECTORY: &str = ".tmp";
const HASH_BUFFER_SIZE: usize = 64 * 1024;

type ObjectPathMap = BTreeMap<(ObjectNamespaceId, ObjectId), PathBuf>;
type ObjectIdentitySet = BTreeSet<(ObjectNamespaceId, ObjectId)>;

/// Why an entry was included in an integrity report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeIntegrityIssueKind {
    /// A durable, active placement has no published object on the Volume.
    Missing,
    /// A published object cannot be read or fails its size/hash/type checks.
    Corrupt,
    /// A valid published object has no active placement evidence.
    Orphan,
    /// A path or identifier does not belong to the approved CAS layout.
    Unknown,
}

/// One read-only integrity finding.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VolumeIntegrityIssue {
    pub kind: VolumeIntegrityIssueKind,
    pub path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_namespace_id: Option<ObjectNamespaceId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<ObjectId>,
    pub detail: String,
}

impl VolumeIntegrityIssue {
    fn new(
        kind: VolumeIntegrityIssueKind,
        path: impl Into<PathBuf>,
        object_namespace_id: Option<ObjectNamespaceId>,
        object_id: Option<ObjectId>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            path: path.into(),
            object_namespace_id,
            object_id,
            detail: detail.into(),
        }
    }
}

/// Aggregate result of a complete Volume scan.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VolumeIntegrityReport {
    pub tenant_id: TenantId,
    pub storage_volume_id: StorageVolumeId,
    /// Number of physical object entries examined, including entries that failed validation.
    pub scanned_objects: u64,
    /// Number of physical files that passed filename, size, and BLAKE3 checks.
    pub verified_objects: u64,
    pub missing: Vec<VolumeIntegrityIssue>,
    pub corrupt: Vec<VolumeIntegrityIssue>,
    pub orphan: Vec<VolumeIntegrityIssue>,
    pub unknown: Vec<VolumeIntegrityIssue>,
}

impl VolumeIntegrityReport {
    #[must_use]
    pub fn issue_count(&self) -> usize {
        self.missing.len() + self.corrupt.len() + self.orphan.len() + self.unknown.len()
    }

    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.issue_count() == 0
    }

    fn push(&mut self, issue: VolumeIntegrityIssue) {
        match issue.kind {
            VolumeIntegrityIssueKind::Missing => self.missing.push(issue),
            VolumeIntegrityIssueKind::Corrupt => self.corrupt.push(issue),
            VolumeIntegrityIssueKind::Orphan => self.orphan.push(issue),
            VolumeIntegrityIssueKind::Unknown => self.unknown.push(issue),
        }
    }

    /// Persists the latest scan result for health tooling. The report is diagnostic only; it is
    /// never used as authority for a Placement and can be replaced by the next scan.
    pub fn write_json(&self, path: impl AsRef<Path>) -> AgentResult<()> {
        let path = path.as_ref();
        let bytes = serde_json::to_vec_pretty(self).map_err(|error| {
            AgentError::new(
                AgentErrorCode::Internal,
                format!("failed to encode Volume integrity report: {error}"),
            )
        })?;
        let temporary = path.with_extension("json.tmp");
        fs::write(&temporary, &bytes).map_err(|error| {
            AgentError::new(
                AgentErrorCode::MountUnavailable,
                format!("failed to write Volume integrity report: {error}"),
            )
        })?;
        fs::rename(&temporary, path).map_err(|error| {
            AgentError::new(
                AgentErrorCode::MountUnavailable,
                format!("failed to publish Volume integrity report: {error}"),
            )
        })?;
        Ok(())
    }
}

/// Read-only scanner for the artifact-scoped Managed CAS on one Agent Volume.
#[derive(Debug, Clone)]
pub struct VolumeIntegrityScanner {
    mount_root: PathBuf,
    tenant_id: TenantId,
    storage_volume_id: StorageVolumeId,
    inventory: Arc<dyn LocalPlacementInventory>,
}

impl VolumeIntegrityScanner {
    #[must_use]
    pub fn new(
        mount_root: impl Into<PathBuf>,
        tenant_id: TenantId,
        storage_volume_id: StorageVolumeId,
        inventory: Arc<dyn LocalPlacementInventory>,
    ) -> Self {
        Self {
            mount_root: mount_root.into(),
            tenant_id,
            storage_volume_id,
            inventory,
        }
    }

    /// Scans all published objects and all active local placement evidence without mutating
    /// either the Volume or the inventory database.
    pub fn scan(&self) -> AgentResult<VolumeIntegrityReport> {
        let mut report = VolumeIntegrityReport {
            tenant_id: self.tenant_id.clone(),
            storage_volume_id: self.storage_volume_id.clone(),
            scanned_objects: 0,
            verified_objects: 0,
            missing: Vec::new(),
            corrupt: Vec::new(),
            orphan: Vec::new(),
            unknown: Vec::new(),
        };

        let inventory = self.inventory.list_all()?;
        let expected = collect_expected(
            &self.mount_root,
            &self.tenant_id,
            &self.storage_volume_id,
            inventory,
            &mut report,
        );
        let (physical, observed) = self.scan_physical(&mut report)?;

        for (key, placements) in &expected {
            if !observed.contains(key) {
                let (namespace, object_id) = key;
                let path = self.object_path(namespace, *object_id);
                let expected_size = placements
                    .first()
                    .map(|placement| placement.size.get().to_string())
                    .unwrap_or_else(|| "unknown".to_owned());
                report.push(VolumeIntegrityIssue::new(
                    VolumeIntegrityIssueKind::Missing,
                    path,
                    Some(namespace.clone()),
                    Some(*object_id),
                    format!(
                        "active placement has no published object (expected size {expected_size})"
                    ),
                ));
            }
        }

        for (key, path) in physical {
            if let Some(placements) = expected.get(&key) {
                let actual_size = fs::symlink_metadata(&path)
                    .map(|metadata| metadata.len())
                    .unwrap_or_default();
                if !placements
                    .iter()
                    .any(|placement| placement.size.get() == actual_size)
                {
                    let expected_sizes = placements
                        .iter()
                        .map(|placement| placement.size.get().to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    report.push(VolumeIntegrityIssue::new(
                        VolumeIntegrityIssueKind::Corrupt,
                        path,
                        Some(key.0),
                        Some(key.1),
                        format!(
                            "object size does not match placement evidence (expected one of [{expected_sizes}], actual {actual_size})"
                        ),
                    ));
                }
            } else {
                report.push(VolumeIntegrityIssue::new(
                    VolumeIntegrityIssueKind::Orphan,
                    path,
                    Some(key.0),
                    Some(key.1),
                    "published object has no active local placement evidence",
                ));
            }
        }

        Ok(report)
    }

    /// Runs a scrub and converts its findings into namespace/placement-scoped observations for
    /// Central.  The physical scanner remains read-only; this method only builds signed-channel
    /// data from the same inventory snapshot and therefore cannot create or retire a placement.
    pub fn scan_report(
        &self,
        scan_id: IntegrityScanId,
        mount_generation: MountGeneration,
        observed_at_unix_ms: UnixMillis,
    ) -> AgentResult<IntegrityScanReport> {
        let report = self.scan()?;
        self.report_to_integrity_report(report, scan_id, mount_generation, observed_at_unix_ms)
    }

    /// Converts an already completed diagnostic scan without walking the Volume again.
    pub fn report_to_integrity_report(
        &self,
        report: VolumeIntegrityReport,
        scan_id: IntegrityScanId,
        mount_generation: MountGeneration,
        observed_at_unix_ms: UnixMillis,
    ) -> AgentResult<IntegrityScanReport> {
        let inventory = self.inventory.list_all()?;
        let placement_generation = inventory
            .iter()
            .map(|placement| placement.placement_generation)
            .max()
            .unwrap_or_else(|| PlacementGeneration::new(1));
        let mut observations = Vec::new();
        for placement in inventory {
            if placement.tenant_id != self.tenant_id
                || placement.storage_volume_id.as_ref() != Some(&self.storage_volume_id)
                || placement.archive_id.is_some()
                || !matches!(
                    placement.state,
                    ObjectPlacementState::Verified | ObjectPlacementState::Retiring
                )
            {
                continue;
            }
            let state = if report.corrupt.iter().any(|issue| {
                issue.object_namespace_id.as_ref() == Some(&placement.object_namespace_id)
                    && issue.object_id == Some(placement.object_id)
            }) {
                PlacementHealthState::Corrupt
            } else if report.missing.iter().any(|issue| {
                issue.object_namespace_id.as_ref() == Some(&placement.object_namespace_id)
                    && issue.object_id == Some(placement.object_id)
            }) {
                PlacementHealthState::Missing
            } else {
                PlacementHealthState::Healthy
            };
            observations.push(PlacementHealthObservation {
                scan_id: scan_id.clone(),
                tenant_id: self.tenant_id.clone(),
                object_namespace_id: placement.object_namespace_id,
                placement_id: placement.placement_id,
                object_id: placement.object_id,
                storage_volume_id: self.storage_volume_id.clone(),
                placement_generation: placement.placement_generation,
                state,
                observed_size: placement.size,
                observed_digest: placement.verified_digest,
                observed_at_unix_ms,
                detail: None,
            });
        }
        let healthy_objects = observations
            .iter()
            .filter(|observation| observation.state == PlacementHealthState::Healthy)
            .count() as u64;
        let missing_objects = observations
            .iter()
            .filter(|observation| observation.state == PlacementHealthState::Missing)
            .count() as u64;
        let corrupt_objects = observations
            .iter()
            .filter(|observation| observation.state == PlacementHealthState::Corrupt)
            .count() as u64;
        Ok(IntegrityScanReport {
            scan: VolumeIntegrityScan {
                scan_id,
                tenant_id: self.tenant_id.clone(),
                storage_volume_id: self.storage_volume_id.clone(),
                placement_generation,
                state: VolumeIntegrityScanState::Complete,
                checked_objects: DecimalU64::new(report.scanned_objects),
                healthy_objects: DecimalU64::new(healthy_objects),
                missing_objects: DecimalU64::new(missing_objects),
                corrupt_objects: DecimalU64::new(corrupt_objects),
                orphan_objects: DecimalU64::new(report.orphan.len() as u64),
                started_at_unix_ms: observed_at_unix_ms,
                finished_at_unix_ms: Some(observed_at_unix_ms),
                error: None,
            },
            mount_generation,
            observations,
            extensions: neoengram_domain::protocol::Extensions::new(),
        })
    }

    fn object_path(&self, namespace: &ObjectNamespaceId, object_id: ObjectId) -> PathBuf {
        self.mount_root
            .join(METADATA_DIRECTORY)
            .join(OBJECTS_DIRECTORY)
            .join(TENANTS_DIRECTORY)
            .join(self.tenant_id.as_str())
            .join(ARTIFACTS_DIRECTORY)
            .join(namespace.as_str())
            .join(OBJECTS_DIRECTORY)
            .join(object_id.to_hex())
    }

    fn scan_physical(
        &self,
        report: &mut VolumeIntegrityReport,
    ) -> AgentResult<(ObjectPathMap, ObjectIdentitySet)> {
        let mount = ordinary_directory(&self.mount_root).map_err(|error| {
            AgentError::new(
                AgentErrorCode::MountUnavailable,
                format!("approved Agent mount is unavailable: {error}"),
            )
        })?;
        let metadata = match child_directory(&mount, METADATA_DIRECTORY, report)? {
            Some(path) => path,
            None => return Ok((BTreeMap::new(), BTreeSet::new())),
        };
        let objects = match child_directory(&metadata, OBJECTS_DIRECTORY, report)? {
            Some(path) => path,
            None => return Ok((BTreeMap::new(), BTreeSet::new())),
        };
        let tenants = match child_directory(&objects, TENANTS_DIRECTORY, report)? {
            Some(path) => path,
            None => return Ok((BTreeMap::new(), BTreeSet::new())),
        };

        let mut physical = BTreeMap::new();
        let mut observed = BTreeSet::new();
        let entries = read_directory(&tenants).map_err(|error| scan_io_error(&tenants, error))?;
        for entry in entries {
            let path = entry.path();
            let Some(name) = file_name(&entry) else {
                report.push(VolumeIntegrityIssue::new(
                    VolumeIntegrityIssueKind::Unknown,
                    path,
                    None,
                    None,
                    "tenant directory entry is not valid UTF-8",
                ));
                continue;
            };
            if name != self.tenant_id.as_str() {
                report.push(VolumeIntegrityIssue::new(
                    VolumeIntegrityIssueKind::Unknown,
                    path,
                    None,
                    None,
                    format!("tenant directory is outside the configured tenant: {name}"),
                ));
                continue;
            }
            let Some(tenant_root) = ordinary_child_directory(&path, report, "tenant")? else {
                continue;
            };
            self.scan_artifacts(&tenant_root, &mut physical, &mut observed, report)?;
        }
        Ok((physical, observed))
    }

    fn scan_artifacts(
        &self,
        tenant_root: &Path,
        physical: &mut ObjectPathMap,
        observed: &mut ObjectIdentitySet,
        report: &mut VolumeIntegrityReport,
    ) -> AgentResult<()> {
        let artifacts = match required_child_directory(tenant_root, ARTIFACTS_DIRECTORY, report)? {
            Some(path) => path,
            None => return Ok(()),
        };
        let entries =
            read_directory(&artifacts).map_err(|error| scan_io_error(&artifacts, error))?;
        for entry in entries {
            let path = entry.path();
            let Some(name) = file_name(&entry) else {
                report.push(VolumeIntegrityIssue::new(
                    VolumeIntegrityIssueKind::Unknown,
                    path,
                    None,
                    None,
                    "artifact directory entry is not valid UTF-8",
                ));
                continue;
            };
            let artifact = match ArtifactId::new(name.clone()) {
                Ok(artifact) => artifact,
                Err(_) => {
                    report.push(VolumeIntegrityIssue::new(
                        VolumeIntegrityIssueKind::Unknown,
                        path,
                        None,
                        None,
                        format!("artifact directory name is invalid: {name}"),
                    ));
                    continue;
                }
            };
            let Some(artifact_root) = ordinary_child_directory(&path, report, "artifact")? else {
                continue;
            };
            let namespace = ObjectNamespaceId::from_artifact(&artifact);
            self.scan_object_directory(&artifact_root, namespace, physical, observed, report)?;
        }
        Ok(())
    }

    fn scan_object_directory(
        &self,
        artifact_root: &Path,
        namespace: ObjectNamespaceId,
        physical: &mut ObjectPathMap,
        observed: &mut ObjectIdentitySet,
        report: &mut VolumeIntegrityReport,
    ) -> AgentResult<()> {
        let objects = match required_child_directory(artifact_root, OBJECTS_DIRECTORY, report)? {
            Some(path) => path,
            None => return Ok(()),
        };
        let entries = read_directory(&objects).map_err(|error| scan_io_error(&objects, error))?;
        for entry in entries {
            let path = entry.path();
            let Some(name) = file_name(&entry) else {
                report.push(VolumeIntegrityIssue::new(
                    VolumeIntegrityIssueKind::Unknown,
                    path,
                    Some(namespace.clone()),
                    None,
                    "object entry is not valid UTF-8",
                ));
                continue;
            };
            if name == OBJECT_TEMP_DIRECTORY {
                if ordinary_child_directory(&path, report, "object temporary")?.is_none() {
                    continue;
                }
                continue;
            }
            let object_id = match name.parse::<ObjectId>() {
                Ok(object_id) => object_id,
                Err(_) => {
                    report.push(VolumeIntegrityIssue::new(
                        VolumeIntegrityIssueKind::Unknown,
                        path,
                        Some(namespace.clone()),
                        None,
                        format!("object filename is not a canonical BLAKE3 ID: {name}"),
                    ));
                    continue;
                }
            };
            observed.insert((namespace.clone(), object_id));
            report.scanned_objects = report.scanned_objects.saturating_add(1);
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) => {
                    report.push(VolumeIntegrityIssue::new(
                        VolumeIntegrityIssueKind::Corrupt,
                        path,
                        Some(namespace.clone()),
                        Some(object_id),
                        format!("cannot inspect object: {error}"),
                    ));
                    continue;
                }
            };
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                report.push(VolumeIntegrityIssue::new(
                    VolumeIntegrityIssueKind::Corrupt,
                    path,
                    Some(namespace.clone()),
                    Some(object_id),
                    "published object is not an ordinary file",
                ));
                continue;
            }
            let expected_size = metadata.len();
            let (actual_size, digest) = match hash_file(&path) {
                Ok(result) => result,
                Err(error) => {
                    report.push(VolumeIntegrityIssue::new(
                        VolumeIntegrityIssueKind::Corrupt,
                        path,
                        Some(namespace.clone()),
                        Some(object_id),
                        error,
                    ));
                    continue;
                }
            };
            let changed = fs::symlink_metadata(&path)
                .map(|after| {
                    after.file_type().is_symlink()
                        || !after.is_file()
                        || after.len() != expected_size
                })
                .unwrap_or(true);
            if changed || actual_size != expected_size {
                report.push(VolumeIntegrityIssue::new(
                    VolumeIntegrityIssueKind::Corrupt,
                    path,
                    Some(namespace.clone()),
                    Some(object_id),
                    format!(
                        "object changed while scanning or has inconsistent size (metadata {expected_size}, read {actual_size})"
                    ),
                ));
                continue;
            }
            if digest.as_bytes() != object_id.digest().as_bytes() {
                report.push(VolumeIntegrityIssue::new(
                    VolumeIntegrityIssueKind::Corrupt,
                    path,
                    Some(namespace.clone()),
                    Some(object_id),
                    format!("BLAKE3 digest does not match object filename: {digest}"),
                ));
                continue;
            }
            report.verified_objects = report.verified_objects.saturating_add(1);
            physical.insert((namespace.clone(), object_id), path);
        }
        Ok(())
    }
}

fn collect_expected(
    mount_root: &Path,
    tenant_id: &TenantId,
    storage_volume_id: &StorageVolumeId,
    inventory: Vec<ObjectPlacement>,
    report: &mut VolumeIntegrityReport,
) -> BTreeMap<(ObjectNamespaceId, ObjectId), Vec<ObjectPlacement>> {
    let mut expected = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for placement in inventory {
        let identity = placement.object_id;
        let key = (placement.object_namespace_id.clone(), placement.object_id);
        if placement.validate().is_err()
            || placement.tenant_id != *tenant_id
            || placement.storage_volume_id.as_ref() != Some(storage_volume_id)
            || placement.archive_id.is_some()
        {
            report.push(VolumeIntegrityIssue::new(
                VolumeIntegrityIssueKind::Unknown,
                mount_root.join(METADATA_DIRECTORY),
                Some(placement.object_namespace_id),
                Some(identity),
                "local placement evidence is malformed or outside this Agent Volume",
            ));
            continue;
        }
        if !matches!(
            placement.state,
            ObjectPlacementState::Verified | ObjectPlacementState::Retiring
        ) {
            continue;
        }
        if !seen.insert((key.0.clone(), key.1, placement.size.get())) {
            continue;
        }
        expected.entry(key).or_insert_with(Vec::new).push(placement);
    }
    expected
}

fn ordinary_directory(path: &Path) -> io::Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "path is not an ordinary directory",
        ));
    }
    Ok(path.to_path_buf())
}

fn child_directory(
    parent: &Path,
    name: &str,
    report: &mut VolumeIntegrityReport,
) -> AgentResult<Option<PathBuf>> {
    let path = parent.join(name);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            report.push(VolumeIntegrityIssue::new(
                VolumeIntegrityIssueKind::Corrupt,
                path,
                None,
                None,
                format!("{name} path is not an ordinary directory"),
            ));
            Ok(None)
        }
        Ok(_) => Ok(Some(path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(scan_io_error(&path, error)),
    }
}

fn required_child_directory(
    parent: &Path,
    name: &str,
    report: &mut VolumeIntegrityReport,
) -> AgentResult<Option<PathBuf>> {
    child_directory(parent, name, report)
}

fn ordinary_child_directory(
    path: &Path,
    report: &mut VolumeIntegrityReport,
    description: &str,
) -> AgentResult<Option<PathBuf>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            report.push(VolumeIntegrityIssue::new(
                VolumeIntegrityIssueKind::Corrupt,
                path,
                None,
                None,
                format!("{description} path is not an ordinary directory"),
            ));
            Ok(None)
        }
        Ok(_) => Ok(Some(path.to_path_buf())),
        Err(error) => Err(scan_io_error(path, error)),
    }
}

fn read_directory(path: &Path) -> io::Result<Vec<fs::DirEntry>> {
    fs::read_dir(path)?.collect()
}

fn file_name(entry: &fs::DirEntry) -> Option<String> {
    entry.file_name().into_string().ok()
}

fn hash_file(path: &Path) -> Result<(u64, blake3::Hash), String> {
    let mut file = File::open(path).map_err(|error| format!("cannot open object: {error}"))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; HASH_BUFFER_SIZE];
    let mut total = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("cannot read object: {error}"))?;
        if read == 0 {
            return Ok((total, hasher.finalize()));
        }
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| "object size exceeds u64".to_owned())?;
        hasher.update(&buffer[..read]);
    }
}

fn scan_io_error(path: &Path, error: io::Error) -> AgentError {
    AgentError::new(
        AgentErrorCode::MountUnavailable,
        format!(
            "failed to inspect Volume integrity path {}: {error}",
            path.display()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryPlacementInventory;
    use neoengram_domain::{
        protocol::{
            materialization::ObjectPlacementState, DecimalU64, ObjectEncoding, PlacementGeneration,
            PlacementId,
        },
        ObjectId,
    };
    use std::io::Write;

    fn setup() -> (
        tempfile::TempDir,
        Arc<InMemoryPlacementInventory>,
        TenantId,
        StorageVolumeId,
        ObjectNamespaceId,
        ObjectId,
    ) {
        let mount = tempfile::tempdir().unwrap();
        let tenant = TenantId::new("tenant-a").unwrap();
        let volume = StorageVolumeId::new("volume-a").unwrap();
        let namespace = ObjectNamespaceId::new("artifact-a").unwrap();
        let object = ObjectId::for_bytes(b"payload");
        let inventory = Arc::new(InMemoryPlacementInventory::default());
        inventory
            .record(ObjectPlacement {
                placement_id: PlacementId::new("placement-a").unwrap(),
                tenant_id: tenant.clone(),
                object_namespace_id: namespace.clone(),
                object_id: object,
                size: DecimalU64::new(7),
                encoding: ObjectEncoding::Raw,
                verified_digest: object.digest(),
                storage_volume_id: Some(volume.clone()),
                archive_id: None,
                placement_generation: PlacementGeneration::new(1),
                state: ObjectPlacementState::Verified,
                failure_domain: "volume:volume-a".to_owned(),
            })
            .unwrap();
        let object_path = mount
            .path()
            .join(".neoengram/objects/tenants/tenant-a/artifacts/artifact-a/objects");
        fs::create_dir_all(&object_path).unwrap();
        (mount, inventory, tenant, volume, namespace, object)
    }

    fn write_object(mount: &Path, namespace: &ObjectNamespaceId, id: ObjectId, bytes: &[u8]) {
        let path = mount
            .join(".neoengram/objects/tenants/tenant-a/artifacts")
            .join(namespace.as_str())
            .join("objects");
        fs::create_dir_all(&path).unwrap();
        fs::File::create(path.join(id.to_hex()))
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }

    #[test]
    fn healthy_object_is_verified() {
        let (mount, inventory, tenant, volume, namespace, object) = setup();
        write_object(mount.path(), &namespace, object, b"payload");
        let report = VolumeIntegrityScanner::new(mount.path(), tenant, volume, inventory)
            .scan()
            .unwrap();
        assert!(report.is_healthy(), "{report:?}");
        assert_eq!(report.scanned_objects, 1);
        assert_eq!(report.verified_objects, 1);
    }

    #[test]
    fn missing_object_is_reported() {
        let (mount, inventory, tenant, volume, _, _) = setup();
        let report = VolumeIntegrityScanner::new(mount.path(), tenant, volume, inventory)
            .scan()
            .unwrap();
        assert_eq!(report.missing.len(), 1);
    }

    #[test]
    fn corrupt_object_is_reported() {
        let (mount, inventory, tenant, volume, namespace, object) = setup();
        write_object(mount.path(), &namespace, object, b"broken");
        let report = VolumeIntegrityScanner::new(mount.path(), tenant, volume, inventory)
            .scan()
            .unwrap();
        assert_eq!(report.corrupt.len(), 1);
        assert!(report.missing.is_empty());
    }

    #[test]
    fn valid_unindexed_object_is_orphan() {
        let (mount, inventory, tenant, volume, namespace, _) = setup();
        let object = ObjectId::for_bytes(b"orphan");
        write_object(mount.path(), &namespace, object, b"orphan");
        let report = VolumeIntegrityScanner::new(mount.path(), tenant, volume, inventory)
            .scan()
            .unwrap();
        assert_eq!(report.orphan.len(), 1);
        assert_eq!(report.missing.len(), 1);
    }

    #[test]
    fn malformed_object_name_is_unknown() {
        let (mount, inventory, tenant, volume, namespace, _) = setup();
        let path = mount
            .path()
            .join(".neoengram/objects/tenants/tenant-a/artifacts")
            .join(namespace.as_str())
            .join("objects/not-an-object-id");
        fs::write(path, b"bad").unwrap();
        let report = VolumeIntegrityScanner::new(mount.path(), tenant, volume, inventory)
            .scan()
            .unwrap();
        assert_eq!(report.unknown.len(), 1);
    }
}
