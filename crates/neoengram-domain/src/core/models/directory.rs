use serde::{Deserialize, Serialize};

use crate::{
    canonical, DirectoryId, ManifestId, PathComponent, ValidationError, ValidationErrorKind,
    ValidationResult,
};

/// The kind of an immutable direct directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectoryEntryKind {
    /// The entry targets an immutable file Manifest.
    File,
    /// The entry targets another immutable Directory.
    Directory,
}

/// A strongly typed target of an immutable Directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum DirectoryEntryTarget {
    /// Canonical identity of a file Manifest.
    Manifest(ManifestId),
    /// Canonical identity of a child Directory.
    Directory(DirectoryId),
}

impl DirectoryEntryTarget {
    /// Returns the entry kind implied by the typed target.
    pub const fn kind(self) -> DirectoryEntryKind {
        match self {
            Self::Manifest(_) => DirectoryEntryKind::File,
            Self::Directory(_) => DirectoryEntryKind::Directory,
        }
    }

    /// Returns the target's raw canonical digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        match self {
            Self::Manifest(id) => id.as_bytes(),
            Self::Directory(id) => id.as_bytes(),
        }
    }
}

/// A direct child of an immutable content-addressed directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    /// Zero-based position in the directory's canonical name ordering.
    pub ordinal: u64,
    /// A single normalized UTF-8 path component.
    pub name: PathComponent,
    /// Whether the target is a file Manifest or another Directory.
    pub kind: DirectoryEntryKind,
    /// Strongly typed Manifest or child Directory identity.
    pub target_id: DirectoryEntryTarget,
    /// Logical file size. Directories always store zero here.
    pub total_size: u64,
}

impl DirectoryEntry {
    /// Constructs a direct file entry.
    pub fn file(
        ordinal: u64,
        name: PathComponent,
        manifest_id: ManifestId,
        total_size: u64,
    ) -> Self {
        Self {
            ordinal,
            name,
            kind: DirectoryEntryKind::File,
            target_id: DirectoryEntryTarget::Manifest(manifest_id),
            total_size,
        }
    }

    /// Constructs a direct child Directory entry.
    pub fn directory(ordinal: u64, name: PathComponent, directory_id: DirectoryId) -> Self {
        Self {
            ordinal,
            name,
            kind: DirectoryEntryKind::Directory,
            target_id: DirectoryEntryTarget::Directory(directory_id),
            total_size: 0,
        }
    }

    /// Validates kind/target agreement and Directory size invariants.
    pub fn validate(&self) -> ValidationResult<()> {
        if self.kind != self.target_id.kind() {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidDirectory,
                format!("Directory entry kind and target disagree: {}", self.name),
            ));
        }
        if self.kind == DirectoryEntryKind::Directory && self.total_size != 0 {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidDirectory,
                format!("child Directory entry carries a file size: {}", self.name),
            ));
        }
        Ok(())
    }
}

/// An immutable Directory containing only direct children in canonical name order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Directory {
    /// Direct entries with contiguous ordinals and strictly increasing names.
    pub entries: Vec<DirectoryEntry>,
}

impl Directory {
    /// Constructs a Directory after validating canonical order and entry invariants.
    pub fn new(entries: Vec<DirectoryEntry>) -> ValidationResult<Self> {
        let directory = Self { entries };
        directory.validate()?;
        Ok(directory)
    }

    /// Validates entries, ordinals, canonical name order, and portable name uniqueness.
    pub fn validate(&self) -> ValidationResult<()> {
        let mut previous_name: Option<&PathComponent> = None;
        let mut portable_names = std::collections::BTreeSet::new();
        for (expected_ordinal, entry) in self.entries.iter().enumerate() {
            entry.validate()?;
            let expected_ordinal = u64::try_from(expected_ordinal).map_err(|_| {
                ValidationError::new(
                    ValidationErrorKind::Overflow,
                    "Directory entry count exceeds u64",
                )
            })?;
            if entry.ordinal != expected_ordinal {
                return Err(ValidationError::new(
                    ValidationErrorKind::InvalidDirectory,
                    format!("Directory entry ordinal is not contiguous: {}", entry.name),
                ));
            }
            if previous_name.is_some_and(|previous| previous >= &entry.name) {
                return Err(ValidationError::new(
                    ValidationErrorKind::InvalidDirectory,
                    format!(
                        "Directory entries must be strictly ordered by name: {}",
                        entry.name
                    ),
                ));
            }
            if !portable_names.insert(entry.name.portable_key()) {
                return Err(ValidationError::new(
                    ValidationErrorKind::InvalidDirectory,
                    format!(
                        "Directory entry names collide under portable case rules: {}",
                        entry.name
                    ),
                ));
            }
            previous_name = Some(&entry.name);
        }
        Ok(())
    }

    /// Computes the canonical `neoengram-directory-v1` identity.
    pub fn canonical_id(&self) -> ValidationResult<DirectoryId> {
        canonical::directory_id(self)
    }
}
