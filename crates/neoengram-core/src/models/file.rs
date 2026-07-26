use serde::{Deserialize, Serialize};

use crate::{
    canonical, ChunkRef, LogicalPath, ManifestId, ValidationError, ValidationErrorKind,
    ValidationResult,
};

/// 生成文件 Manifest 时采用的分块策略。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkingStrategy {
    /// 使用内容定义边界的 FastCDC 分块。
    #[default]
    FastCdc,
    /// 将整个非空文件保存为单个对象。
    WholeFile,
}

/// An immutable file Manifest containing ordered payload chunk references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Total logical file length in bytes.
    pub total_size: u64,
    /// Chunking strategy used to construct the Manifest.
    pub chunking: ChunkingStrategy,
    /// Contiguous chunks ordered by their logical file offset.
    pub chunks: Vec<ChunkRef>,
}

impl Manifest {
    /// Constructs a Manifest after validating chunk coverage and strategy invariants.
    pub fn new(
        total_size: u64,
        chunking: ChunkingStrategy,
        chunks: Vec<ChunkRef>,
    ) -> ValidationResult<Self> {
        let manifest = Self {
            total_size,
            chunking,
            chunks,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validates contiguous coverage, exact total size, and WholeFile cardinality.
    pub fn validate(&self) -> ValidationResult<()> {
        let mut next_offset = 0_u64;
        for chunk in &self.chunks {
            chunk.validate().map_err(|error| {
                ValidationError::new(
                    ValidationErrorKind::InvalidManifest,
                    format!("invalid Manifest chunk: {error}"),
                )
            })?;
            if chunk.offset != next_offset {
                return Err(ValidationError::new(
                    ValidationErrorKind::InvalidManifest,
                    format!(
                        "Manifest chunks are not contiguous at object {}",
                        chunk.object_id
                    ),
                ));
            }
            next_offset = next_offset.checked_add(chunk.size).ok_or_else(|| {
                ValidationError::new(
                    ValidationErrorKind::Overflow,
                    "Manifest total chunk size exceeds u64",
                )
            })?;
        }
        if next_offset != self.total_size {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidManifest,
                "Manifest chunks do not cover the declared total size",
            ));
        }
        if self.chunking == ChunkingStrategy::WholeFile
            && !((self.total_size == 0 && self.chunks.is_empty())
                || (self.total_size > 0 && self.chunks.len() == 1))
        {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidManifest,
                "WholeFile Manifest must contain zero chunks for an empty file or one complete chunk",
            ));
        }
        Ok(())
    }

    /// Computes the canonical `neoengram-manifest-v4` identity.
    pub fn canonical_id(&self) -> ValidationResult<ManifestId> {
        canonical::manifest_id(self)
    }

    /// Returns the number of chunk references as a checked `u64`.
    pub fn chunk_count(&self) -> ValidationResult<u64> {
        u64::try_from(self.chunks.len()).map_err(|_| {
            ValidationError::new(
                ValidationErrorKind::Overflow,
                "Manifest chunk count exceeds u64",
            )
        })
    }
}

/// One path-to-Manifest record in an Index snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRecord {
    /// Portable repository-relative path.
    pub path: LogicalPath,
    /// Canonical identity of the immutable file Manifest.
    pub manifest_id: ManifestId,
    /// Total logical file length in bytes.
    pub total_size: u64,
    /// Number of chunks recorded by the Manifest.
    pub chunk_count: u64,
}

impl FileRecord {
    /// Constructs and validates an Index file record.
    pub fn new(
        path: LogicalPath,
        manifest_id: ManifestId,
        total_size: u64,
        chunk_count: u64,
    ) -> ValidationResult<Self> {
        let record = Self {
            path,
            manifest_id,
            total_size,
            chunk_count,
        };
        record.validate()?;
        Ok(record)
    }

    /// Creates an Index record from a validated Manifest and its canonical ID.
    pub fn from_manifest(path: LogicalPath, manifest: &Manifest) -> ValidationResult<Self> {
        manifest.validate()?;
        Self::new(
            path,
            manifest.canonical_id()?,
            manifest.total_size,
            manifest.chunk_count()?,
        )
    }

    /// Validates the relationship between the logical size and chunk count.
    pub fn validate(&self) -> ValidationResult<()> {
        if (self.total_size == 0) != (self.chunk_count == 0) {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidIndex,
                format!(
                    "file {} has inconsistent total size and chunk count",
                    self.path
                ),
            ));
        }
        Ok(())
    }
}
