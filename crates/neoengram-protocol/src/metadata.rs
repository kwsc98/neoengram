use std::collections::{BTreeMap, BTreeSet};

use neoengram_core::{
    ChunkRef, ChunkingStrategy, ContentDigest, FileRecord, IndexDelta, IndexMutation, IndexVersion,
    LogicalPath, Manifest, ManifestId, ObjectId, ObjectSpec,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::validation::{parse_unique_json, validate_extension_keys, CONTENT_DIGEST_PATTERN};
use crate::{
    jcs_blake3, jcs_bytes, ArtifactId, ArtifactPlacementId, DecimalU64, Extensions, IndexRevision,
    JobId, MetadataBatchId, ObjectReceiptId, PlaygroundId, ProjectId, ProtocolError,
    ProtocolResult, StorageVolumeId, TenantId, UnixMillis, MAX_METADATA_PAGE_BYTES,
    MAX_RECORDS_PER_PAGE,
};

pub const METADATA_SCHEMA_VERSION_V1: u16 = 1;

/// Object identity and byte length without a physical storage path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WireObjectSpec {
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub object_id: ObjectId,
    pub size: DecimalU64,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl WireObjectSpec {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_extension_keys(&self.extensions, &["object_id", "size"])
    }
}

/// An IndexVersion adapted to the wire's decimal-string integer rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WireIndexVersion {
    pub revision: IndexRevision,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub digest: ContentDigest,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl WireIndexVersion {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_extension_keys(&self.extensions, &["revision", "digest"])
    }
}

impl From<&IndexVersion> for WireIndexVersion {
    fn from(value: &IndexVersion) -> Self {
        Self {
            revision: IndexRevision::new(value.revision),
            digest: value.digest,
            extensions: Extensions::new(),
        }
    }
}

impl From<IndexVersion> for WireIndexVersion {
    fn from(value: IndexVersion) -> Self {
        Self::from(&value)
    }
}

impl From<WireIndexVersion> for IndexVersion {
    fn from(value: WireIndexVersion) -> Self {
        Self {
            revision: value.revision.get(),
            digest: value.digest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetadataBatchKind {
    Manifest,
    IndexDelta,
    ObjectReceipt,
}

/// Full authority and CAS scope shared by a descriptor and every page in its batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MetadataBatchScope {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: PlaygroundId,
    pub job_id: JobId,
    pub base_index_version: WireIndexVersion,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl MetadataBatchScope {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.base_index_version.validate()?;
        validate_extension_keys(
            &self.extensions,
            &[
                "tenant_id",
                "project_id",
                "artifact_id",
                "playground_id",
                "job_id",
                "base_index_version",
            ],
        )
    }
}

/// Ordered integrity entry for one page. `encoded_bytes` is the RFC 8785 JCS byte length.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MetadataPageDescriptor {
    pub page_number: u32,
    pub record_count: u32,
    /// Byte length of this page's complete RFC 8785 canonical JSON representation.
    pub encoded_bytes: DecimalU64,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub page_digest: ContentDigest,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl MetadataPageDescriptor {
    fn validate(&self) -> ProtocolResult<()> {
        validate_extension_keys(
            &self.extensions,
            &[
                "page_number",
                "record_count",
                "encoded_bytes",
                "page_digest",
            ],
        )
    }
}

/// Immutable declaration uploaded before any page is accepted into staging.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MetadataBatchDescriptor {
    pub batch_id: MetadataBatchId,
    pub scope: MetadataBatchScope,
    pub kind: MetadataBatchKind,
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub schema_version: u16,
    pub page_count: u32,
    pub record_count: DecimalU64,
    /// Sum of the canonical JSON byte lengths declared by `pages`.
    pub encoded_bytes: DecimalU64,
    pub pages: Vec<MetadataPageDescriptor>,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub batch_digest: ContentDigest,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl MetadataBatchDescriptor {
    pub fn from_pages(
        batch_id: MetadataBatchId,
        scope: MetadataBatchScope,
        pages: &[MetadataBatchPage],
        extensions: Extensions,
    ) -> ProtocolResult<Self> {
        let first = pages.first().ok_or_else(|| ProtocolError::InvalidField {
            field: "metadata_batch.pages",
            reason: "must contain at least one page".to_owned(),
        })?;
        let kind = first.kind();
        let page_count = u32::try_from(pages.len()).map_err(|_| ProtocolError::LimitExceeded {
            limit_name: "metadata batch page count",
            limit: u32::MAX as usize,
            actual: pages.len(),
        })?;
        let mut page_descriptors = Vec::with_capacity(pages.len());
        let mut record_count = 0_u64;
        let mut encoded_bytes = 0_u64;
        for page in pages {
            page.validate()?;
            if page.batch_id != batch_id
                || page.scope != scope
                || page.kind() != kind
                || page.page_count != page_count
            {
                return Err(ProtocolError::InvalidField {
                    field: "metadata_batch.pages",
                    reason: "page batch ID, scope, kind, or page count differs from descriptor"
                        .to_owned(),
                });
            }
            let page_records =
                u32::try_from(page.record_count()).map_err(|_| ProtocolError::LimitExceeded {
                    limit_name: "metadata page records",
                    limit: u32::MAX as usize,
                    actual: page.record_count(),
                })?;
            let page_bytes =
                u64::try_from(jcs_bytes(page)?.len()).map_err(|_| ProtocolError::InvalidField {
                    field: "metadata_page.encoded_bytes",
                    reason: "encoded length does not fit u64".to_owned(),
                })?;
            record_count = record_count
                .checked_add(u64::from(page_records))
                .ok_or_else(|| ProtocolError::InvalidField {
                    field: "metadata_batch.record_count",
                    reason: "record count overflow".to_owned(),
                })?;
            encoded_bytes = encoded_bytes.checked_add(page_bytes).ok_or_else(|| {
                ProtocolError::InvalidField {
                    field: "metadata_batch.encoded_bytes",
                    reason: "encoded byte count overflow".to_owned(),
                }
            })?;
            page_descriptors.push(MetadataPageDescriptor {
                page_number: page.page_number,
                record_count: page_records,
                encoded_bytes: DecimalU64::new(page_bytes),
                page_digest: page.page_digest,
                extensions: Extensions::new(),
            });
        }
        let mut descriptor = Self {
            batch_id,
            scope,
            kind,
            schema_version: METADATA_SCHEMA_VERSION_V1,
            page_count,
            record_count: DecimalU64::new(record_count),
            encoded_bytes: DecimalU64::new(encoded_bytes),
            pages: page_descriptors,
            batch_digest: empty_digest()?,
            extensions,
        };
        descriptor.batch_digest = descriptor.computed_digest()?;
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn computed_digest(&self) -> ProtocolResult<ContentDigest> {
        jcs_blake3(&MetadataBatchDigestInput {
            batch_id: &self.batch_id,
            scope: &self.scope,
            kind: self.kind,
            schema_version: self.schema_version,
            page_count: self.page_count,
            record_count: self.record_count,
            encoded_bytes: self.encoded_bytes,
            pages: &self.pages,
            extensions: &self.extensions,
        })
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        if self.schema_version != METADATA_SCHEMA_VERSION_V1 {
            return Err(ProtocolError::InvalidField {
                field: "metadata_batch.schema_version",
                reason: format!("unsupported schema version {}", self.schema_version),
            });
        }
        self.scope.validate()?;
        if self.page_count == 0 || self.page_count as usize != self.pages.len() {
            return Err(ProtocolError::InvalidField {
                field: "metadata_batch.page_count",
                reason: "must be non-zero and match pages length".to_owned(),
            });
        }
        let mut records = 0_u64;
        let mut bytes = 0_u64;
        for (expected, page) in self.pages.iter().enumerate() {
            page.validate()?;
            if page.page_number as usize != expected {
                return Err(ProtocolError::InvalidField {
                    field: "metadata_batch.pages",
                    reason: "page descriptors must be consecutive and ordered from zero".to_owned(),
                });
            }
            if page.record_count as usize > MAX_RECORDS_PER_PAGE {
                return Err(ProtocolError::LimitExceeded {
                    limit_name: "metadata page records",
                    limit: MAX_RECORDS_PER_PAGE,
                    actual: page.record_count as usize,
                });
            }
            if page.encoded_bytes.get() > MAX_METADATA_PAGE_BYTES as u64 {
                return Err(ProtocolError::LimitExceeded {
                    limit_name: "metadata page bytes",
                    limit: MAX_METADATA_PAGE_BYTES,
                    actual: usize::try_from(page.encoded_bytes.get()).unwrap_or(usize::MAX),
                });
            }
            records = records
                .checked_add(u64::from(page.record_count))
                .ok_or_else(|| ProtocolError::InvalidField {
                    field: "metadata_batch.record_count",
                    reason: "record count overflow".to_owned(),
                })?;
            bytes = bytes.checked_add(page.encoded_bytes.get()).ok_or_else(|| {
                ProtocolError::InvalidField {
                    field: "metadata_batch.encoded_bytes",
                    reason: "encoded byte count overflow".to_owned(),
                }
            })?;
        }
        if records != self.record_count.get() || bytes != self.encoded_bytes.get() {
            return Err(ProtocolError::InvalidField {
                field: "metadata_batch totals",
                reason: "record_count or encoded_bytes does not match ordered page descriptors"
                    .to_owned(),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "batch_id",
                "scope",
                "kind",
                "schema_version",
                "page_count",
                "record_count",
                "encoded_bytes",
                "pages",
                "batch_digest",
            ],
        )?;
        let computed = self.computed_digest()?;
        if computed != self.batch_digest {
            return Err(ProtocolError::InvalidDigest(format!(
                "metadata batch digest mismatch: expected {}, observed {}",
                self.batch_digest, computed
            )));
        }
        Ok(())
    }

    /// Validates that a received page belongs to this exact descriptor and integrity sequence.
    pub fn validate_page(&self, page: &MetadataBatchPage) -> ProtocolResult<()> {
        self.validate()?;
        page.validate()?;
        if page.batch_id != self.batch_id
            || page.scope != self.scope
            || page.kind() != self.kind
            || page.schema_version != self.schema_version
            || page.page_count != self.page_count
        {
            return Err(ProtocolError::InvalidField {
                field: "metadata_page.scope",
                reason: "page does not match descriptor batch ID, scope, kind, schema, or count"
                    .to_owned(),
            });
        }
        let expected = self.pages.get(page.page_number as usize).ok_or_else(|| {
            ProtocolError::InvalidField {
                field: "metadata_page.page_number",
                reason: "page is not declared by descriptor".to_owned(),
            }
        })?;
        let encoded_bytes =
            u64::try_from(jcs_bytes(page)?.len()).map_err(|_| ProtocolError::InvalidField {
                field: "metadata_page.encoded_bytes",
                reason: "encoded length does not fit u64".to_owned(),
            })?;
        if expected.record_count as usize != page.record_count()
            || expected.encoded_bytes.get() != encoded_bytes
            || expected.page_digest != page.page_digest
        {
            return Err(ProtocolError::InvalidDigest(
                "page does not match its ordered descriptor entry".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct MetadataBatchDigestInput<'a> {
    batch_id: &'a MetadataBatchId,
    scope: &'a MetadataBatchScope,
    kind: MetadataBatchKind,
    schema_version: u16,
    page_count: u32,
    record_count: DecimalU64,
    encoded_bytes: DecimalU64,
    pages: &'a [MetadataPageDescriptor],
    #[serde(flatten)]
    extensions: &'a Extensions,
}

/// One independently verified metadata page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MetadataBatchPage {
    pub batch_id: MetadataBatchId,
    pub scope: MetadataBatchScope,
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub schema_version: u16,
    pub page_number: u32,
    pub page_count: u32,
    #[serde(flatten)]
    pub records: MetadataBatchRecords,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub page_digest: ContentDigest,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl MetadataBatchPage {
    /// Decodes and validates one raw I-JSON page, rejecting duplicate object members and enforcing
    /// the byte limit before JSON allocation.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        if bytes.len() > MAX_METADATA_PAGE_BYTES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "metadata page bytes",
                limit: MAX_METADATA_PAGE_BYTES,
                actual: bytes.len(),
            });
        }
        let page: Self = serde_json::from_value(parse_unique_json(bytes)?)?;
        page.validate()?;
        Ok(page)
    }

    pub fn new(
        batch_id: MetadataBatchId,
        scope: MetadataBatchScope,
        page_number: u32,
        page_count: u32,
        records: MetadataBatchRecords,
        extensions: Extensions,
    ) -> ProtocolResult<Self> {
        let mut page = Self {
            batch_id,
            scope,
            schema_version: METADATA_SCHEMA_VERSION_V1,
            page_number,
            page_count,
            records,
            page_digest: empty_digest()?,
            extensions,
        };
        page.page_digest = page.computed_digest()?;
        page.validate()?;
        Ok(page)
    }

    #[must_use]
    pub const fn kind(&self) -> MetadataBatchKind {
        self.records.kind()
    }

    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    pub fn computed_digest(&self) -> ProtocolResult<ContentDigest> {
        jcs_blake3(&MetadataPageDigestInput {
            batch_id: &self.batch_id,
            scope: &self.scope,
            schema_version: self.schema_version,
            page_number: self.page_number,
            page_count: self.page_count,
            records: &self.records,
            extensions: &self.extensions,
        })
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        if self.schema_version != METADATA_SCHEMA_VERSION_V1 {
            return Err(ProtocolError::InvalidField {
                field: "metadata_page.schema_version",
                reason: format!("unsupported schema version {}", self.schema_version),
            });
        }
        self.scope.validate()?;
        if self.page_count == 0 || self.page_number >= self.page_count {
            return Err(ProtocolError::InvalidField {
                field: "metadata_page.page_number",
                reason: format!(
                    "page {} is outside page_count {}",
                    self.page_number, self.page_count
                ),
            });
        }
        if self.record_count() > MAX_RECORDS_PER_PAGE {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "metadata page records",
                limit: MAX_RECORDS_PER_PAGE,
                actual: self.record_count(),
            });
        }
        self.records.validate()?;
        validate_extension_keys(
            &self.extensions,
            &[
                "batch_id",
                "scope",
                "schema_version",
                "page_number",
                "page_count",
                "kind",
                "records",
                "page_digest",
            ],
        )?;
        let computed = self.computed_digest()?;
        if computed != self.page_digest {
            return Err(ProtocolError::InvalidDigest(format!(
                "metadata page digest mismatch: expected {}, observed {}",
                self.page_digest, computed
            )));
        }
        let encoded = jcs_bytes(self)?;
        if encoded.len() > MAX_METADATA_PAGE_BYTES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "metadata page bytes",
                limit: MAX_METADATA_PAGE_BYTES,
                actual: encoded.len(),
            });
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct MetadataPageDigestInput<'a> {
    batch_id: &'a MetadataBatchId,
    scope: &'a MetadataBatchScope,
    schema_version: u16,
    page_number: u32,
    page_count: u32,
    #[serde(flatten)]
    records: &'a MetadataBatchRecords,
    #[serde(flatten)]
    extensions: &'a Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "records", rename_all = "snake_case")]
pub enum MetadataBatchRecords {
    Manifest(#[schemars(length(max = 4096))] Vec<ManifestRecord>),
    IndexDelta(#[schemars(length(max = 4096))] Vec<IndexDeltaRecord>),
    ObjectReceipt(#[schemars(length(max = 4096))] Vec<ObjectReceiptRecord>),
}

impl MetadataBatchRecords {
    #[must_use]
    pub const fn kind(&self) -> MetadataBatchKind {
        match self {
            Self::Manifest(_) => MetadataBatchKind::Manifest,
            Self::IndexDelta(_) => MetadataBatchKind::IndexDelta,
            Self::ObjectReceipt(_) => MetadataBatchKind::ObjectReceipt,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Manifest(records) => records.len(),
            Self::IndexDelta(records) => records.len(),
            Self::ObjectReceipt(records) => records.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Manifest(records) => records.iter().try_for_each(ManifestRecord::validate),
            Self::IndexDelta(records) => records.iter().try_for_each(IndexDeltaRecord::validate),
            Self::ObjectReceipt(records) => {
                records.iter().try_for_each(ObjectReceiptRecord::validate)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ManifestRecord {
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub manifest_id: ManifestId,
    pub total_size: DecimalU64,
    pub chunking: WireChunkingStrategy,
    /// Zero-based ordinal of the first chunk carried by this fragment.
    pub chunk_start: DecimalU64,
    pub chunks: Vec<WireChunkRef>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl ManifestRecord {
    fn validate(&self) -> ProtocolResult<()> {
        self.chunks.iter().try_for_each(WireChunkRef::validate)?;
        validate_extension_keys(
            &self.extensions,
            &[
                "manifest_id",
                "total_size",
                "chunking",
                "chunk_start",
                "chunks",
            ],
        )?;

        let fragment_chunk_count =
            u64::try_from(self.chunks.len()).map_err(|_| ProtocolError::InvalidField {
                field: "manifest.chunk_start",
                reason: "fragment chunk count does not fit u64".to_owned(),
            })?;
        self.chunk_start
            .get()
            .checked_add(fragment_chunk_count)
            .ok_or_else(|| ProtocolError::InvalidField {
                field: "manifest.chunk_start",
                reason: "fragment chunk ordinal range exceeds u64".to_owned(),
            })?;

        let Some(first) = self.chunks.first() else {
            if self.chunk_start.get() != 0 || self.total_size.get() != 0 {
                return Err(ProtocolError::InvalidField {
                    field: "manifest.fragment",
                    reason: "an empty fragment is only valid for an empty Manifest at chunk ordinal zero"
                        .to_owned(),
                });
            }
            return Ok(());
        };
        if self.total_size.get() == 0 {
            return Err(ProtocolError::InvalidField {
                field: "manifest.total_size",
                reason: "a zero-length Manifest cannot contain chunks".to_owned(),
            });
        }
        if self.chunk_start.get() == 0 && first.offset.get() != 0 {
            return Err(ProtocolError::InvalidField {
                field: "manifest.chunks",
                reason: "the first Manifest fragment must begin at byte offset zero".to_owned(),
            });
        }

        let mut next_offset = first.offset.get();
        for chunk in &self.chunks {
            if chunk.offset.get() != next_offset {
                return Err(ProtocolError::InvalidField {
                    field: "manifest.chunks",
                    reason: "chunks within a Manifest fragment must be contiguous".to_owned(),
                });
            }
            next_offset = next_offset.checked_add(chunk.size.get()).ok_or_else(|| {
                ProtocolError::InvalidField {
                    field: "manifest.chunks",
                    reason: "fragment byte range exceeds u64".to_owned(),
                }
            })?;
        }
        if next_offset > self.total_size.get() {
            return Err(ProtocolError::InvalidField {
                field: "manifest.chunks",
                reason: "Manifest fragment extends beyond total_size".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WireChunkingStrategy {
    FastCdc,
    WholeFile,
}

impl From<ChunkingStrategy> for WireChunkingStrategy {
    fn from(value: ChunkingStrategy) -> Self {
        match value {
            ChunkingStrategy::FastCdc => Self::FastCdc,
            ChunkingStrategy::WholeFile => Self::WholeFile,
        }
    }
}

impl From<WireChunkingStrategy> for ChunkingStrategy {
    fn from(value: WireChunkingStrategy) -> Self {
        match value {
            WireChunkingStrategy::FastCdc => Self::FastCdc,
            WireChunkingStrategy::WholeFile => Self::WholeFile,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WireChunkRef {
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub object_id: ObjectId,
    pub offset: DecimalU64,
    pub size: DecimalU64,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl WireChunkRef {
    fn validate(&self) -> ProtocolResult<()> {
        if self.size.get() == 0 {
            return Err(ProtocolError::InvalidField {
                field: "manifest.chunk.size",
                reason: "must be greater than zero".to_owned(),
            });
        }
        self.offset
            .get()
            .checked_add(self.size.get())
            .ok_or_else(|| ProtocolError::InvalidField {
                field: "manifest.chunk",
                reason: "offset plus size exceeds u64".to_owned(),
            })?;
        validate_extension_keys(&self.extensions, &["object_id", "offset", "size"])
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mutation", rename_all = "snake_case")]
pub enum IndexDeltaRecord {
    Upsert {
        #[schemars(with = "String", length(min = 1))]
        path: LogicalPath,
        #[schemars(
            with = "String",
            length(equal = 64),
            regex(pattern = CONTENT_DIGEST_PATTERN)
        )]
        manifest_id: ManifestId,
        total_size: DecimalU64,
        chunk_count: DecimalU64,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
    Delete {
        #[schemars(with = "String", length(min = 1))]
        path: LogicalPath,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
}

impl IndexDeltaRecord {
    fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Upsert {
                total_size,
                chunk_count,
                extensions,
                ..
            } => {
                if (total_size.get() == 0) != (chunk_count.get() == 0) {
                    return Err(ProtocolError::InvalidField {
                        field: "index_delta.upsert",
                        reason: "total_size and chunk_count must both be zero or both be non-zero"
                            .to_owned(),
                    });
                }
                validate_extension_keys(
                    extensions,
                    &[
                        "mutation",
                        "path",
                        "manifest_id",
                        "total_size",
                        "chunk_count",
                    ],
                )
            }
            Self::Delete { extensions, .. } => {
                validate_extension_keys(extensions, &["mutation", "path"])
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ObjectReceiptRecord {
    pub receipt_id: ObjectReceiptId,
    pub tenant_id: TenantId,
    pub artifact_id: ArtifactId,
    pub job_id: JobId,
    pub storage_volume_id: StorageVolumeId,
    pub artifact_placement_id: ArtifactPlacementId,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub object_id: ObjectId,
    pub size: DecimalU64,
    pub verified_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl ObjectReceiptRecord {
    fn validate(&self) -> ProtocolResult<()> {
        validate_extension_keys(
            &self.extensions,
            &[
                "receipt_id",
                "tenant_id",
                "artifact_id",
                "job_id",
                "storage_volume_id",
                "artifact_placement_id",
                "object_id",
                "size",
                "verified_at_unix_ms",
            ],
        )
    }
}

/// Canonical logical publication reconstructed from complete MetadataBatch pages.
///
/// Receipt identities, timestamps, placement, page boundaries, and JSON extensions remain wire
/// evidence and are deliberately excluded from this model. Their enclosing pages are still bound
/// by descriptor and JCS digests before reconstruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataPublication {
    pub index_mutations: Vec<IndexMutation>,
    pub manifests: Vec<Manifest>,
    pub object_specs: Vec<ObjectSpec>,
}

impl MetadataPublication {
    /// Reassembles and validates one closed publication graph from complete metadata pages.
    pub fn from_pages(pages: &[MetadataBatchPage]) -> ProtocolResult<Self> {
        let mut ordered_pages = BTreeMap::<(usize, u32), &MetadataBatchPage>::new();
        let mut batch_ids: [Option<&MetadataBatchId>; 3] = [None, None, None];
        let mut page_counts = [0_u32; 3];
        for page in pages {
            page.validate()?;
            let kind = metadata_kind_index(page.kind());
            if let Some(batch_id) = batch_ids[kind] {
                if batch_id != &page.batch_id {
                    return Err(invalid_publication(
                        "publication contains more than one batch of the same metadata kind",
                    ));
                }
            } else {
                batch_ids[kind] = Some(&page.batch_id);
                page_counts[kind] = page.page_count;
            }
            if page_counts[kind] != page.page_count
                || ordered_pages
                    .insert((kind, page.page_number), page)
                    .is_some()
            {
                return Err(invalid_publication(
                    "publication metadata pages repeat or disagree on page_count",
                ));
            }
        }
        for (kind, page_count) in page_counts.into_iter().enumerate() {
            for page_number in 0..page_count {
                if !ordered_pages.contains_key(&(kind, page_number)) {
                    return Err(invalid_publication(
                        "publication metadata pages have a sequence gap",
                    ));
                }
            }
        }

        let mut manifest_fragments = BTreeMap::<ManifestId, PublicationManifestFragments>::new();
        let mut index_mutations = Vec::new();
        let mut object_specs = BTreeMap::<ObjectId, u64>::new();
        let mut receipt_ids = BTreeSet::new();
        for page in ordered_pages.values() {
            match &page.records {
                MetadataBatchRecords::Manifest(records) => {
                    for record in records {
                        let chunks = record
                            .chunks
                            .iter()
                            .map(|chunk| {
                                ChunkRef::new(chunk.object_id, chunk.offset.get(), chunk.size.get())
                            })
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|error| invalid_publication(error.to_string()))?;
                        let fragments = manifest_fragments
                            .entry(record.manifest_id)
                            .or_insert_with(|| PublicationManifestFragments {
                                total_size: record.total_size.get(),
                                chunking: record.chunking.into(),
                                chunks_by_start: BTreeMap::new(),
                            });
                        if fragments.total_size != record.total_size.get()
                            || fragments.chunking != record.chunking.into()
                            || fragments
                                .chunks_by_start
                                .insert(record.chunk_start.get(), chunks)
                                .is_some()
                        {
                            return Err(invalid_publication(format!(
                                "Manifest {} fragments conflict or repeat",
                                record.manifest_id
                            )));
                        }
                    }
                }
                MetadataBatchRecords::IndexDelta(records) => {
                    for record in records {
                        index_mutations.push(match record {
                            IndexDeltaRecord::Upsert {
                                path,
                                manifest_id,
                                total_size,
                                chunk_count,
                                ..
                            } => IndexMutation::Upsert {
                                record: FileRecord::new(
                                    path.clone(),
                                    *manifest_id,
                                    total_size.get(),
                                    chunk_count.get(),
                                )
                                .map_err(|error| invalid_publication(error.to_string()))?,
                            },
                            IndexDeltaRecord::Delete { path, .. } => {
                                IndexMutation::Delete { path: path.clone() }
                            }
                        });
                    }
                }
                MetadataBatchRecords::ObjectReceipt(records) => {
                    for record in records {
                        if !receipt_ids.insert(record.receipt_id.clone())
                            || object_specs
                                .insert(record.object_id, record.size.get())
                                .is_some()
                        {
                            return Err(invalid_publication(
                                "publication contains duplicate object receipt material",
                            ));
                        }
                    }
                }
            }
        }

        neoengram_core::validate_index_mutation_paths(
            index_mutations.iter().map(IndexMutation::path),
        )
        .map_err(|error| invalid_publication(error.to_string()))?;

        let mut manifests = BTreeMap::<ManifestId, Manifest>::new();
        let mut manifest_objects = BTreeMap::<ObjectId, u64>::new();
        for (manifest_id, fragments) in manifest_fragments {
            let mut next_chunk_start = 0_u64;
            let mut chunks = Vec::new();
            for (chunk_start, fragment) in fragments.chunks_by_start {
                if chunk_start != next_chunk_start {
                    return Err(invalid_publication(format!(
                        "Manifest {manifest_id} has a chunk ordinal gap"
                    )));
                }
                next_chunk_start =
                    next_chunk_start
                        .checked_add(u64::try_from(fragment.len()).map_err(|_| {
                            invalid_publication("Manifest fragment length exceeds u64")
                        })?)
                        .ok_or_else(|| invalid_publication("Manifest chunk ordinal exceeds u64"))?;
                chunks.extend(fragment);
            }
            let manifest = Manifest::new(fragments.total_size, fragments.chunking, chunks)
                .map_err(|error| invalid_publication(error.to_string()))?;
            let observed = manifest
                .canonical_id()
                .map_err(|error| invalid_publication(error.to_string()))?;
            if observed != manifest_id {
                return Err(invalid_publication(format!(
                    "Manifest {manifest_id} does not match canonical content {observed}"
                )));
            }
            for chunk in &manifest.chunks {
                if manifest_objects
                    .insert(chunk.object_id, chunk.size)
                    .is_some_and(|size| size != chunk.size)
                {
                    return Err(invalid_publication(
                        "publication Manifests disagree about an object size",
                    ));
                }
            }
            manifests.insert(manifest_id, manifest);
        }
        if manifest_objects != object_specs {
            return Err(invalid_publication(
                "Manifest objects and ObjectReceipt records differ",
            ));
        }

        let referenced_manifests = index_mutations
            .iter()
            .filter_map(|mutation| match mutation {
                IndexMutation::Upsert { record } => Some(record.manifest_id),
                IndexMutation::Delete { .. } => None,
            })
            .collect::<BTreeSet<_>>();
        if manifests.keys().copied().collect::<BTreeSet<_>>() != referenced_manifests {
            return Err(invalid_publication(
                "publication Manifests are not the exact Index upsert set",
            ));
        }
        for mutation in &index_mutations {
            if let IndexMutation::Upsert { record } = mutation {
                let manifest = manifests.get(&record.manifest_id).ok_or_else(|| {
                    invalid_publication("Index upsert references an absent Manifest")
                })?;
                if manifest.total_size != record.total_size
                    || manifest
                        .chunk_count()
                        .map_err(|error| invalid_publication(error.to_string()))?
                        != record.chunk_count
                {
                    return Err(invalid_publication(
                        "Index upsert metadata differs from its Manifest",
                    ));
                }
            }
        }

        Ok(Self {
            index_mutations,
            manifests: manifests.into_values().collect(),
            object_specs: object_specs
                .into_iter()
                .map(|(id, size)| ObjectSpec::new(id, size))
                .collect(),
        })
    }

    /// Computes the shared publication digest for this material and wire resource scope.
    pub fn publication_digest(
        &self,
        scope: &MetadataBatchScope,
        result_index_digest: ContentDigest,
    ) -> ProtocolResult<ContentDigest> {
        let base_index_version = IndexVersion::new(
            scope.base_index_version.revision.get(),
            scope.base_index_version.digest,
        );
        let index_delta = IndexDelta::new(
            base_index_version,
            self.index_mutations.clone(),
            result_index_digest,
        )
        .map_err(|error| invalid_publication(error.to_string()))?;
        neoengram_core::canonical::add_publication_digest(
            scope.tenant_id.as_str(),
            scope.project_id.as_str(),
            scope.artifact_id.as_str(),
            scope.playground_id.as_str(),
            scope.job_id.as_str(),
            &base_index_version,
            &index_delta,
            &self.manifests,
            &self.object_specs,
        )
        .map_err(|error| invalid_publication(error.to_string()))
    }
}

struct PublicationManifestFragments {
    total_size: u64,
    chunking: ChunkingStrategy,
    chunks_by_start: BTreeMap<u64, Vec<ChunkRef>>,
}

fn metadata_kind_index(kind: MetadataBatchKind) -> usize {
    match kind {
        MetadataBatchKind::Manifest => 0,
        MetadataBatchKind::IndexDelta => 1,
        MetadataBatchKind::ObjectReceipt => 2,
    }
}

fn invalid_publication(reason: impl Into<String>) -> ProtocolError {
    ProtocolError::InvalidField {
        field: "metadata_publication",
        reason: reason.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum MetadataProtocolSchema {
    Descriptor(MetadataBatchDescriptor),
    Page(MetadataBatchPage),
}

impl MetadataProtocolSchema {
    /// Decodes one metadata DTO through the shared duplicate-key and runtime validator.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        let value = parse_unique_json(bytes)?;
        let message: Self = serde_json::from_value(value)?;
        message.validate()?;
        Ok(message)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Descriptor(message) => message.validate(),
            Self::Page(message) => message.validate(),
        }
    }
}

fn empty_digest() -> ProtocolResult<ContentDigest> {
    jcs_blake3(&())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn object_id() -> ObjectId {
        ObjectId::from_str(&"11".repeat(32)).unwrap()
    }

    fn scope() -> MetadataBatchScope {
        MetadataBatchScope {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id: ProjectId::new("project-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            playground_id: PlaygroundId::new("playground-a").unwrap(),
            job_id: JobId::new("job-a").unwrap(),
            base_index_version: WireIndexVersion {
                revision: IndexRevision::new(4),
                digest: ContentDigest::from_str(&"44".repeat(32)).unwrap(),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        }
    }

    #[test]
    fn page_digest_detects_record_changes() {
        let records = MetadataBatchRecords::ObjectReceipt(vec![ObjectReceiptRecord {
            receipt_id: ObjectReceiptId::new("receipt-1").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            job_id: JobId::new("job-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
            object_id: object_id(),
            size: DecimalU64::new(10),
            verified_at_unix_ms: UnixMillis::new(20),
            extensions: Extensions::new(),
        }]);
        let mut page = MetadataBatchPage::new(
            MetadataBatchId::new("batch-a").unwrap(),
            scope(),
            0,
            1,
            records,
            Extensions::new(),
        )
        .unwrap();
        assert!(page.validate().is_ok());
        let MetadataBatchRecords::ObjectReceipt(records) = &mut page.records else {
            panic!("wrong page kind");
        };
        records[0].size = DecimalU64::new(11);
        assert!(matches!(
            page.validate(),
            Err(ProtocolError::InvalidDigest(_))
        ));
    }

    #[test]
    fn metadata_runtime_rejects_non_v1_schema_versions() {
        let page = MetadataBatchPage::new(
            MetadataBatchId::new("batch-version").unwrap(),
            scope(),
            0,
            1,
            MetadataBatchRecords::Manifest(Vec::new()),
            Extensions::new(),
        )
        .unwrap();
        let descriptor = MetadataBatchDescriptor::from_pages(
            page.batch_id.clone(),
            page.scope.clone(),
            std::slice::from_ref(&page),
            Extensions::new(),
        )
        .unwrap();

        let mut unsupported_page = page;
        unsupported_page.schema_version = 2;
        assert!(matches!(
            unsupported_page.validate(),
            Err(ProtocolError::InvalidField {
                field: "metadata_page.schema_version",
                ..
            })
        ));

        let mut unsupported_descriptor = descriptor;
        unsupported_descriptor.schema_version = 2;
        assert!(matches!(
            unsupported_descriptor.validate(),
            Err(ProtocolError::InvalidField {
                field: "metadata_batch.schema_version",
                ..
            })
        ));
    }

    #[test]
    fn page_rejects_more_than_4096_records() {
        let record = ObjectReceiptRecord {
            receipt_id: ObjectReceiptId::new("receipt-1").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            job_id: JobId::new("job-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
            object_id: object_id(),
            size: DecimalU64::new(10),
            verified_at_unix_ms: UnixMillis::new(20),
            extensions: Extensions::new(),
        };
        let records = MetadataBatchRecords::ObjectReceipt(vec![record; 4097]);
        let result = MetadataBatchPage::new(
            MetadataBatchId::new("batch-a").unwrap(),
            scope(),
            0,
            1,
            records,
            Extensions::new(),
        );
        assert!(matches!(result, Err(ProtocolError::LimitExceeded { .. })));
    }

    #[test]
    fn descriptor_binds_ordered_pages_and_full_scope() {
        let batch_id = MetadataBatchId::new("batch-a").unwrap();
        let scope = scope();
        let first = MetadataBatchPage::new(
            batch_id.clone(),
            scope.clone(),
            0,
            2,
            MetadataBatchRecords::ObjectReceipt(Vec::new()),
            Extensions::new(),
        )
        .unwrap();
        let second = MetadataBatchPage::new(
            batch_id.clone(),
            scope.clone(),
            1,
            2,
            MetadataBatchRecords::ObjectReceipt(Vec::new()),
            Extensions::new(),
        )
        .unwrap();
        let descriptor = MetadataBatchDescriptor::from_pages(
            batch_id,
            scope,
            &[first.clone(), second.clone()],
            Extensions::new(),
        )
        .unwrap();
        descriptor.validate_page(&first).unwrap();
        descriptor.validate_page(&second).unwrap();

        let mut wrong_scope = second;
        wrong_scope.scope.playground_id = PlaygroundId::new("playground-b").unwrap();
        wrong_scope.page_digest = wrong_scope.computed_digest().unwrap();
        assert!(descriptor.validate_page(&wrong_scope).is_err());
    }

    #[test]
    fn descriptor_uses_serializer_independent_jcs_byte_counts() {
        let batch_id = MetadataBatchId::new("batch-a").unwrap();
        let page = MetadataBatchPage::new(
            batch_id.clone(),
            scope(),
            0,
            1,
            MetadataBatchRecords::ObjectReceipt(Vec::new()),
            Extensions::new(),
        )
        .unwrap();
        let descriptor = MetadataBatchDescriptor::from_pages(
            batch_id,
            page.scope.clone(),
            std::slice::from_ref(&page),
            Extensions::new(),
        )
        .unwrap();
        let canonical_len = u64::try_from(jcs_bytes(&page).unwrap().len()).unwrap();
        assert_eq!(descriptor.pages[0].encoded_bytes.get(), canonical_len);

        let pretty = serde_json::to_vec_pretty(&page).unwrap();
        assert_ne!(u64::try_from(pretty.len()).unwrap(), canonical_len);
        let decoded = MetadataBatchPage::decode_json(&pretty).unwrap();
        descriptor.validate_page(&decoded).unwrap();
    }

    #[test]
    fn manifest_record_validates_only_its_local_fragment() {
        let record = ManifestRecord {
            manifest_id: ManifestId::from_bytes([0x55; 32]),
            total_size: DecimalU64::new(20),
            chunking: WireChunkingStrategy::FastCdc,
            chunk_start: DecimalU64::new(2),
            chunks: vec![
                WireChunkRef {
                    object_id: object_id(),
                    offset: DecimalU64::new(8),
                    size: DecimalU64::new(4),
                    extensions: Extensions::new(),
                },
                WireChunkRef {
                    object_id: ObjectId::from_bytes([0x22; 32]),
                    offset: DecimalU64::new(12),
                    size: DecimalU64::new(4),
                    extensions: Extensions::new(),
                },
            ],
            extensions: Extensions::new(),
        };

        record.validate().unwrap();
    }

    #[test]
    fn manifest_record_rejects_invalid_local_fragment_boundaries() {
        let base = ManifestRecord {
            manifest_id: ManifestId::from_bytes([0x55; 32]),
            total_size: DecimalU64::new(20),
            chunking: WireChunkingStrategy::FastCdc,
            chunk_start: DecimalU64::new(0),
            chunks: vec![WireChunkRef {
                object_id: object_id(),
                offset: DecimalU64::new(0),
                size: DecimalU64::new(4),
                extensions: Extensions::new(),
            }],
            extensions: Extensions::new(),
        };

        let mut nonzero_first_offset = base.clone();
        nonzero_first_offset.chunks[0].offset = DecimalU64::new(1);
        assert!(nonzero_first_offset.validate().is_err());

        let mut local_gap = base;
        local_gap.chunks.push(WireChunkRef {
            object_id: ObjectId::from_bytes([0x22; 32]),
            offset: DecimalU64::new(5),
            size: DecimalU64::new(4),
            extensions: Extensions::new(),
        });
        assert!(local_gap.validate().is_err());
    }

    #[test]
    fn manifest_record_allows_only_the_canonical_empty_fragment_shape() {
        let empty = ManifestRecord {
            manifest_id: ManifestId::from_bytes([0x55; 32]),
            total_size: DecimalU64::new(0),
            chunking: WireChunkingStrategy::FastCdc,
            chunk_start: DecimalU64::new(0),
            chunks: Vec::new(),
            extensions: Extensions::new(),
        };
        empty.validate().unwrap();

        let mut nonzero_start = empty.clone();
        nonzero_start.chunk_start = DecimalU64::new(1);
        assert!(nonzero_start.validate().is_err());

        let mut missing_nonempty_chunks = empty;
        missing_nonempty_chunks.total_size = DecimalU64::new(1);
        assert!(missing_nonempty_chunks.validate().is_err());
    }

    #[test]
    fn manifest_fragment_has_no_nested_record_count_limit() {
        let chunks = (0_u64..=MAX_RECORDS_PER_PAGE as u64)
            .map(|offset| WireChunkRef {
                object_id: object_id(),
                offset: DecimalU64::new(offset),
                size: DecimalU64::new(1),
                extensions: Extensions::new(),
            })
            .collect::<Vec<_>>();
        let record = ManifestRecord {
            manifest_id: ManifestId::from_bytes([0x55; 32]),
            total_size: DecimalU64::new(chunks.len() as u64),
            chunking: WireChunkingStrategy::FastCdc,
            chunk_start: DecimalU64::new(0),
            chunks,
            extensions: Extensions::new(),
        };

        record.validate().unwrap();
    }

    #[test]
    fn recursive_validation_rejects_nested_reserved_extension_keys() {
        let mut chunk_extensions = Extensions::new();
        chunk_extensions.insert("object_id".to_owned(), serde_json::json!("shadow"));
        let result = MetadataBatchPage::new(
            MetadataBatchId::new("batch-a").unwrap(),
            scope(),
            0,
            1,
            MetadataBatchRecords::Manifest(vec![ManifestRecord {
                manifest_id: ManifestId::from_str(&"55".repeat(32)).unwrap(),
                total_size: DecimalU64::new(10),
                chunking: WireChunkingStrategy::WholeFile,
                chunk_start: DecimalU64::new(0),
                chunks: vec![WireChunkRef {
                    object_id: object_id(),
                    offset: DecimalU64::new(0),
                    size: DecimalU64::new(10),
                    extensions: chunk_extensions,
                }],
                extensions: Extensions::new(),
            }]),
            Extensions::new(),
        );
        assert!(matches!(
            result,
            Err(ProtocolError::InvalidField {
                field: "extensions",
                ..
            })
        ));
    }

    #[test]
    fn page_rejects_encoded_payload_over_eight_mib() {
        let mut extensions = Extensions::new();
        extensions.insert(
            "future_payload".to_owned(),
            serde_json::Value::String("x".repeat(MAX_METADATA_PAGE_BYTES)),
        );
        let result = MetadataBatchPage::new(
            MetadataBatchId::new("batch-a").unwrap(),
            scope(),
            0,
            1,
            MetadataBatchRecords::ObjectReceipt(Vec::new()),
            extensions,
        );
        assert!(matches!(
            result,
            Err(ProtocolError::LimitExceeded {
                limit_name: "metadata page bytes",
                ..
            })
        ));
    }

    #[test]
    fn metadata_page_decode_rejects_duplicate_members_recursively() {
        let pages: [&[u8]; 2] = [
            br#"{"batch_id":"batch-first","batch_id":"batch-last"}"#,
            br#"{"future_page":{"nested":{"mode":"first","mode":"last"}}}"#,
        ];

        for page in pages {
            let error = MetadataBatchPage::decode_json(page).unwrap_err();
            assert_eq!(error.stable_code(), "PROTOCOL_INVALID");
            assert!(error.to_string().contains("duplicate JSON object member"));
        }
    }
}
