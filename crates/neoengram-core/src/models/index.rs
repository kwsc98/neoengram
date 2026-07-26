use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    canonical, validate_path_set, ContentDigest, FileRecord, LogicalPath, ValidationError,
    ValidationErrorKind, ValidationResult,
};

use super::MAX_INDEX_MUTATIONS_PER_PAGE;

/// Version used for optimistic compare-and-swap publication of an Index snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexVersion {
    /// Monotonic backend revision participating in compare-and-swap.
    pub revision: u64,
    /// Canonical digest of the complete sorted snapshot contents.
    pub digest: ContentDigest,
}

impl IndexVersion {
    /// Constructs a version from a backend revision and canonical snapshot digest.
    pub const fn new(revision: u64, digest: ContentDigest) -> Self {
        Self { revision, digest }
    }

    /// Computes a version for a complete, canonical-order snapshot.
    pub fn from_snapshot(revision: u64, records: &[FileRecord]) -> ValidationResult<Self> {
        Ok(Self::new(
            revision,
            canonical::index_snapshot_digest(records)?,
        ))
    }
}

/// One exact-path change in a paged Index delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IndexMutation {
    /// Inserts a new path or replaces its existing file record.
    Upsert {
        /// Complete replacement record.
        record: FileRecord,
    },
    /// Removes one exact logical path from the Index.
    Delete {
        /// Exact path to remove.
        path: LogicalPath,
    },
}

impl IndexMutation {
    /// Returns the exact path affected by this mutation.
    pub const fn path(&self) -> &LogicalPath {
        match self {
            Self::Upsert { record } => &record.path,
            Self::Delete { path } => path,
        }
    }

    /// Validates the mutation's contained model.
    pub fn validate(&self) -> ValidationResult<()> {
        match self {
            Self::Upsert { record } => record.validate(),
            Self::Delete { .. } => Ok(()),
        }
    }
}

/// One bounded, canonical-order page in an [`IndexDelta`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDeltaPage {
    /// Zero-based position in the complete delta.
    pub page_number: u32,
    /// Strictly path-ordered, portable-unique mutations in this page.
    pub mutations: Vec<IndexMutation>,
}

impl IndexDeltaPage {
    /// Constructs and validates one non-empty bounded page.
    pub fn new(page_number: u32, mutations: Vec<IndexMutation>) -> ValidationResult<Self> {
        let page = Self {
            page_number,
            mutations,
        };
        page.validate()?;
        Ok(page)
    }

    /// Validates the page bound, contained records, and local canonical path order.
    pub fn validate(&self) -> ValidationResult<()> {
        if self.mutations.is_empty() {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidIndex,
                format!("Index delta page {} cannot be empty", self.page_number),
            ));
        }
        if self.mutations.len() > MAX_INDEX_MUTATIONS_PER_PAGE {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidIndex,
                format!(
                    "Index delta page {} contains more than {MAX_INDEX_MUTATIONS_PER_PAGE} mutations",
                    self.page_number
                ),
            ));
        }
        for mutation in &self.mutations {
            mutation.validate()?;
        }
        validate_index_mutation_paths(self.mutations.iter().map(IndexMutation::path))
    }
}

/// An arbitrary-size canonical sequence of bounded Index mutation pages.
///
/// `result_digest` is the canonical digest of the complete snapshot after every page is applied.
/// It allows the publisher to verify the candidate before CAS. Page boundaries are transport
/// details and do not participate in the logical delta's canonical digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDelta {
    /// Version against which these mutations were prepared.
    pub base_version: IndexVersion,
    /// Consecutive, zero-based bounded pages covering the complete logical mutation sequence.
    pub pages: Vec<IndexDeltaPage>,
    /// Expected canonical digest of the complete resulting Index snapshot.
    pub result_digest: ContentDigest,
}

impl IndexDelta {
    /// Constructs a delta from a flat canonical mutation sequence.
    ///
    /// Mutations are split deterministically into maximally sized pages. An empty mutation
    /// sequence is represented by an empty page list and is valid.
    pub fn new(
        base_version: IndexVersion,
        mutations: Vec<IndexMutation>,
        result_digest: ContentDigest,
    ) -> ValidationResult<Self> {
        let mut mutations = mutations.into_iter();
        let mut pages = Vec::new();
        loop {
            let page_mutations = mutations
                .by_ref()
                .take(MAX_INDEX_MUTATIONS_PER_PAGE)
                .collect::<Vec<_>>();
            if page_mutations.is_empty() {
                break;
            }
            let page_number = u32::try_from(pages.len()).map_err(|_| {
                ValidationError::new(
                    ValidationErrorKind::Overflow,
                    "Index delta page count exceeds u32",
                )
            })?;
            pages.push(IndexDeltaPage {
                page_number,
                mutations: page_mutations,
            });
        }
        Self::from_pages(base_version, pages, result_digest)
    }

    /// Constructs a delta from explicitly bounded pages.
    pub fn from_pages(
        base_version: IndexVersion,
        pages: Vec<IndexDeltaPage>,
        result_digest: ContentDigest,
    ) -> ValidationResult<Self> {
        let delta = Self {
            base_version,
            pages,
            result_digest,
        };
        delta.validate()?;
        Ok(delta)
    }

    /// Returns the mutations in logical canonical order, independent of page boundaries.
    pub fn iter(&self) -> impl Iterator<Item = &IndexMutation> {
        self.pages.iter().flat_map(|page| page.mutations.iter())
    }

    /// Returns the total number of mutations across all pages.
    #[must_use]
    pub fn mutation_count(&self) -> usize {
        self.pages.iter().map(|page| page.mutations.len()).sum()
    }

    /// Returns the number of bounded pages.
    #[must_use]
    pub const fn page_count(&self) -> usize {
        self.pages.len()
    }

    /// Validates page bounds and numbering plus global path order and portable uniqueness.
    pub fn validate(&self) -> ValidationResult<()> {
        for (expected, page) in self.pages.iter().enumerate() {
            let expected = u32::try_from(expected).map_err(|_| {
                ValidationError::new(
                    ValidationErrorKind::Overflow,
                    "Index delta page count exceeds u32",
                )
            })?;
            if page.page_number != expected {
                return Err(ValidationError::new(
                    ValidationErrorKind::InvalidIndex,
                    format!(
                        "Index delta pages must be consecutive from zero: expected {expected}, observed {}",
                        page.page_number
                    ),
                ));
            }
            page.validate()?;
        }
        validate_index_mutation_paths(self.iter().map(IndexMutation::path))
    }

    /// Computes the canonical identity of this logical delta and its publication preconditions.
    pub fn canonical_digest(&self) -> ValidationResult<ContentDigest> {
        canonical::index_delta_digest(self)
    }
}

/// Validates the canonical path order and portable uniqueness of an Index mutation sequence.
///
/// Unlike [`validate_index_snapshot`], this validator deliberately permits ancestor/descendant
/// pairs such as `a` and `a/b`. A delta needs both paths to represent a file-to-directory or
/// directory-to-file transition; the publisher must validate the complete resulting snapshot.
pub fn validate_index_mutation_paths<'a>(
    paths: impl IntoIterator<Item = &'a LogicalPath>,
) -> ValidationResult<()> {
    let mut previous: Option<&LogicalPath> = None;
    let mut portable_paths = BTreeSet::new();
    for path in paths {
        if previous.is_some_and(|previous| previous >= path) {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidIndex,
                format!("Index mutations must be strictly ordered by path: {path}"),
            ));
        }
        if !portable_paths.insert(path.portable_key()) {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidIndex,
                format!("Index mutations collide under portable case rules: {path}"),
            ));
        }
        previous = Some(path);
    }
    Ok(())
}

/// Validates a complete Index snapshot in canonical path order.
pub fn validate_index_snapshot(records: &[FileRecord]) -> ValidationResult<()> {
    for record in records {
        record.validate()?;
    }
    validate_path_set(records.iter().map(|record| &record.path)).map_err(|error| {
        ValidationError::new(
            ValidationErrorKind::InvalidIndex,
            format!("invalid Index snapshot: {error}"),
        )
    })
}
