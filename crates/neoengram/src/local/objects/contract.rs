//! Local immutable, content-addressed payload storage contract.
//!
//! The interface deliberately works with streams and logical object IDs instead of physical
//! paths. Local loose-file and pack-file stores can therefore share the same correctness contract
//! without forcing callers to materialize whole objects in memory. Remote transfer has separate
//! asynchronous negotiation, authorization, and retry semantics and does not implement this trait.

use std::{
    fmt::Debug,
    io::{self, Read, Write},
    num::NonZeroUsize,
    path::Path,
};

use anyhow::{ensure, Context, Result};

/// Bounds one batch probe so an accidental whole-repository request cannot exhaust memory.
#[allow(dead_code)]
pub(crate) const MAX_OBJECT_CHECK_BATCH: usize = 4_096;
/// Bounds the memory returned by a single enumeration call.
pub(crate) const MAX_OBJECT_LIST_PAGE_SIZE: usize = 4_096;

/// A content-addressed object's identity and exact byte length.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ObjectSpec {
    pub(crate) id: String,
    pub(crate) size: u64,
}

impl ObjectSpec {
    /// Constructs a specification after validating the canonical BLAKE3 object ID.
    pub(crate) fn new(id: impl Into<String>, size: u64) -> Result<Self> {
        let id = id.into();
        validate_object_id(&id)?;
        Ok(Self { id, size })
    }
}

/// Metadata returned by the physical object store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectMeta {
    pub(crate) id: String,
    pub(crate) size: u64,
}

/// Result of checking one expected object without reading its entire payload.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ObjectCheck {
    Present(ObjectMeta),
    Missing(ObjectSpec),
    SizeMismatch {
        expected: ObjectSpec,
        actual_size: u64,
    },
}

/// A bounded page of logical objects.
///
/// `next_cursor` is backend-owned and must only be passed back to the same store. A `None` cursor
/// means this traversal is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectPage {
    pub(crate) objects: Vec<ObjectMeta>,
    pub(crate) next_cursor: Option<String>,
}

/// Outcome of an immutable, idempotent object publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PutOutcome {
    Created,
    AlreadyPresent,
}

/// Outcome of requesting a backend-native hard link to an object payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HardLinkOutcome {
    Linked,
    Unsupported,
}

/// Local storage boundary for immutable chunk payloads.
///
/// Implementations must never replace an object at an existing ID. `put_from` verifies both the
/// declared size and BLAKE3 digest before publication. `copy_to` verifies while streaming; callers
/// must discard a partially written destination if it returns an error. Successful writes become
/// crash-durable only after `durability_barrier` succeeds, and metadata must not reference them
/// before that barrier.
#[allow(dead_code)]
pub(crate) trait ObjectStore: Debug + Send + Sync {
    /// Creates or completes the backend's empty physical layout without replacing objects.
    fn initialize(&self) -> Result<()>;

    /// Validates the backend's required physical layout.
    fn validate_layout(&self) -> Result<()>;

    /// Streams, validates, and immutably publishes one object.
    ///
    /// An implementation may return `AlreadyPresent` without consuming `source` after fully
    /// verifying the existing object.
    fn put_from(&self, expected: &ObjectSpec, source: &mut dyn Read) -> Result<PutOutcome>;

    /// Streams one complete object to `target`, checking exact size and BLAKE3 along the way.
    fn copy_to(&self, expected: &ObjectSpec, target: &mut dyn Write) -> Result<()>;

    /// Reports whether this backend can expose verified objects through native hard links.
    fn supports_hard_links(&self) -> bool {
        false
    }

    /// Creates a no-replace hard link after fully verifying the source object.
    ///
    /// Backends without a stable local loose-file representation return `Unsupported`.
    fn hard_link_to(&self, _expected: &ObjectSpec, _target: &Path) -> Result<HardLinkOutcome> {
        Ok(HardLinkOutcome::Unsupported)
    }

    /// Reads and verifies one complete object without retaining its bytes.
    fn verify(&self, expected: &ObjectSpec) -> Result<()> {
        self.copy_to(expected, &mut io::sink())
    }

    /// Returns physical metadata for one logical object without hashing its contents.
    fn stat(&self, id: &str) -> Result<Option<ObjectMeta>>;

    /// Checks a bounded set of expected objects while preserving input order.
    fn check_many(&self, expected: &[ObjectSpec]) -> Result<Vec<ObjectCheck>> {
        ensure!(
            expected.len() <= MAX_OBJECT_CHECK_BATCH,
            "对象批量检查超过上限 {}: {}",
            MAX_OBJECT_CHECK_BATCH,
            expected.len()
        );
        let mut checks = Vec::with_capacity(expected.len());
        for spec in expected {
            validate_object_spec(spec)?;
            match self.stat(&spec.id)? {
                None => checks.push(ObjectCheck::Missing(spec.clone())),
                Some(meta) if meta.size == spec.size => {
                    checks.push(ObjectCheck::Present(meta));
                }
                Some(meta) => checks.push(ObjectCheck::SizeMismatch {
                    expected: spec.clone(),
                    actual_size: meta.size,
                }),
            }
        }
        Ok(checks)
    }

    /// Lists at most `limit` logical objects after an opaque backend cursor.
    fn list_page(&self, cursor: Option<&str>, limit: NonZeroUsize) -> Result<ObjectPage>;

    /// Removes one immutable object during an explicitly coordinated garbage-collection pass.
    ///
    /// Object stores must treat a missing object as a successful no-op and must never follow a
    /// symlink or remove a path outside their own layout.  The repository layer is responsible
    /// for proving that the object is unreachable before calling this method.
    fn remove(&self, id: &str) -> Result<bool>;

    /// Makes every successful publication completed before this call crash-durable.
    fn durability_barrier(&self) -> Result<()>;
}

pub(super) fn validate_object_spec(spec: &ObjectSpec) -> Result<()> {
    validate_object_id(&spec.id)
}

pub(super) fn validate_object_id(id: &str) -> Result<()> {
    let parsed =
        blake3::Hash::from_hex(id).with_context(|| format!("不是有效的 BLAKE3 对象 ID: {id}"))?;
    ensure!(
        parsed.to_hex().as_str() == id,
        "BLAKE3 对象 ID 必须是 64 位小写十六进制: {id}"
    );
    Ok(())
}
