use serde::{Deserialize, Serialize};

use crate::{ObjectId, ValidationError, ValidationErrorKind, ValidationResult};

/// The identity and exact byte length of an immutable payload object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectSpec {
    /// Content-addressed object identity.
    pub id: ObjectId,
    /// Exact payload length in bytes.
    pub size: u64,
}

impl ObjectSpec {
    /// Constructs an immutable payload specification.
    pub const fn new(id: ObjectId, size: u64) -> Self {
        Self { id, size }
    }

    /// Computes a specification directly from complete payload bytes.
    pub fn for_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = bytes.as_ref();
        Self {
            id: ObjectId::for_bytes(bytes),
            size: bytes.len() as u64,
        }
    }
}

/// A reference to one immutable payload object within a file Manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkRef {
    /// Identity of the payload object containing this chunk.
    pub object_id: ObjectId,
    /// Byte offset of the chunk within the logical file.
    pub offset: u64,
    /// Number of payload bytes in this chunk.
    pub size: u64,
}

impl ChunkRef {
    /// Constructs and validates a chunk reference.
    pub fn new(object_id: ObjectId, offset: u64, size: u64) -> ValidationResult<Self> {
        let chunk = Self {
            object_id,
            offset,
            size,
        };
        chunk.validate()?;
        Ok(chunk)
    }

    /// Validates the per-chunk invariants independent of its containing Manifest.
    pub fn validate(&self) -> ValidationResult<()> {
        if self.size == 0 {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidObject,
                "chunk size must be greater than zero",
            ));
        }
        self.offset.checked_add(self.size).ok_or_else(|| {
            ValidationError::new(
                ValidationErrorKind::Overflow,
                "chunk end offset exceeds u64",
            )
        })?;
        Ok(())
    }

    /// Returns the payload specification expected in the object store.
    pub const fn object_spec(self) -> ObjectSpec {
        ObjectSpec::new(self.object_id, self.size)
    }
}
