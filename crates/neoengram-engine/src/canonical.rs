use neoengram_core::ContentDigest;

use crate::{EngineError, EngineResult, ErrorCode};

/// Incremental encoder for Engine-owned canonical identities.
///
/// Every variable-width value is prefixed with its unsigned 64-bit big-endian byte length. The
/// domain is encoded the same way before any payload, so schemas cannot collide by concatenation.
pub(crate) struct CanonicalHasher(blake3::Hasher);

impl CanonicalHasher {
    pub(crate) fn new(domain: &[u8]) -> EngineResult<Self> {
        let mut hasher = Self(blake3::Hasher::new());
        hasher.write_bytes(domain)?;
        Ok(hasher)
    }

    pub(crate) fn write_u8(&mut self, value: u8) {
        self.0.update(&[value]);
    }

    pub(crate) fn write_u64(&mut self, value: u64) {
        self.0.update(&value.to_be_bytes());
    }

    pub(crate) fn write_len(&mut self, length: usize) -> EngineResult<()> {
        let length = u64::try_from(length).map_err(|_| {
            EngineError::new(
                ErrorCode::InvalidArgument,
                "canonical collection length exceeds u64",
            )
        })?;
        self.write_u64(length);
        Ok(())
    }

    pub(crate) fn write_bytes(&mut self, bytes: &[u8]) -> EngineResult<()> {
        self.write_len(bytes.len())?;
        self.0.update(bytes);
        Ok(())
    }

    pub(crate) fn write_str(&mut self, value: &str) -> EngineResult<()> {
        self.write_bytes(value.as_bytes())
    }

    pub(crate) fn write_digest(&mut self, digest: ContentDigest) -> EngineResult<()> {
        self.write_bytes(digest.as_bytes())
    }

    pub(crate) fn finish(self) -> ContentDigest {
        ContentDigest::from_bytes(*self.0.finalize().as_bytes())
    }
}
