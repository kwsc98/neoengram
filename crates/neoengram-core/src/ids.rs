use std::{fmt, str::FromStr};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::{ValidationError, ValidationErrorKind, ValidationResult};

const DIGEST_BYTES: usize = 32;
const DIGEST_HEX_LEN: usize = DIGEST_BYTES * 2;

/// A validated 256-bit BLAKE3 digest.
///
/// JSON and other human-readable Serde formats represent the digest as exactly 64 lowercase
/// hexadecimal characters.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentDigest([u8; DIGEST_BYTES]);

impl ContentDigest {
    /// Constructs a digest from its exact 32-byte representation.
    pub const fn from_bytes(bytes: [u8; DIGEST_BYTES]) -> Self {
        Self(bytes)
    }

    /// Hashes a complete byte slice with BLAKE3.
    pub fn hash(bytes: impl AsRef<[u8]>) -> Self {
        Self(*blake3::hash(bytes.as_ref()).as_bytes())
    }

    /// Returns the exact 32-byte representation used by canonical encoders.
    pub const fn as_bytes(&self) -> &[u8; DIGEST_BYTES] {
        &self.0
    }

    /// Returns the canonical 64-character lowercase hexadecimal representation.
    pub fn to_hex(self) -> String {
        self.to_string()
    }
}

impl fmt::Display for ContentDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for ContentDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ContentDigest")
            .field(&self.to_string())
            .finish()
    }
}

impl FromStr for ContentDigest {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != DIGEST_HEX_LEN
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ValidationError::new(
                ValidationErrorKind::InvalidDigest,
                format!("content digest must be {DIGEST_HEX_LEN} lowercase hexadecimal characters"),
            ));
        }
        let parsed = blake3::Hash::from_hex(value).map_err(|_| {
            ValidationError::new(
                ValidationErrorKind::InvalidDigest,
                "content digest is not valid BLAKE3 hexadecimal",
            )
        })?;
        Ok(Self(*parsed.as_bytes()))
    }
}

impl TryFrom<&str> for ContentDigest {
    type Error = ValidationError;

    fn try_from(value: &str) -> ValidationResult<Self> {
        value.parse()
    }
}

impl TryFrom<String> for ContentDigest {
    type Error = ValidationError;

    fn try_from(value: String) -> ValidationResult<Self> {
        value.parse()
    }
}

impl Serialize for ContentDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ContentDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

macro_rules! typed_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(ContentDigest);

        impl $name {
            /// Constructs the typed identifier from a validated content digest.
            pub const fn from_digest(digest: ContentDigest) -> Self {
                Self(digest)
            }

            /// Constructs the typed identifier from its exact 32-byte representation.
            pub const fn from_bytes(bytes: [u8; DIGEST_BYTES]) -> Self {
                Self(ContentDigest::from_bytes(bytes))
            }

            /// Returns the underlying content digest.
            pub const fn digest(self) -> ContentDigest {
                self.0
            }

            /// Returns the exact 32 bytes used by canonical encoders.
            pub const fn as_bytes(&self) -> &[u8; DIGEST_BYTES] {
                self.0.as_bytes()
            }

            /// Returns the canonical lowercase hexadecimal representation.
            pub fn to_hex(self) -> String {
                self.0.to_hex()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, formatter)
            }
        }

        impl FromStr for $name {
            type Err = ValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                value.parse().map(Self)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = ValidationError;

            fn try_from(value: &str) -> ValidationResult<Self> {
                value.parse()
            }
        }

        impl TryFrom<String> for $name {
            type Error = ValidationError;

            fn try_from(value: String) -> ValidationResult<Self> {
                value.parse()
            }
        }

        impl From<ContentDigest> for $name {
            fn from(value: ContentDigest) -> Self {
                Self(value)
            }
        }

        impl From<$name> for ContentDigest {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

typed_id!(
    /// The BLAKE3 identity of an immutable payload object.
    ObjectId
);
typed_id!(
    /// The canonical identity of an immutable file Manifest.
    ManifestId
);
typed_id!(
    /// The canonical identity of an immutable Directory.
    DirectoryId
);
typed_id!(
    /// The canonical identity of an immutable Commit.
    CommitId
);

impl ObjectId {
    /// Computes the object identity for a complete immutable payload.
    pub fn for_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self(ContentDigest::hash(bytes))
    }
}
