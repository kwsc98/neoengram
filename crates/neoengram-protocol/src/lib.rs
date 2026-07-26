//! Versioned, transport-independent NeoEngram wire contracts.
//!
//! This crate contains only serializable protocol types, validation, canonical JSON digest
//! helpers, and JSON Schema generation. It intentionally has no dependency on an HTTP runtime,
//! a database, a filesystem, the CLI, or the execution engine.

mod control;
mod digest;
mod error;
mod ids;
mod metadata;
mod s3;
mod scalars;
mod schema;
mod validation;

pub use control::*;
pub use digest::{jcs_blake3, jcs_bytes};
pub use error::{ProtocolError, ProtocolResult};
pub use ids::*;
pub use metadata::*;
pub use s3::*;
pub use scalars::*;
pub use schema::{control_schema, metadata_schema, s3_schema};

/// The only protocol version emitted by this crate.
pub const PROTOCOL_VERSION_V1: ProtocolVersion = ProtocolVersion::V1;

/// Maximum encoded size of an in-band control message.
pub const MAX_CONTROL_MESSAGE_BYTES: usize = 1024 * 1024;

/// Maximum encoded size of one out-of-band metadata page.
pub const MAX_METADATA_PAGE_BYTES: usize = 8 * 1024 * 1024;

/// Maximum number of records carried by one metadata page or negotiation request.
pub const MAX_RECORDS_PER_PAGE: usize = 4096;

/// Unknown JSON object members retained across decode/encode cycles.
pub type Extensions = std::collections::BTreeMap<String, serde_json::Value>;
