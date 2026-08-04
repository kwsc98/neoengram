//! Standalone process adapter for the transport-independent NeoEngram Agent core.
//!
//! HTTP, process lifecycle, configuration, and health signaling live here. The underlying
//! `neoengram-agent` and `neoengram-protocol` crates remain independent of any HTTP stack.

mod backoff;
mod client;
mod config;
mod durability;
mod error;
mod health;
mod identity;
mod runtime;
mod status_clock;

pub use backoff::EnrollmentBackoff;
pub use client::{EnrollmentClient, EnrollmentClientError, ReqwestEnrollmentClient};
pub use config::{
    AgentConfig, LoggingConfig, LoggingFormat, PvcReference, RegistrationConfig, SessionConfig,
    StorageAccessMode, StorageBackendType, StorageConfig,
};
pub use error::{AgentDaemonError, AgentDaemonResult};
pub use health::{check_health, HealthMode, RuntimeHealthPhase};
pub use identity::{
    load_or_create_identity, load_persisted_identity, signing_key_from_identity,
    PersistedIdentitySummary,
};
pub use neoengram_agent::{FilesystemMountObservation, MountProbeCondition};
pub use runtime::{run, run_with, FilesystemProbe, MountProbe};
