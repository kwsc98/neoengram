//! Transport-independent Agent state and durable local adapters.
//!
//! This module is embedded in the Agent service package so the process adapter and the
//! execution state machine are shipped and versioned as one unit. The public re-exports are
//! surfaced by the parent crate for callers that use the Agent library API.

mod agent;
mod error;
mod fakes;
mod lifecycle;
mod memory;
mod mount_probe;
mod outbound;
mod placement_inventory;
mod ports;
mod single_volume;
mod sqlite_ledger;
mod sqlite_storage;
mod state;
mod system_identity;

pub use agent::Agent;
pub use error::{AgentError, AgentErrorCode, AgentResult};
pub use fakes::{FakeAddExecutor, FakeClock, FakeObjectTransfer, FakeReportSink};
pub use lifecycle::{
    LifecycleClaimOutcome, LifecycleCompleteOutcome, LifecycleJournal, LifecycleJournalRecord,
    LifecycleJournalState, SqliteLifecycleJournal, SqliteLifecycleJournalConfig,
};
pub use memory::InMemoryLedger;
pub use mount_probe::{
    verify_volume_marker, DevelopmentDirectoryMountProbeConfig, FilesystemMountObservation,
    FilesystemMountProbeConfig, MountProbeCondition, VOLUME_MARKER_FILE_NAME,
};
pub use outbound::{
    DurableReportSink, QueuedAgentReport, SqliteOutboundReportQueue,
    SqliteOutboundReportQueueConfig,
};
pub use placement_inventory::{
    InMemoryPlacementInventory, LocalPlacementInventory, PlacementInventoryConfig,
    SqlitePlacementInventory,
};
pub use ports::{
    AddExecutor, AssignmentValidator, BasicAssignmentValidator, Clock, Ledger, ObjectTransfer,
    OutboundReportQueue, ReportSink,
};
pub use single_volume::{
    MountStatusCondition, SingleVolumeAgentConfig, SingleVolumeAssignmentValidator,
    SingleVolumeMountProbe, SingleVolumeMountStatus,
};
pub use sqlite_ledger::{SqliteLedger, SqliteLedgerConfig};
pub use state::{
    AgentAssignmentState, AgentReport, AssignmentAcceptance, AssignmentKey, ClaimDisposition,
    ClaimOutcome, LedgerClaim, LedgerRecord, PreparedExecution, TransferReceipt,
};
pub use system_identity::{
    AgentCertificateState, ApprovedAgentIdentity, PrivateKeyMaterial, SqliteSystemIdentityStore,
    SystemIdentityRecord, SystemIdentitySeed, TerminalEnrollmentOutcome, TerminalEnrollmentState,
};
