//! Durable, transport-independent execution state machine for managed NeoEngram agents.
//!
//! The crate deliberately contains no network client, terminal output, or standalone repository
//! integration. Assignments and decisions arrive as protocol values; production-local adapters
//! provide durable state and one explicitly configured Volume probe.

mod agent;
mod error;
mod fakes;
mod memory;
mod mount_probe;
mod ports;
mod single_volume;
mod sqlite_ledger;
mod sqlite_storage;
mod state;
mod system_identity;

pub use agent::Agent;
pub use error::{AgentError, AgentErrorCode, AgentResult};
pub use fakes::{FakeAddExecutor, FakeClock, FakeObjectTransfer, FakeReportSink};
pub use memory::InMemoryLedger;
pub use mount_probe::{
    FilesystemMountObservation, FilesystemMountProbeConfig, MountProbeCondition,
    VOLUME_MARKER_FILE_NAME,
};
pub use ports::{
    AddExecutor, AssignmentValidator, BasicAssignmentValidator, Clock, Ledger, ObjectTransfer,
    ReportSink,
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
