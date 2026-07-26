//! Durable, transport-independent execution state machine for managed NeoEngram agents.
//!
//! The crate deliberately contains no network client, filesystem discovery, terminal output, or
//! standalone repository integration. Assignments and decisions arrive as protocol values, while
//! every external effect is expressed through an explicit synchronous port.

mod agent;
mod error;
mod fakes;
mod memory;
mod ports;
mod state;

pub use agent::Agent;
pub use error::{AgentError, AgentErrorCode, AgentResult};
pub use fakes::{FakeAddExecutor, FakeClock, FakeObjectTransfer, FakeReportSink};
pub use memory::InMemoryLedger;
pub use ports::{
    AddExecutor, AssignmentValidator, BasicAssignmentValidator, Clock, Ledger, ObjectTransfer,
    ReportSink,
};
pub use state::{
    AgentAssignmentState, AgentReport, AssignmentAcceptance, AssignmentKey, ClaimDisposition,
    ClaimOutcome, LedgerClaim, LedgerRecord, PreparedExecution, TransferReceipt,
};
