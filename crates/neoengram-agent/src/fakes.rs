use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use neoengram_engine::PreparedAdd;
use neoengram_protocol::{AddAssignment, JobDecision};

use crate::{
    AddExecutor, AgentError, AgentErrorCode, AgentReport, AgentResult, AssignmentKey, Clock,
    ObjectTransfer, ReportSink, TransferReceipt,
};

/// Scriptable Add executor with deterministic FIFO results.
#[derive(Debug)]
pub struct FakeAddExecutor {
    results: Mutex<VecDeque<AgentResult<PreparedAdd>>>,
    calls: Mutex<Vec<AssignmentKey>>,
}

impl FakeAddExecutor {
    #[must_use]
    pub fn new(prepared: PreparedAdd) -> Self {
        Self::with_results([Ok(prepared)])
    }

    #[must_use]
    pub fn with_results(results: impl IntoIterator<Item = AgentResult<PreparedAdd>>) -> Self {
        Self {
            results: Mutex::new(results.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn calls(&self) -> AgentResult<Vec<AssignmentKey>> {
        self.calls
            .lock()
            .map(|calls| calls.clone())
            .map_err(fake_lock_error)
    }
}

impl AddExecutor for FakeAddExecutor {
    fn prepare_add(&self, assignment: &AddAssignment) -> AgentResult<PreparedAdd> {
        self.calls
            .lock()
            .map_err(fake_lock_error)?
            .push(AssignmentKey::from_assignment(assignment));
        self.results
            .lock()
            .map_err(fake_lock_error)?
            .pop_front()
            .unwrap_or_else(|| {
                Err(AgentError::new(
                    AgentErrorCode::ExecutionFailed,
                    "fake Add executor has no scripted result",
                ))
            })
    }
}

/// Scriptable immutable-object transfer and finalization adapter.
#[derive(Debug)]
pub struct FakeObjectTransfer {
    transfer_results: Mutex<VecDeque<AgentResult<TransferReceipt>>>,
    staging_results: Mutex<VecDeque<AgentResult<()>>>,
    finalize_results: Mutex<VecDeque<AgentResult<()>>>,
    transfers: Mutex<Vec<AssignmentKey>>,
    stagings: Mutex<Vec<(AssignmentKey, TransferReceipt)>>,
    staged_receipts: Mutex<BTreeMap<AssignmentKey, TransferReceipt>>,
    finalizations: Mutex<Vec<(AssignmentKey, JobDecision)>>,
}

impl FakeObjectTransfer {
    #[must_use]
    pub fn new(receipt: TransferReceipt) -> Self {
        Self::with_results([Ok(receipt)], [Ok(())])
    }

    #[must_use]
    pub fn with_results(
        transfer_results: impl IntoIterator<Item = AgentResult<TransferReceipt>>,
        finalize_results: impl IntoIterator<Item = AgentResult<()>>,
    ) -> Self {
        Self::with_staging_results(transfer_results, [Ok(())], finalize_results)
    }

    #[must_use]
    pub fn with_staging_results(
        transfer_results: impl IntoIterator<Item = AgentResult<TransferReceipt>>,
        staging_results: impl IntoIterator<Item = AgentResult<()>>,
        finalize_results: impl IntoIterator<Item = AgentResult<()>>,
    ) -> Self {
        Self {
            transfer_results: Mutex::new(transfer_results.into_iter().collect()),
            staging_results: Mutex::new(staging_results.into_iter().collect()),
            finalize_results: Mutex::new(finalize_results.into_iter().collect()),
            transfers: Mutex::new(Vec::new()),
            stagings: Mutex::new(Vec::new()),
            staged_receipts: Mutex::new(BTreeMap::new()),
            finalizations: Mutex::new(Vec::new()),
        }
    }

    pub fn transfers(&self) -> AgentResult<Vec<AssignmentKey>> {
        self.transfers
            .lock()
            .map(|calls| calls.clone())
            .map_err(fake_lock_error)
    }

    pub fn finalizations(&self) -> AgentResult<Vec<(AssignmentKey, JobDecision)>> {
        self.finalizations
            .lock()
            .map(|calls| calls.clone())
            .map_err(fake_lock_error)
    }

    pub fn stagings(&self) -> AgentResult<Vec<(AssignmentKey, TransferReceipt)>> {
        self.stagings
            .lock()
            .map(|calls| calls.clone())
            .map_err(fake_lock_error)
    }
}

impl ObjectTransfer for FakeObjectTransfer {
    fn transfer(
        &self,
        assignment: &AddAssignment,
        _prepared: &PreparedAdd,
    ) -> AgentResult<TransferReceipt> {
        self.transfers
            .lock()
            .map_err(fake_lock_error)?
            .push(AssignmentKey::from_assignment(assignment));
        self.transfer_results
            .lock()
            .map_err(fake_lock_error)?
            .pop_front()
            .unwrap_or_else(|| {
                Err(AgentError::new(
                    AgentErrorCode::ObjectTransferFailed,
                    "fake object transfer has no scripted result",
                ))
            })
    }

    fn stage_metadata(
        &self,
        assignment: &AddAssignment,
        receipt: &TransferReceipt,
    ) -> AgentResult<()> {
        receipt.validate(assignment)?;
        let key = AssignmentKey::from_assignment(assignment);
        self.stagings
            .lock()
            .map_err(fake_lock_error)?
            .push((key.clone(), receipt.clone()));
        if let Some(existing) = self
            .staged_receipts
            .lock()
            .map_err(fake_lock_error)?
            .get(&key)
            .cloned()
        {
            if existing == *receipt {
                return Ok(());
            }
            return Err(AgentError::new(
                AgentErrorCode::ObjectTransferFailed,
                "fake metadata staging received different material for an existing assignment",
            ));
        }
        let result = self
            .staging_results
            .lock()
            .map_err(fake_lock_error)?
            .pop_front()
            .unwrap_or_else(|| {
                Err(AgentError::new(
                    AgentErrorCode::ObjectTransferFailed,
                    "fake metadata staging has no scripted result",
                ))
            });
        if result.is_ok() {
            self.staged_receipts
                .lock()
                .map_err(fake_lock_error)?
                .insert(key, receipt.clone());
        }
        result
    }

    fn finalize(
        &self,
        assignment: &AddAssignment,
        _prepared: &PreparedAdd,
        decision: &JobDecision,
    ) -> AgentResult<()> {
        self.finalizations
            .lock()
            .map_err(fake_lock_error)?
            .push((AssignmentKey::from_assignment(assignment), decision.clone()));
        self.finalize_results
            .lock()
            .map_err(fake_lock_error)?
            .pop_front()
            .unwrap_or_else(|| {
                Err(AgentError::new(
                    AgentErrorCode::ObjectTransferFailed,
                    "fake object finalizer has no scripted result",
                ))
            })
    }
}

/// Capturing report sink for state-machine and composition tests.
#[derive(Debug, Default)]
pub struct FakeReportSink {
    reports: Mutex<Vec<AgentReport>>,
    failures: Mutex<VecDeque<AgentError>>,
}

impl FakeReportSink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn failing(failures: impl IntoIterator<Item = AgentError>) -> Self {
        Self {
            reports: Mutex::new(Vec::new()),
            failures: Mutex::new(failures.into_iter().collect()),
        }
    }

    pub fn reports(&self) -> AgentResult<Vec<AgentReport>> {
        self.reports
            .lock()
            .map(|reports| reports.clone())
            .map_err(fake_lock_error)
    }
}

impl ReportSink for FakeReportSink {
    fn send(&self, report: AgentReport) -> AgentResult<()> {
        if let Some(error) = self.failures.lock().map_err(fake_lock_error)?.pop_front() {
            return Err(error);
        }
        self.reports.lock().map_err(fake_lock_error)?.push(report);
        Ok(())
    }
}

/// Manually controlled clock.
#[derive(Debug)]
pub struct FakeClock {
    now_unix_ms: AtomicU64,
}

impl FakeClock {
    #[must_use]
    pub const fn new(now_unix_ms: u64) -> Self {
        Self {
            now_unix_ms: AtomicU64::new(now_unix_ms),
        }
    }

    pub fn set(&self, now_unix_ms: u64) {
        self.now_unix_ms.store(now_unix_ms, Ordering::SeqCst);
    }

    pub fn advance(&self, delta_ms: u64) -> AgentResult<u64> {
        self.now_unix_ms
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(delta_ms)
            })
            .map(|previous| previous + delta_ms)
            .map_err(|_| AgentError::new(AgentErrorCode::Internal, "fake clock overflow"))
    }
}

impl Clock for FakeClock {
    fn now_unix_ms(&self) -> AgentResult<u64> {
        Ok(self.now_unix_ms.load(Ordering::SeqCst))
    }
}

fn fake_lock_error<T>(_error: std::sync::PoisonError<T>) -> AgentError {
    AgentError::new(AgentErrorCode::Internal, "fake adapter lock is poisoned")
}
