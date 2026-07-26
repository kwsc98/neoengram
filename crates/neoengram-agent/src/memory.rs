use std::{collections::BTreeMap, sync::Mutex};

use crate::{
    AgentError, AgentErrorCode, AgentResult, AssignmentKey, ClaimOutcome, Ledger, LedgerClaim,
    LedgerRecord,
};

/// Mutex-backed reference Ledger used by component tests and in-process compositions.
#[derive(Debug, Default)]
pub struct InMemoryLedger {
    records: Mutex<BTreeMap<AssignmentKey, LedgerRecord>>,
}

impl InMemoryLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn records(&self) -> AgentResult<Vec<LedgerRecord>> {
        let records = self.records.lock().map_err(lock_error)?;
        Ok(records.values().cloned().collect())
    }
}

impl Ledger for InMemoryLedger {
    fn claim(&self, claim: LedgerClaim) -> AgentResult<ClaimOutcome> {
        let key = AssignmentKey::from_assignment(&claim.assignment);
        let mut records = self.records.lock().map_err(lock_error)?;
        if let Some(existing) = records.get(&key) {
            if existing.request_digest == claim.assignment.request_digest {
                return Ok(ClaimOutcome::Existing(existing.clone()));
            }
            return Err(AgentError::new(
                AgentErrorCode::JobIdReused,
                format!(
                    "job {} was already claimed with a different request digest",
                    key
                ),
            ));
        }
        let record = LedgerRecord::claimed(claim.assignment, claim.claimed_at_unix_ms);
        records.insert(key, record.clone());
        Ok(ClaimOutcome::Claimed(record))
    }

    fn load(&self, key: &AssignmentKey) -> AgentResult<Option<LedgerRecord>> {
        let records = self.records.lock().map_err(lock_error)?;
        Ok(records.get(key).cloned())
    }

    fn compare_exchange(
        &self,
        expected_revision: u64,
        mut next: LedgerRecord,
    ) -> AgentResult<LedgerRecord> {
        let mut records = self.records.lock().map_err(lock_error)?;
        let current = records.get(&next.key).ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::AssignmentNotFound,
                format!("assignment {} is not present in the ledger", next.key),
            )
        })?;
        if current.revision != expected_revision {
            return Err(AgentError::new(
                AgentErrorCode::LedgerConflict,
                format!(
                    "assignment {} revision changed from {} to {}",
                    next.key, expected_revision, current.revision
                ),
            ));
        }
        if current.key != next.key
            || current.request_digest != next.request_digest
            || current.assignment != next.assignment
            || next.key != AssignmentKey::from_assignment(&next.assignment)
            || next.request_digest != next.assignment.request_digest
        {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "ledger compare-exchange cannot change the durable assignment payload",
            ));
        }
        next.revision = expected_revision.checked_add(1).ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::Internal,
                "ledger revision overflow while updating assignment",
            )
        })?;
        records.insert(next.key.clone(), next.clone());
        Ok(next)
    }
}

fn lock_error<T>(_error: std::sync::PoisonError<T>) -> AgentError {
    AgentError::new(
        AgentErrorCode::Internal,
        "in-memory ledger lock is poisoned",
    )
}
