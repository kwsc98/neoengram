use serde::{Deserialize, Serialize};

use neoengram_core::LogicalPath;

use crate::EngineResult;

/// Stable phase names suitable for progress reporting and audit enrichment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ProgressPhase {
    Preparing,
    Scanning,
    Snapshotting,
    Chunking,
    PublishingObjects,
    BuildingCandidate,
    Prepared,
    Journaling,
    ApplyingMutation,
    Verifying,
    Recovering,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ProgressUnit {
    Items,
    Files,
    Objects,
    Bytes,
    Actions,
}

/// A structured progress observation. It intentionally contains no presentation text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressEvent {
    pub phase: ProgressPhase,
    pub unit: ProgressUnit,
    pub completed: u64,
    pub total: Option<u64>,
    pub logical_path: Option<LogicalPath>,
}

impl ProgressEvent {
    #[must_use]
    pub const fn new(phase: ProgressPhase, unit: ProgressUnit, completed: u64) -> Self {
        Self {
            phase,
            unit,
            completed,
            total: None,
            logical_path: None,
        }
    }

    #[must_use]
    pub const fn with_total(mut self, total: u64) -> Self {
        self.total = Some(total);
        self
    }

    #[must_use]
    pub fn at_path(mut self, logical_path: LogicalPath) -> Self {
        self.logical_path = Some(logical_path);
        self
    }
}

pub trait ProgressSink: Send + Sync {
    fn emit(&self, event: &ProgressEvent) -> EngineResult<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopProgressSink;

impl ProgressSink for NoopProgressSink {
    fn emit(&self, _event: &ProgressEvent) -> EngineResult<()> {
        Ok(())
    }
}
