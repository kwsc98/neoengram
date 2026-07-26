//! 工作区扫描、物化与恢复事务。

mod checkout;
mod ignore;
mod import;
mod recovery;
mod remove;
mod restore;
mod scan;
mod snapshot;
mod workspace;

pub(crate) use checkout::{checkout_snapshot, checkout_snapshot_with_progress, CheckoutOutcome};
pub(crate) use ignore::IgnoreRules;
pub(crate) use import::chunk_file;
pub(crate) use recovery::{recover_repository_locked, RecoveryReport};
pub(crate) use remove::{remove_tracked_path, repository_scope, RemovalRecovery};
pub(crate) use restore::{restore_worktree_paths, RestoreRecovery};
pub(crate) use scan::{collect_files_with_ignore, validate_input_path};
pub(crate) use snapshot::{materialize_read_only_snapshot, ExportMode, ReadOnlySnapshotOutcome};
pub(crate) use workspace::{file_matches_node, lenient_leaf_metadata, workspace_path};

/// 集成测试使用的进程级故障点。命中时跳过 Drop，模拟事务进程被直接终止。
fn test_crash_at(point: &'static str) {
    if cfg!(debug_assertions) && crate::failure_injected(point) {
        crate::emit_diagnostic(format!("test crash at {point}"));
        std::process::exit(86);
    }
}

/// 集成测试使用的进程级同步点。正常命令不会读取 stdin。
fn test_pause_at(point: &'static str) {
    if cfg!(debug_assertions) && crate::failure_injected(point) {
        crate::emit_diagnostic(format!("test pause at {point}"));
        let mut release = String::new();
        let _ = std::io::stdin().read_line(&mut release);
    }
}
