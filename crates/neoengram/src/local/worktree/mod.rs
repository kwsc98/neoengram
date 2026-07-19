//! 工作区扫描、物化与恢复事务。

mod checkout;
mod ignore;
mod import;
mod recovery;
mod remove;
mod scan;
mod snapshot;
mod workspace;

pub(crate) use checkout::{checkout_snapshot, CheckoutOutcome};
pub(crate) use ignore::IgnoreRules;
pub(crate) use import::chunk_file;
pub(crate) use recovery::{recover_repository, RecoveryReport};
pub(crate) use remove::{remove_tracked_path, repository_scope, RemovalRecovery};
pub(crate) use scan::{collect_files_with_ignore, validate_input_path};
pub(crate) use snapshot::{materialize_read_only_snapshot, ReadOnlySnapshotOutcome};
pub(crate) use workspace::{
    file_matches_node, lenient_leaf_metadata, safe_leaf_metadata, workspace_path,
};

/// 集成测试使用的进程级故障点。命中时跳过 Drop，模拟事务进程被直接终止。
fn test_crash_at(point: &str) {
    if cfg!(debug_assertions)
        && std::env::var("NEOENGRAM_TEST_CRASH_AT").is_ok_and(|configured| configured == point)
    {
        eprintln!("test crash at {point}");
        std::process::exit(86);
    }
}
