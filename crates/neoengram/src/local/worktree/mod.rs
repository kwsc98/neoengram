//! 工作区扫描、物化与恢复事务。

mod checkout;
mod import;
mod recovery;
mod remove;
mod scan;

pub(crate) use checkout::{checkout_snapshot, CheckoutOutcome};
pub(crate) use import::chunk_file;
pub(crate) use recovery::{recover_repository, RecoveryReport};
pub(crate) use remove::remove_tracked_path;
pub(crate) use scan::{collect_files, validate_input_path};

/// 集成测试使用的进程级故障点。命中时跳过 Drop，模拟事务进程被直接终止。
fn test_crash_at(point: &str) {
    if cfg!(debug_assertions)
        && std::env::var("NEOENGRAM_TEST_CRASH_AT").is_ok_and(|configured| configured == point)
    {
        eprintln!("test crash at {point}");
        std::process::exit(86);
    }
}
