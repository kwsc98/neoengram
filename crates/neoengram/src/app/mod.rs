pub(crate) mod add;
pub(crate) mod checkout;
pub(crate) mod commit;
pub(crate) mod fsck;
pub(crate) mod init;
pub(crate) mod log;
pub(crate) mod recover;
pub(crate) mod rm;
pub(crate) mod show;
pub(crate) mod status;

/// 集成测试使用的进程级故障点。默认环境下这段代码完全不生效；命中时直接退出，刻意
/// 跳过 Rust Drop，以模拟进程被 kill 后 advisory lock 由内核释放、事务目录留在磁盘。
pub(crate) fn test_crash_at(point: &str) {
    if cfg!(debug_assertions)
        && std::env::var("NEOENGRAM_TEST_CRASH_AT").is_ok_and(|configured| configured == point)
    {
        eprintln!("test crash at {point}");
        std::process::exit(86);
    }
}
