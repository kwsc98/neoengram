pub(crate) mod add;
pub(crate) mod checkout;
pub(crate) mod commit;
pub(crate) mod diff;
pub(crate) mod export;
pub(crate) mod fsck;
pub(crate) mod gc;
pub(crate) mod init;
pub(crate) mod log;
pub(crate) mod mount;
pub(crate) mod recover;
pub(crate) mod restore;
pub(crate) mod rm;
pub(crate) mod show;
pub(crate) mod status;
pub(crate) mod workspace;

/// 集成测试使用的进程级故障点。默认环境下这段代码完全不生效；命中时直接退出，刻意
/// 跳过 Rust Drop，以模拟进程被 kill 后 advisory lock 由内核释放、事务目录留在磁盘。
pub(crate) fn test_crash_at(point: &'static str) {
    if cfg!(debug_assertions) && crate::failure_injected(point) {
        crate::emit_diagnostic(format!("test crash at {point}"));
        std::process::exit(86);
    }
}

/// 集成测试使用的进程级同步点。测试进程关闭或写入 stdin 后命令继续执行；正常环境下
/// 不读取 stdin，也不改变命令行为。
pub(crate) fn test_pause_at(point: &'static str) {
    if cfg!(debug_assertions) && crate::failure_injected(point) {
        crate::emit_diagnostic(format!("test pause at {point}"));
        let mut release = String::new();
        let _ = std::io::stdin().read_line(&mut release);
    }
}
