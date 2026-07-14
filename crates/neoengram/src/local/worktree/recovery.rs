use std::{fs, path::Path};

use anyhow::{bail, ensure, Context, Result};

use crate::local::repository::Repository;

use super::{checkout, remove, RemovalRecovery};

pub(crate) enum RecoveryReport {
    Nothing,
    Checkout {
        target_id: String,
        aborted: bool,
    },
    Removal {
        id: String,
        outcome: RemovalRecovery,
    },
}

pub(crate) fn recover_repository(repository: &Repository, abort: bool) -> Result<RecoveryReport> {
    // recovery lock 不会因 transactions 非空而拒绝；它仍与 add/commit/checkout 使用
    // 同一 OS advisory lock，因此恢复期间不会有另一个 NeoEngram 写命令交错执行。
    let mut lock = repository.acquire_recovery_lock()?;
    let transactions = repository.unfinished_transactions()?;
    if transactions.is_empty() {
        return Ok(RecoveryReport::Nothing);
    }
    ensure!(
        transactions.len() == 1,
        "检测到多个未完成事务，无法安全推断恢复顺序: {}",
        transactions
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let transaction = transactions.first().context("内部错误：事务列表为空")?;
    let operation = read_operation(transaction)?;
    match operation.as_str() {
        "checkout" => {
            let outcome = checkout::recover_transaction(repository, transaction, abort, &mut lock)?;
            Ok(RecoveryReport::Checkout {
                target_id: outcome.target_id().to_owned(),
                aborted: outcome.aborted(),
            })
        }
        "rm" => {
            let id = transaction
                .file_name()
                .and_then(|name| name.to_str())
                .context("rm 事务目录名不是有效 UTF-8")?
                .to_owned();
            lock.set_operation("rm-recover", Some(&id))?;
            let outcome = remove::recover_transaction(repository, transaction, abort)?;
            lock.set_operation("rm-recover", None)?;
            Ok(RecoveryReport::Removal { id, outcome })
        }
        other => bail!("不支持的事务类型 {other:?}: {}", transaction.display()),
    }
}

fn read_operation(transaction: &Path) -> Result<String> {
    let path = transaction.join("OPERATION");
    let metadata = fs::symlink_metadata(&path)
        .with_context(|| format!("无法检查事务类型文件: {}", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "事务类型路径不是普通文件: {}",
        path.display()
    );
    let operation = fs::read_to_string(&path)
        .with_context(|| format!("无法读取事务类型: {}", path.display()))?;
    ensure!(!operation.trim().is_empty(), "事务类型不能为空");
    Ok(operation.trim().to_owned())
}
