use std::{
    error::Error,
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Child, ChildStderr, Command, Output, Stdio},
};

use rusqlite::Connection;

type TestResult = Result<(), Box<dyn Error>>;

const MAIN_REFERENCE: &str = "refs/heads/main";

#[test]
fn checkout_recover_can_abort_or_finish_after_a_publish_crash() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("a.bin"), b"a-v1")?;
    fs::write(repository.path().join("b.bin"), b"b-v1")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v1"])?;
    let first = main_commit_id(repository.path())?;

    fs::write(repository.path().join("a.bin"), b"a-v2")?;
    fs::write(repository.path().join("b.bin"), b"b-v2")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v2"])?;
    let second = main_commit_id(repository.path())?;

    crash_checkout(repository.path(), &first, "checkout-after-publish")?;
    assert_eq!(transaction_count(repository.path())?, 1);
    assert_eq!(main_commit_id(repository.path())?, second);
    assert_eq!(read_head(repository.path())?, symbolic_main_head());
    let blocked = command(repository.path())
        .args(["commit", "-m", "must block"])
        .output()?;
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("recover"));

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(repository.path().join("a.bin"))?, b"a-v2");
    assert_eq!(fs::read(repository.path().join("b.bin"))?, b"b-v2");
    assert_eq!(main_commit_id(repository.path())?, second);
    assert_eq!(read_head(repository.path())?, symbolic_main_head());
    assert_eq!(transaction_count(repository.path())?, 0);

    crash_checkout(repository.path(), &first, "checkout-after-publish")?;
    run_success(repository.path(), &["recover"])?;
    assert_eq!(fs::read(repository.path().join("a.bin"))?, b"a-v1");
    assert_eq!(fs::read(repository.path().join("b.bin"))?, b"b-v1");
    assert_eq!(main_commit_id(repository.path())?, second);
    assert_eq!(read_head(repository.path())?, StoredHead::Detached(first));
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn checkout_recovery_restores_or_finishes_after_index_publish_crash() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("model.bin"), b"v1")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v1"])?;
    let first = main_commit_id(repository.path())?;
    let first_index = read_index(repository.path())?;

    fs::write(repository.path().join("model.bin"), b"v2")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v2"])?;
    let second = main_commit_id(repository.path())?;
    let index_before = read_index(repository.path())?;

    crash_checkout(repository.path(), &first, "checkout-after-index")?;
    assert_eq!(fs::read(repository.path().join("model.bin"))?, b"v1");
    assert_eq!(read_index(repository.path())?, first_index);
    assert_eq!(main_commit_id(repository.path())?, second);
    assert_eq!(read_head(repository.path())?, symbolic_main_head());

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(repository.path().join("model.bin"))?, b"v2");
    assert_eq!(read_index(repository.path())?, index_before);
    assert_eq!(main_commit_id(repository.path())?, second);
    assert_eq!(read_head(repository.path())?, symbolic_main_head());
    assert_eq!(transaction_count(repository.path())?, 0);

    crash_checkout(repository.path(), &first, "checkout-after-index")?;
    run_success(repository.path(), &["recover"])?;
    assert_eq!(fs::read(repository.path().join("model.bin"))?, b"v1");
    assert_eq!(read_index(repository.path())?, first_index);
    assert_eq!(main_commit_id(repository.path())?, second);
    assert_eq!(read_head(repository.path())?, StoredHead::Detached(first));
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn checkout_recovery_handles_a_crash_after_detached_head_publish() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");

    fs::write(&model, b"v1")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v1"])?;
    let first = main_commit_id(repository.path())?;
    let first_index = read_index(repository.path())?;

    fs::write(&model, b"v2")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v2"])?;
    let second = main_commit_id(repository.path())?;
    let second_index = read_index(repository.path())?;

    crash_checkout(repository.path(), &first, "checkout-after-head")?;
    assert_eq!(fs::read(&model)?, b"v1");
    assert_eq!(read_index(repository.path())?, first_index);
    assert_eq!(main_commit_id(repository.path())?, second);
    assert_eq!(
        read_head(repository.path())?,
        StoredHead::Detached(first.clone())
    );
    assert_eq!(transaction_count(repository.path())?, 1);

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"v2");
    assert_eq!(read_index(repository.path())?, second_index);
    assert_eq!(main_commit_id(repository.path())?, second);
    assert_eq!(read_head(repository.path())?, symbolic_main_head());
    assert_eq!(transaction_count(repository.path())?, 0);

    crash_checkout(repository.path(), &first, "checkout-after-head")?;
    run_success(repository.path(), &["recover"])?;
    assert_eq!(fs::read(&model)?, b"v1");
    assert_eq!(read_index(repository.path())?, first_index);
    assert_eq!(main_commit_id(repository.path())?, second);
    assert_eq!(read_head(repository.path())?, StoredHead::Detached(first));
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn rm_recovery_rolls_back_before_index_publish_and_finishes_after_it() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"model")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    let crashed = command(repository.path())
        .env("NEOENGRAM_TEST_CRASH_AT", "rm-after-backup")
        .args(["rm", "model.bin"])
        .output()?;
    assert_crashed(&crashed);
    assert!(!model.exists());
    assert_eq!(read_index(repository.path())?.files.len(), 1);
    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"model");
    assert_eq!(read_index(repository.path())?.files.len(), 1);

    let crashed = command(repository.path())
        .env("NEOENGRAM_TEST_CRASH_AT", "rm-after-index")
        .args(["rm", "model.bin"])
        .output()?;
    assert_crashed(&crashed);
    assert!(!model.exists());
    assert!(read_index(repository.path())?.files.is_empty());

    let refused_abort = command(repository.path())
        .args(["recover", "--abort"])
        .output()?;
    assert!(!refused_abort.status.success());
    assert_eq!(transaction_count(repository.path())?, 1);
    run_success(repository.path(), &["recover"])?;
    assert_eq!(transaction_count(repository.path())?, 0);
    assert!(!model.exists());
    assert!(read_index(repository.path())?.files.is_empty());
    Ok(())
}

#[test]
fn checkout_abort_preserves_a_file_created_after_an_unpublished_intent() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("keep.bin"), b"keep")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "without new file"])?;
    let without_file = main_commit_id(repository.path())?;

    fs::write(repository.path().join("new.bin"), b"committed target")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "with new file"])?;
    let with_file = main_commit_id(repository.path())?;
    run_success(
        repository.path(),
        &["checkout", without_file.as_str(), "--force"],
    )?;
    assert!(!repository.path().join("new.bin").exists());

    crash_checkout(repository.path(), &with_file, "checkout-after-intent")?;
    fs::write(repository.path().join("new.bin"), b"created after crash")?;
    run_success(repository.path(), &["recover", "--abort"])?;

    assert_eq!(
        fs::read(repository.path().join("new.bin"))?,
        b"created after crash"
    );
    assert_eq!(transaction_count(repository.path())?, 0);
    assert_eq!(main_commit_id(repository.path())?, with_file);
    assert_eq!(
        read_head(repository.path())?,
        StoredHead::Detached(without_file)
    );
    let index = read_index(repository.path())?;
    assert!(index.files.iter().all(|file| file.path != "new.bin"));
    Ok(())
}

#[test]
fn checkout_transaction_is_visible_only_after_atomic_publish() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"v1")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v1"])?;
    let first = main_commit_id(repository.path())?;

    fs::write(&model, b"v2")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v2"])?;
    let second_index = read_index(repository.path())?;

    crash_checkout(
        repository.path(),
        &first,
        "checkout-before-transaction-publish",
    )?;
    assert_eq!(fs::read(&model)?, b"v2");
    assert_eq!(read_index(repository.path())?, second_index);
    let drafts = transaction_names(repository.path())?;
    assert_eq!(drafts.len(), 1);
    assert!(drafts
        .iter()
        .all(|name| name.starts_with(".neoengram-tmp-checkout-")));
    run_success(repository.path(), &["recover"])?;
    assert_eq!(transaction_count(repository.path())?, 0);

    crash_checkout(
        repository.path(),
        &first,
        "checkout-after-transaction-publish",
    )?;
    assert_eq!(fs::read(&model)?, b"v2");
    assert_eq!(read_index(repository.path())?, second_index);
    let transactions = transaction_names(repository.path())?;
    assert_eq!(transactions.len(), 1);
    assert!(transactions
        .iter()
        .all(|name| name.starts_with("checkout-")));
    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"v2");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn rm_transaction_is_visible_only_after_atomic_publish() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"model")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    let crashed = command(repository.path())
        .env("NEOENGRAM_TEST_CRASH_AT", "rm-before-transaction-publish")
        .args(["rm", "model.bin"])
        .output()?;
    assert_crashed(&crashed);
    assert_eq!(fs::read(&model)?, b"model");
    assert_eq!(read_index(repository.path())?.files.len(), 1);
    let drafts = transaction_names(repository.path())?;
    assert_eq!(drafts.len(), 1);
    assert!(drafts
        .iter()
        .all(|name| name.starts_with(".neoengram-tmp-rm-")));
    run_success(repository.path(), &["recover"])?;
    assert_eq!(transaction_count(repository.path())?, 0);

    let crashed = command(repository.path())
        .env("NEOENGRAM_TEST_CRASH_AT", "rm-after-transaction-publish")
        .args(["rm", "model.bin"])
        .output()?;
    assert_crashed(&crashed);
    assert_eq!(fs::read(&model)?, b"model");
    assert_eq!(read_index(repository.path())?.files.len(), 1);
    let transactions = transaction_names(repository.path())?;
    assert_eq!(transactions.len(), 1);
    assert!(transactions.iter().all(|name| name.starts_with("rm-")));
    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"model");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn checkout_recovery_preserves_transaction_when_original_and_backup_are_missing() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"v1")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v1"])?;
    let first = main_commit_id(repository.path())?;

    fs::write(&model, b"v2")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v2"])?;
    crash_checkout(repository.path(), &first, "checkout-after-intent")?;
    fs::remove_file(&model)?;

    let failed = command(repository.path())
        .args(["recover", "--abort"])
        .output()?;
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("同时缺失"));
    assert_eq!(transaction_count(repository.path())?, 1);

    fs::write(&model, b"v2")?;
    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"v2");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[cfg(unix)]
#[test]
fn checkout_abort_does_not_treat_an_unreadable_staged_path_as_missing() -> TestResult {
    use std::os::unix::fs::PermissionsExt;

    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("keep.bin"), b"keep")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "without target"])?;
    let without_target = main_commit_id(repository.path())?;

    fs::write(repository.path().join("target.bin"), b"target")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "with target"])?;
    let with_target = main_commit_id(repository.path())?;
    run_success(
        repository.path(),
        &["checkout", without_target.as_str(), "--force"],
    )?;

    crash_checkout(repository.path(), &with_target, "checkout-after-intent")?;
    let target = repository.path().join("target.bin");
    fs::write(&target, b"target")?;
    let transaction = transaction_names(repository.path())?
        .into_iter()
        .next()
        .ok_or_else(|| std::io::Error::other("missing checkout transaction"))?;
    let staged = repository
        .path()
        .join(".neoengram/transactions")
        .join(transaction)
        .join("staged");
    fs::set_permissions(&staged, fs::Permissions::from_mode(0o000))?;
    let failed = command(repository.path())
        .args(["recover", "--abort"])
        .output()?;
    fs::set_permissions(&staged, fs::Permissions::from_mode(0o755))?;

    assert!(!failed.status.success());
    assert_eq!(fs::read(&target)?, b"target");
    assert_eq!(transaction_count(repository.path())?, 1);

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&target)?, b"target");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn checkout_recovery_handles_file_to_directory_transition_after_publish_crash() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let parent = repository.path().join("a");

    fs::write(&parent, b"file version")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "file"])?;
    let file_commit = main_commit_id(repository.path())?;

    fs::remove_file(&parent)?;
    fs::create_dir(&parent)?;
    fs::write(parent.join("b.bin"), b"directory version")?;
    run_success(repository.path(), &["add", "-A", "."])?;
    run_success(repository.path(), &["commit", "-m", "directory"])?;
    let directory_commit = main_commit_id(repository.path())?;

    run_success(
        repository.path(),
        &["checkout", file_commit.as_str(), "--force"],
    )?;
    crash_checkout(
        repository.path(),
        &directory_commit,
        "checkout-after-publish",
    )?;
    assert_eq!(fs::read(parent.join("b.bin"))?, b"directory version");
    assert_eq!(transaction_count(repository.path())?, 1);

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&parent)?, b"file version");
    assert_eq!(
        read_head(repository.path())?,
        StoredHead::Detached(file_commit)
    );
    assert_eq!(transaction_count(repository.path())?, 0);

    crash_checkout(
        repository.path(),
        &directory_commit,
        "checkout-after-publish",
    )?;
    run_success(repository.path(), &["recover"])?;
    assert_eq!(fs::read(parent.join("b.bin"))?, b"directory version");
    assert_eq!(
        read_head(repository.path())?,
        StoredHead::Detached(directory_commit)
    );
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn checkout_publish_does_not_replace_a_file_created_after_backup() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let (first, second) = create_two_model_commits(repository.path())?;
    run_success(repository.path(), &["checkout", first.as_str(), "--force"])?;

    let (mut checkout, mut stderr) = spawn_paused(
        repository.path(),
        &["checkout", second.as_str(), "--force"],
        "checkout-before-worktree-publish",
    )?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"external")?;
    let (success, error) = release_paused(&mut checkout, &mut stderr)?;
    assert!(!success, "checkout unexpectedly replaced a racing file");
    assert!(!error.is_empty());
    assert_eq!(fs::read(&model)?, b"external");
    assert_eq!(transaction_count(repository.path())?, 1);

    fs::remove_file(&model)?;
    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"v1");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn checkout_abort_does_not_replace_a_file_created_during_backup_restore() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let (first, second) = create_two_model_commits(repository.path())?;
    run_success(repository.path(), &["checkout", first.as_str(), "--force"])?;
    crash_checkout(repository.path(), &second, "checkout-after-publish")?;

    let (mut recover, mut stderr) = spawn_paused(
        repository.path(),
        &["recover", "--abort"],
        "checkout-before-backup-restore",
    )?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"external")?;
    let (success, error) = release_paused(&mut recover, &mut stderr)?;
    assert!(!success, "recover unexpectedly replaced a racing file");
    assert!(!error.is_empty());
    assert_eq!(fs::read(&model)?, b"external");
    assert_eq!(transaction_count(repository.path())?, 1);

    fs::remove_file(&model)?;
    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"v1");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn checkout_reset_crash_does_not_reuse_destroyed_publication_evidence() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("keep.bin"), b"keep")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "without target"])?;
    let without_target = main_commit_id(repository.path())?;

    let target = repository.path().join("target.bin");
    fs::write(&target, b"target")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "with target"])?;
    let with_target = main_commit_id(repository.path())?;
    run_success(
        repository.path(),
        &["checkout", without_target.as_str(), "--force"],
    )?;
    crash_checkout(repository.path(), &with_target, "checkout-after-publish")?;

    let crashed = command(repository.path())
        .env("NEOENGRAM_TEST_CRASH_AT", "checkout-after-reset-journal")
        .args(["recover"])
        .output()?;
    assert_crashed(&crashed);
    assert!(!target.exists());
    fs::write(&target, b"target")?;

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&target)?, b"target");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn rm_recovery_preserves_both_original_and_backup_on_ambiguity() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"original")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    let crashed = command(repository.path())
        .env("NEOENGRAM_TEST_CRASH_AT", "rm-after-backup")
        .args(["rm", "model.bin"])
        .output()?;
    assert_crashed(&crashed);
    fs::write(&model, b"external")?;

    let failed = command(repository.path())
        .args(["recover", "--abort"])
        .output()?;
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("同时存在"));
    assert_eq!(fs::read(&model)?, b"external");
    assert_eq!(transaction_count(repository.path())?, 1);

    fs::remove_file(&model)?;
    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"original");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn rm_recovery_preserves_transaction_when_original_and_backup_are_missing() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"original")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    let crashed = command(repository.path())
        .env("NEOENGRAM_TEST_CRASH_AT", "rm-after-backup")
        .args(["rm", "model.bin"])
        .output()?;
    assert_crashed(&crashed);
    let transaction = transaction_names(repository.path())?
        .into_iter()
        .next()
        .ok_or_else(|| std::io::Error::other("missing rm transaction"))?;
    fs::remove_file(
        repository
            .path()
            .join(".neoengram/transactions")
            .join(transaction)
            .join("backup/model.bin"),
    )?;

    let failed = command(repository.path())
        .args(["recover", "--abort"])
        .output()?;
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("同时缺失"));
    assert_eq!(transaction_count(repository.path())?, 1);

    fs::write(&model, b"original")?;
    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"original");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn rm_abort_does_not_replace_a_file_created_during_backup_restore() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"original")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    let crashed = command(repository.path())
        .env("NEOENGRAM_TEST_CRASH_AT", "rm-after-backup")
        .args(["rm", "model.bin"])
        .output()?;
    assert_crashed(&crashed);
    let (mut recover, mut stderr) = spawn_paused(
        repository.path(),
        &["recover", "--abort"],
        "rm-before-backup-restore",
    )?;
    fs::write(&model, b"external")?;
    let (success, error) = release_paused(&mut recover, &mut stderr)?;
    assert!(!success, "recover unexpectedly replaced a racing file");
    assert!(!error.is_empty());
    assert_eq!(fs::read(&model)?, b"external");
    assert_eq!(transaction_count(repository.path())?, 1);

    fs::remove_file(&model)?;
    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"original");
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn rm_records_a_move_before_post_rename_sync_can_fail() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"only copy")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;
    let index_before = read_index(repository.path())?;

    let failed = command(repository.path())
        .env("NEOENGRAM_TEST_FAIL_AFTER_RENAME", "before-sync")
        .args(["rm", "model.bin"])
        .output()?;
    assert!(!failed.status.success());
    assert_eq!(fs::read(&model)?, b"only copy");
    assert_eq!(read_index(repository.path())?, index_before);
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

fn crash_checkout(repository: &Path, target: &str, point: &str) -> TestResult {
    let output = command(repository)
        .env("NEOENGRAM_TEST_CRASH_AT", point)
        .args(["checkout", target, "--force"])
        .output()?;
    assert_crashed(&output);
    Ok(())
}

fn assert_crashed(output: &Output) {
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("test crash at"));
}

fn create_two_model_commits(repository: &Path) -> Result<(String, String), Box<dyn Error>> {
    let model = repository.join("model.bin");
    fs::write(&model, b"v1")?;
    run_success(repository, &["add", "."])?;
    run_success(repository, &["commit", "-m", "v1"])?;
    let first = main_commit_id(repository)?;

    fs::write(&model, b"v2")?;
    run_success(repository, &["add", "."])?;
    run_success(repository, &["commit", "-m", "v2"])?;
    let second = main_commit_id(repository)?;
    Ok((first, second))
}

fn spawn_paused(
    repository: &Path,
    arguments: &[&str],
    point: &str,
) -> Result<(Child, BufReader<ChildStderr>), Box<dyn Error>> {
    let mut child = command(repository)
        .env("NEOENGRAM_TEST_PAUSE_AT", point)
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| std::io::Error::other("paused child has no stderr"))?;
    let mut stderr = BufReader::new(stderr);
    let mut marker = String::new();
    stderr.read_line(&mut marker)?;
    assert_eq!(marker.trim(), format!("test pause at {point}"));
    Ok((child, stderr))
}

fn release_paused(
    child: &mut Child,
    stderr: &mut BufReader<ChildStderr>,
) -> Result<(bool, String), Box<dyn Error>> {
    child
        .stdin
        .take()
        .ok_or_else(|| std::io::Error::other("paused child has no stdin"))?
        .write_all(b"\n")?;
    let status = child.wait()?;
    let mut error = String::new();
    stderr.read_to_string(&mut error)?;
    Ok((status.success(), error))
}

fn initialize(path: &Path) -> TestResult {
    run_success(path, &["init"])?;
    Ok(())
}

fn run_success(path: &Path, arguments: &[&str]) -> Result<Output, Box<dyn Error>> {
    let output = command(path).args(arguments).output()?;
    assert!(
        output.status.success(),
        "command failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output)
}

fn command(current_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_neoengram"));
    command.current_dir(current_dir);
    command
}

#[derive(Debug, PartialEq, Eq)]
struct StoredIndex {
    format_version: u32,
    files: Vec<StoredFileRecord>,
}

#[derive(Debug, PartialEq, Eq)]
struct StoredFileRecord {
    path: String,
    total_size: u64,
    chunk_count: u64,
    manifest_id: String,
}

fn read_index(repository: &Path) -> Result<StoredIndex, Box<dyn Error>> {
    let connection = Connection::open(repository.join(".neoengram/metadata/metadata.sqlite3"))?;
    let format_version: i64 = connection.query_row(
        "SELECT index_format FROM workspaces WHERE id = zeroblob(16)",
        [],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT path, total_size, chunk_count, lower(hex(manifest_id)) \
         FROM workspace_index_files WHERE workspace_id = zeroblob(16) ORDER BY path",
    )?;
    let files = statement
        .query_map([], |row| {
            Ok(StoredFileRecord {
                path: row.get(0)?,
                total_size: u64::try_from(row.get::<_, i64>(1)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        error.into(),
                    )
                })?,
                chunk_count: u64::try_from(row.get::<_, i64>(2)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Integer,
                        error.into(),
                    )
                })?,
                manifest_id: row.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StoredIndex {
        format_version: u32::try_from(format_version)?,
        files,
    })
}

#[derive(Debug, PartialEq, Eq)]
enum StoredHead {
    Symbolic(String),
    Detached(String),
}

fn read_head(repository: &Path) -> Result<StoredHead, Box<dyn Error>> {
    let connection = Connection::open(repository.join(".neoengram/metadata/metadata.sqlite3"))?;
    let (kind, target): (String, String) = connection.query_row(
        "SELECT head_kind, head_target FROM workspaces WHERE id = zeroblob(16)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if kind == "attached" {
        Ok(StoredHead::Symbolic(target))
    } else {
        Ok(StoredHead::Detached(target))
    }
}

fn symbolic_main_head() -> StoredHead {
    StoredHead::Symbolic(MAIN_REFERENCE.to_owned())
}

fn main_commit_id(repository: &Path) -> Result<String, Box<dyn Error>> {
    let connection = Connection::open(repository.join(".neoengram/metadata/metadata.sqlite3"))?;
    Ok(connection.query_row(
        "SELECT target FROM refs WHERE name = 'refs/heads/main'",
        [],
        |row| row.get(0),
    )?)
}

fn transaction_count(repository: &Path) -> Result<usize, Box<dyn Error>> {
    Ok(fs::read_dir(repository.join(".neoengram/transactions"))?
        .collect::<Result<Vec<_>, _>>()?
        .len())
}

fn transaction_names(repository: &Path) -> Result<Vec<String>, Box<dyn Error>> {
    let mut names = fs::read_dir(repository.join(".neoengram/transactions"))?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>, _>>()?;
    names.sort_unstable();
    Ok(names)
}
