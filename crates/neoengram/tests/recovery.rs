use std::{
    error::Error,
    fs,
    path::Path,
    process::{Command, Output},
};

use serde::Deserialize;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn checkout_recover_can_abort_or_finish_after_a_publish_crash() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("a.bin"), b"a-v1")?;
    fs::write(repository.path().join("b.bin"), b"b-v1")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v1"])?;
    let first = current_commit_id(repository.path())?;

    fs::write(repository.path().join("a.bin"), b"a-v2")?;
    fs::write(repository.path().join("b.bin"), b"b-v2")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v2"])?;
    let second = current_commit_id(repository.path())?;

    crash_checkout(repository.path(), &first, "checkout-after-publish")?;
    assert_eq!(transaction_count(repository.path())?, 1);
    let blocked = command(repository.path())
        .args(["commit", "-m", "must block"])
        .output()?;
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("recover"));

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(repository.path().join("a.bin"))?, b"a-v2");
    assert_eq!(fs::read(repository.path().join("b.bin"))?, b"b-v2");
    assert_eq!(current_commit_id(repository.path())?, second);
    assert_eq!(transaction_count(repository.path())?, 0);

    crash_checkout(repository.path(), &first, "checkout-after-publish")?;
    run_success(repository.path(), &["recover"])?;
    assert_eq!(fs::read(repository.path().join("a.bin"))?, b"a-v1");
    assert_eq!(fs::read(repository.path().join("b.bin"))?, b"b-v1");
    assert_eq!(current_commit_id(repository.path())?, second);
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
}

#[test]
fn checkout_abort_restores_index_after_it_was_atomically_published() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("model.bin"), b"v1")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v1"])?;
    let first = current_commit_id(repository.path())?;

    fs::write(repository.path().join("model.bin"), b"v2")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "v2"])?;
    let index_before = read_index(repository.path())?;

    crash_checkout(repository.path(), &first, "checkout-after-index")?;
    assert_eq!(fs::read(repository.path().join("model.bin"))?, b"v1");
    assert_ne!(read_index(repository.path())?, index_before);

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(repository.path().join("model.bin"))?, b"v2");
    assert_eq!(read_index(repository.path())?, index_before);
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
    let without_file = current_commit_id(repository.path())?;

    fs::write(repository.path().join("new.bin"), b"committed target")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "with new file"])?;
    let with_file = current_commit_id(repository.path())?;
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
    let index = read_index(repository.path())?;
    assert!(index.files.iter().all(|file| file.path != "new.bin"));
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

fn initialize(path: &Path) -> TestResult {
    run_success(path, &["init", "--metadata-store", "json"])?;
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

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct StoredIndex {
    format_version: u32,
    files: Vec<StoredFileRecord>,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct StoredFileRecord {
    path: String,
    total_size: u64,
    chunk_count: u64,
    manifest_id: String,
}

fn read_index(repository: &Path) -> Result<StoredIndex, Box<dyn Error>> {
    Ok(serde_json::from_slice(&fs::read(
        repository.join(".neoengram/metadata/index.json"),
    )?)?)
}

fn current_commit_id(repository: &Path) -> Result<String, Box<dyn Error>> {
    Ok(
        fs::read_to_string(repository.join(".neoengram/metadata/refs/heads/main"))?
            .trim()
            .to_owned(),
    )
}

fn transaction_count(repository: &Path) -> Result<usize, Box<dyn Error>> {
    Ok(fs::read_dir(repository.join(".neoengram/transactions"))?
        .collect::<Result<Vec<_>, _>>()?
        .len())
}
