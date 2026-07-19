#![cfg(unix)]

use std::{
    error::Error,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

use serde::Deserialize;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn rm_preserves_data_when_source_directory_sync_fails_after_rename() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let locked = repository.path().join("locked");
    fs::create_dir(&locked)?;
    let model = locked.join("model.bin");
    fs::write(&model, b"unique payload")?;
    run_success(repository.path(), &["add", "locked/model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    fs::set_permissions(&locked, fs::Permissions::from_mode(0o333))?;
    let failed = command(repository.path())
        .args(["rm", "--force", "locked/model.bin"])
        .output()?;
    // Restore permissions immediately so assertions and TempDir cleanup cannot strand the test.
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755))?;

    assert!(
        !failed.status.success(),
        "injected rm unexpectedly succeeded"
    );
    assert_eq!(fs::read(&model)?, b"unique payload");
    assert_eq!(read_index(repository.path())?.files.len(), 1);
    assert_eq!(transaction_count(repository.path())?, 1);

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"unique payload");
    assert_eq!(read_index(repository.path())?.files.len(), 1);
    assert_eq!(transaction_count(repository.path())?, 0);
    Ok(())
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

#[derive(Deserialize)]
struct StoredIndex {
    files: Vec<StoredFileRecord>,
}

#[derive(Deserialize)]
struct StoredFileRecord {}

fn read_index(repository: &Path) -> Result<StoredIndex, Box<dyn Error>> {
    Ok(serde_json::from_slice(&fs::read(
        repository.join(".neoengram/metadata/index.json"),
    )?)?)
}

fn transaction_count(repository: &Path) -> Result<usize, Box<dyn Error>> {
    Ok(fs::read_dir(repository.join(".neoengram/transactions"))?
        .collect::<Result<Vec<_>, _>>()?
        .len())
}
