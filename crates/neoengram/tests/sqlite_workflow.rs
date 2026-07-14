use std::{error::Error, fs, path::Path, process::Command};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn sqlite_backend_supports_the_cli_workflow_across_processes() -> TestResult {
    let repository = tempfile::tempdir()?;

    run_success(repository.path(), &["init", "."])?;
    fs::write(repository.path().join("model.bin"), b"model-v1")?;

    run_success(repository.path(), &["add", "model.bin"])?;
    let status = run_success(repository.path(), &["status"])?;
    assert!(String::from_utf8(status.stdout)?.contains("added: model.bin"));

    run_success(repository.path(), &["commit", "-m", "sqlite snapshot"])?;
    let log = run_success(repository.path(), &["log"])?;
    assert!(String::from_utf8(log.stdout)?.contains("sqlite snapshot"));
    let show = run_success(repository.path(), &["show", "HEAD"])?;
    assert!(String::from_utf8(show.stdout)?.contains("model.bin"));
    run_success(repository.path(), &["fsck"])?;

    fs::write(repository.path().join("model.bin"), b"modified")?;
    run_success(repository.path(), &["checkout", "HEAD", "--force"])?;
    assert_eq!(fs::read(repository.path().join("model.bin"))?, b"model-v1");
    Ok(())
}

#[test]
fn sqlite_backend_recovers_checkout_after_an_index_publish_crash() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init", "."])?;

    let model = repository.path().join("model.bin");
    fs::write(&model, b"model-v1")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "v1"])?;
    let first_commit = head_commit_id(repository.path())?;

    fs::write(&model, b"model-v2")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "v2"])?;
    let second_commit = head_commit_id(repository.path())?;

    crash_checkout(repository.path(), &first_commit, "checkout-after-index")?;
    assert_eq!(fs::read(&model)?, b"model-v1");
    assert_head_commit(repository.path(), &second_commit)?;
    assert_eq!(transaction_count(repository.path())?, 1);

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"model-v2");
    assert_head_commit(repository.path(), &second_commit)?;
    assert_eq!(transaction_count(repository.path())?, 0);
    let status = run_success(repository.path(), &["status"])?;
    assert!(String::from_utf8(status.stdout)?.contains("working tree clean"));

    crash_checkout(repository.path(), &first_commit, "checkout-after-index")?;
    run_success(repository.path(), &["recover"])?;
    assert_eq!(fs::read(&model)?, b"model-v1");
    assert_head_commit(repository.path(), &first_commit)?;
    assert_eq!(transaction_count(repository.path())?, 0);
    let status = run_success(repository.path(), &["status"])?;
    assert!(String::from_utf8(status.stdout)?.contains("working tree clean"));
    run_success(repository.path(), &["fsck"])?;
    Ok(())
}

#[test]
fn sqlite_backend_recovers_checkout_after_detached_head_publish() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init", "."])?;

    let model = repository.path().join("model.bin");
    fs::write(&model, b"model-v1")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "v1"])?;
    let first_commit = head_commit_id(repository.path())?;

    fs::write(&model, b"model-v2")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "v2"])?;
    let second_commit = head_commit_id(repository.path())?;

    crash_checkout(repository.path(), &first_commit, "checkout-after-head")?;
    assert_eq!(fs::read(&model)?, b"model-v1");
    assert_head_commit(repository.path(), &first_commit)?;
    assert_eq!(transaction_count(repository.path())?, 1);

    run_success(repository.path(), &["recover", "--abort"])?;
    assert_eq!(fs::read(&model)?, b"model-v2");
    assert_head_commit(repository.path(), &second_commit)?;
    assert_eq!(transaction_count(repository.path())?, 0);

    crash_checkout(repository.path(), &first_commit, "checkout-after-head")?;
    run_success(repository.path(), &["recover"])?;
    assert_eq!(fs::read(&model)?, b"model-v1");
    assert_head_commit(repository.path(), &first_commit)?;
    assert_eq!(transaction_count(repository.path())?, 0);
    let status = run_success(repository.path(), &["status"])?;
    assert!(String::from_utf8(status.stdout)?.contains("working tree clean"));
    run_success(repository.path(), &["fsck"])?;
    Ok(())
}

fn crash_checkout(repository: &Path, target: &str, point: &str) -> TestResult {
    let output = command(repository)
        .env("NEOENGRAM_TEST_CRASH_AT", point)
        .args(["checkout", target, "--force"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains(point));
    Ok(())
}

fn assert_head_commit(repository: &Path, expected: &str) -> TestResult {
    assert_eq!(head_commit_id(repository)?, expected);
    let show = run_success(repository, &["show", "HEAD"])?;
    let show = String::from_utf8(show.stdout)?;
    assert!(
        show.starts_with(&format!("commit {expected}\n")),
        "show HEAD resolved to an unexpected commit:\n{show}"
    );
    Ok(())
}

fn head_commit_id(repository: &Path) -> Result<String, Box<dyn Error>> {
    let output = run_success(repository, &["log", "--max-count", "1"])?;
    let stdout = String::from_utf8(output.stdout)?;
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("commit "))
        .map(str::to_owned)
        .ok_or_else(|| "log output did not contain a commit ID".into())
}

fn transaction_count(repository: &Path) -> Result<usize, Box<dyn Error>> {
    Ok(fs::read_dir(repository.join(".neoengram/transactions"))?
        .collect::<Result<Vec<_>, _>>()?
        .len())
}

fn run_success(
    repository: &Path,
    arguments: &[&str],
) -> Result<std::process::Output, Box<dyn Error>> {
    let output = command(repository).args(arguments).output()?;
    if !output.status.success() {
        return Err(format!(
            "neoengram {} failed:\n{}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output)
}

fn command(current_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_neoengram"));
    command.current_dir(current_dir);
    command
}
