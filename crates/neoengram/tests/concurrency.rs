use std::{
    error::Error,
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Child, ChildStderr, Command, Output, Stdio},
};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn add_shared_lock_excludes_worktree_writers() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"v1")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;
    fs::write(&model, b"v2")?;

    let (mut add, mut add_stderr) = spawn_paused(
        repository.path(),
        &["add", "model.bin"],
        "add-before-index-publish",
    )?;

    // A second process can take another shared worktree lock while add is paused.
    run_success(repository.path(), &["status"])?;

    for arguments in [
        &["checkout", "HEAD", "--force"][..],
        &["rm", "model.bin", "--force"][..],
        &["restore", "model.bin", "--force"][..],
    ] {
        let output = command(repository.path()).args(arguments).output()?;
        assert!(
            !output.status.success(),
            "worktree writer unexpectedly succeeded: {arguments:?}"
        );
    }
    assert_eq!(fs::read(&model)?, b"v2");

    let (success, error) = release(&mut add, &mut add_stderr)?;
    assert!(success, "add failed after releasing pause: {error}");
    let status = String::from_utf8(run_success(repository.path(), &["status"])?.stdout)?;
    assert!(status.contains("Changes to be committed:"));
    assert!(status.contains("modified: model.bin"));
    Ok(())
}

#[test]
fn exclusive_worktree_lock_rejects_a_shared_reader_in_another_process() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"tracked")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;
    fs::remove_file(&model)?;

    let (mut restore, mut restore_stderr) = spawn_paused(
        repository.path(),
        &["restore", "model.bin"],
        "restore-before-publish",
    )?;
    let status = command(repository.path()).arg("status").output()?;
    assert!(!status.status.success());
    assert!(String::from_utf8_lossy(&status.stderr).contains("工作区"));

    let (success, error) = release(&mut restore, &mut restore_stderr)?;
    assert!(success, "restore failed after releasing pause: {error}");
    assert_eq!(fs::read(&model)?, b"tracked");
    Ok(())
}

#[test]
fn add_cas_does_not_overwrite_an_index_only_update() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"v1")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    fs::write(&model, b"v2")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    fs::write(repository.path().join("new.bin"), b"new")?;
    let (mut add, mut add_stderr) = spawn_paused(
        repository.path(),
        &["add", "new.bin"],
        "add-before-index-publish",
    )?;

    run_success(repository.path(), &["restore", "--staged", "model.bin"])?;
    let (success, error) = release(&mut add, &mut add_stderr)?;
    assert!(!success, "add unexpectedly overwrote the newer index");
    assert!(
        error.contains("add 扫描期间发生变化"),
        "unexpected add error: {error}"
    );

    let status = String::from_utf8(run_success(repository.path(), &["status"])?.stdout)?;
    assert!(status.contains("Changes not staged for commit:"));
    assert!(status.contains("modified: model.bin"));
    assert!(status.contains("Untracked files:"));
    assert!(status.contains("new.bin"));
    Ok(())
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

fn release(
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
