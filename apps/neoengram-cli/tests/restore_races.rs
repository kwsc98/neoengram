use std::{
    error::Error,
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Child, ChildStderr, Command, Output, Stdio},
};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn restore_revalidates_the_destination_at_publication() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"tracked")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    fs::remove_file(&model)?;
    let (mut child, mut stderr) = spawn_paused_restore(
        repository.path(),
        &["restore", "model.bin"],
        "restore-before-publish",
    )?;
    fs::write(&model, b"created after preflight")?;
    let (success, error) = release(&mut child, &mut stderr)?;
    assert!(!success, "restore unexpectedly succeeded");
    assert!(error.contains("未暂存修改"), "unexpected error: {error}");
    assert_eq!(fs::read(&model)?, b"created after preflight");

    fs::write(&model, b"dirty before restore")?;
    let (mut child, mut stderr) = spawn_paused_restore(
        repository.path(),
        &["restore", "--force", "model.bin"],
        "restore-before-publish",
    )?;
    fs::write(&model, b"dirty after preflight")?;
    let (success, error) = release(&mut child, &mut stderr)?;
    assert!(success, "forced restore failed: {error}");
    assert_eq!(fs::read(&model)?, b"tracked");
    Ok(())
}

#[test]
fn restore_uses_no_replace_when_the_destination_was_absent() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"tracked")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;
    fs::remove_file(&model)?;

    let (mut child, mut stderr) = spawn_paused_restore(
        repository.path(),
        &["restore", "--force", "model.bin"],
        "restore-before-noreplace",
    )?;
    fs::write(&model, b"won publication race")?;
    let (success, error) = release(&mut child, &mut stderr)?;
    assert!(!success, "restore unexpectedly replaced a new file");
    assert!(error.contains("发布前被创建"), "unexpected error: {error}");
    assert_eq!(fs::read(&model)?, b"won publication race");
    Ok(())
}

#[test]
fn restore_never_replaces_a_directory_created_after_preflight() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"tracked")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;
    fs::remove_file(&model)?;

    let (mut child, mut stderr) = spawn_paused_restore(
        repository.path(),
        &["restore", "--force", "model.bin"],
        "restore-before-publish",
    )?;
    fs::create_dir(&model)?;
    let (success, error) = release(&mut child, &mut stderr)?;
    assert!(!success, "restore unexpectedly replaced a directory");
    assert!(error.contains("不是普通文件"), "unexpected error: {error}");
    assert!(model.is_dir());
    Ok(())
}

#[cfg(unix)]
#[test]
fn restore_never_replaces_a_symlink_or_special_file_created_after_preflight() -> TestResult {
    use std::os::unix::fs::{symlink, FileTypeExt};

    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"tracked")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;
    fs::remove_file(&model)?;

    let outside = repository.path().join("outside.bin");
    fs::write(&outside, b"outside")?;
    let (mut child, mut stderr) = spawn_paused_restore(
        repository.path(),
        &["restore", "--force", "model.bin"],
        "restore-before-publish",
    )?;
    symlink(&outside, &model)?;
    let (success, error) = release(&mut child, &mut stderr)?;
    assert!(!success, "restore unexpectedly replaced a symlink");
    assert!(error.contains("不是普通文件"), "unexpected error: {error}");
    assert!(fs::symlink_metadata(&model)?.file_type().is_symlink());
    assert_eq!(fs::read(&outside)?, b"outside");
    fs::remove_file(&model)?;

    let (mut child, mut stderr) = spawn_paused_restore(
        repository.path(),
        &["restore", "--force", "model.bin"],
        "restore-before-publish",
    )?;
    let mkfifo = Command::new("mkfifo").arg(&model).status()?;
    assert!(mkfifo.success(), "mkfifo failed for {}", model.display());
    let (success, error) = release(&mut child, &mut stderr)?;
    assert!(!success, "restore unexpectedly replaced a special file");
    assert!(error.contains("不是普通文件"), "unexpected error: {error}");
    assert!(fs::symlink_metadata(&model)?.file_type().is_fifo());
    Ok(())
}

fn spawn_paused_restore(
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
        .ok_or_else(|| std::io::Error::other("restore child has no stderr"))?;
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
        .ok_or_else(|| std::io::Error::other("restore child has no stdin"))?
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
