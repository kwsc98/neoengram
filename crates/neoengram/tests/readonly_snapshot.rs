#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

use rusqlite::Connection;

type RepositoryState = (String, String, i64, Vec<u8>);

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn export_materializes_an_atomic_external_snapshot() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init"])?;

    fs::create_dir(repository.join("nested"))?;
    fs::write(repository.join("nested/model.bin"), b"model-v1")?;
    fs::write(repository.join("empty.bin"), [])?;
    run_success(&repository, &["add", "."])?;
    run_success(&repository, &["commit", "-m", "snapshot"])?;

    let state_before = repository_state(&repository)?;
    let worktree_before = fs::read(repository.join("nested/model.bin"))?;
    // macOS exposes `/var` as a symlink to `/private/var`; pass a canonical parent because the
    // command intentionally rejects symlinked destination ancestors.
    let destination = fs::canonicalize(outer.path())?.join("readonly");
    let destination_arg = destination.to_string_lossy().into_owned();

    let output = run_success(&repository, &["export", "HEAD", destination_arg.as_str()])?;
    assert!(String::from_utf8(output.stdout)?.contains("Exported read-only snapshot"));
    assert_eq!(fs::read(destination.join("nested/model.bin"))?, b"model-v1");
    assert_eq!(fs::metadata(destination.join("empty.bin"))?.len(), 0);
    assert_eq!(repository_state(&repository)?, state_before);
    assert_eq!(
        fs::read(repository.join("nested/model.bin"))?,
        worktree_before
    );

    assert_eq!(
        fs::metadata(&destination)?.permissions().mode() & 0o777,
        0o555
    );
    assert_eq!(
        fs::metadata(destination.join("nested/model.bin"))?
            .permissions()
            .mode()
            & 0o777,
        0o444
    );
    assert_eq!(
        fs::metadata(destination.join("empty.bin"))?
            .permissions()
            .mode()
            & 0o777,
        0o444
    );
    assert!(fs::write(destination.join("nested/model.bin"), b"blocked").is_err());

    let existing = run_command(&repository, &["export", "HEAD", destination_arg.as_str()])?;
    assert!(!existing.status.success());
    assert!(String::from_utf8_lossy(&existing.stderr).contains("已存在"));
    Ok(())
}

#[test]
fn export_rejects_destinations_inside_the_worktree() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init"])?;
    let output = run_command(&repository, &["export", "HEAD", "snapshot"])?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("不能位于当前 NeoEngram 工作区"));
    assert!(!repository.join("snapshot").exists());
    Ok(())
}

#[test]
fn export_supports_main_and_explicit_commits_without_touching_head() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init"])?;

    let model = repository.join("model.bin");
    fs::write(&model, b"v1")?;
    let first = commit(&repository, "first")?;
    fs::write(&model, b"v2")?;
    let second = commit(&repository, "second")?;
    let head_before = repository_state(&repository)?;

    let first_destination = fs::canonicalize(outer.path())?.join("first-snapshot");
    run_success(
        &repository,
        &[
            "export",
            first.as_str(),
            first_destination.to_str().unwrap(),
        ],
    )?;
    assert_eq!(fs::read(first_destination.join("model.bin"))?, b"v1");

    let main_destination = fs::canonicalize(outer.path())?.join("main-snapshot");
    run_success(
        &repository,
        &["export", "main", main_destination.to_str().unwrap()],
    )?;
    assert_eq!(fs::read(main_destination.join("model.bin"))?, b"v2");
    assert_eq!(first.len(), 64);
    assert_eq!(second.len(), 64);
    assert_eq!(repository_state(&repository)?, head_before);
    Ok(())
}

#[test]
fn export_does_not_publish_a_partial_directory_on_corrupt_chunk() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init"])?;
    fs::write(repository.join("model.bin"), b"payload")?;
    run_success(&repository, &["add", "."])?;
    commit(&repository, "base")?;

    let object = fs::read_dir(repository.join(".neoengram/objects"))?
        .filter_map(Result::ok)
        .find(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.len() == 64)
        })
        .ok_or("missing loose object")?
        .path();
    fs::write(object, b"broken")?;

    let destination = fs::canonicalize(outer.path())?.join("corrupt-snapshot");
    let output = run_command(
        &repository,
        &["export", "HEAD", destination.to_str().unwrap()],
    )?;
    assert!(!output.status.success());
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn export_works_with_the_default_sqlite_backend() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init"])?;
    fs::write(repository.join("model.bin"), b"sqlite-payload")?;
    commit(&repository, "sqlite")?;

    let destination = fs::canonicalize(outer.path())?.join("sqlite-snapshot");
    run_success(
        &repository,
        &["export", "HEAD", destination.to_str().unwrap()],
    )?;
    assert_eq!(fs::read(destination.join("model.bin"))?, b"sqlite-payload");
    Ok(())
}

#[test]
fn export_drops_staging_when_a_chunk_is_corrupt() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init"])?;
    fs::write(repository.join("model.bin"), b"model payload")?;
    run_success(&repository, &["add", "model.bin"])?;
    run_success(&repository, &["commit", "-m", "corrupt"])?;

    let object = fs::read_dir(repository.join(".neoengram/objects"))?
        .filter_map(Result::ok)
        .find(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.len() == 64)
        })
        .ok_or("missing loose object")?
        .path();
    fs::write(&object, b"corrupt")?;

    let destination = fs::canonicalize(outer.path())?.join("corrupt-readonly");
    let destination_arg = destination.to_string_lossy().into_owned();
    let output = run_command(&repository, &["export", "HEAD", destination_arg.as_str()])?;
    assert!(!output.status.success());
    assert!(!destination.exists());
    assert!(!fs::read_dir(outer.path())?.any(|entry| {
        entry.ok().is_some_and(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".neoengram-read-only-")
        })
    }));
    Ok(())
}

#[test]
fn export_accepts_the_managed_exports_directory() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init"])?;
    fs::write(repository.path().join("model.bin"), b"managed")?;
    commit(repository.path(), "managed")?;
    let destination = fs::canonicalize(repository.path())?.join("exports/release");
    run_success(
        repository.path(),
        &["export", "HEAD", destination.to_str().unwrap()],
    )?;
    assert_eq!(fs::read(destination.join("model.bin"))?, b"managed");
    let status = String::from_utf8(run_success(repository.path(), &["status"])?.stdout)?;
    assert!(status.contains("working tree clean"));
    Ok(())
}

fn run_success(
    path: &Path,
    args: &[&str],
) -> Result<std::process::Output, Box<dyn std::error::Error>> {
    let output = run_command(path, args)?;
    assert!(
        output.status.success(),
        "command {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output)
}

fn repository_state(repository: &Path) -> Result<RepositoryState, Box<dyn std::error::Error>> {
    let connection = Connection::open(repository.join(".neoengram/metadata/metadata.sqlite3"))?;
    Ok(connection.query_row(
        "SELECT head_kind, head_target, index_revision, index_accumulator \
         FROM workspaces WHERE id = zeroblob(16)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?)
}

fn run_command(path: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_neoengram"));
    command.current_dir(path).args(args).output()
}

fn commit(path: &Path, message: &str) -> Result<String, Box<dyn std::error::Error>> {
    run_success(path, &["add", "."])?;
    let output = run_success(path, &["commit", "-m", message])?;
    let stdout = String::from_utf8(output.stdout)?;
    Ok(stdout
        .lines()
        .find_map(|line| line.strip_prefix("Committed "))
        .ok_or("commit output did not contain an id")?
        .trim()
        .to_owned())
}
