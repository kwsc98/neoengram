#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn checkout_read_only_materializes_an_atomic_external_snapshot() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init", "--metadata-store", "json"])?;

    fs::create_dir(repository.join("nested"))?;
    fs::write(repository.join("nested/model.bin"), b"model-v1")?;
    fs::write(repository.join("empty.bin"), [])?;
    run_success(&repository, &["add", "."])?;
    run_success(&repository, &["commit", "-m", "snapshot"])?;

    let head_before = fs::read(repository.join(".neoengram/metadata/HEAD"))?;
    let index_before = fs::read(repository.join(".neoengram/metadata/index.json"))?;
    let worktree_before = fs::read(repository.join("nested/model.bin"))?;
    // macOS exposes `/var` as a symlink to `/private/var`; pass a canonical parent because the
    // command intentionally rejects symlinked destination ancestors.
    let destination = fs::canonicalize(outer.path())?.join("readonly");
    let destination_arg = destination.to_string_lossy().into_owned();

    let output = run_success(
        &repository,
        &["checkout", "HEAD", "--read-only", destination_arg.as_str()],
    )?;
    assert!(String::from_utf8(output.stdout)?.contains("Materialized read-only snapshot"));
    assert_eq!(fs::read(destination.join("nested/model.bin"))?, b"model-v1");
    assert_eq!(fs::metadata(destination.join("empty.bin"))?.len(), 0);
    assert_eq!(
        fs::read(repository.join(".neoengram/metadata/HEAD"))?,
        head_before
    );
    assert_eq!(
        fs::read(repository.join(".neoengram/metadata/index.json"))?,
        index_before
    );
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

    let existing = run_command(
        &repository,
        &["checkout", "HEAD", "--read-only", destination_arg.as_str()],
    )?;
    assert!(!existing.status.success());
    assert!(String::from_utf8_lossy(&existing.stderr).contains("已存在"));
    Ok(())
}

#[test]
fn checkout_read_only_rejects_destinations_inside_the_worktree() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init"])?;
    let output = run_command(&repository, &["checkout", "--read-only", "snapshot"])?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("不能位于当前 NeoEngram 工作区"));
    assert!(!repository.join("snapshot").exists());
    Ok(())
}

#[test]
fn checkout_read_only_supports_main_and_explicit_commits_without_touching_head() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init", "--metadata-store", "json"])?;

    let model = repository.join("model.bin");
    fs::write(&model, b"v1")?;
    let first = commit(&repository, "first")?;
    fs::write(&model, b"v2")?;
    let second = commit(&repository, "second")?;
    let head_before = fs::read(repository.join(".neoengram/metadata/HEAD"))?;

    let first_destination = fs::canonicalize(outer.path())?.join("first-snapshot");
    run_success(
        &repository,
        &[
            "checkout",
            first.as_str(),
            "--read-only",
            first_destination.to_str().unwrap(),
        ],
    )?;
    assert_eq!(fs::read(first_destination.join("model.bin"))?, b"v1");

    let main_destination = fs::canonicalize(outer.path())?.join("main-snapshot");
    run_success(
        &repository,
        &[
            "checkout",
            "main",
            "--read-only",
            main_destination.to_str().unwrap(),
        ],
    )?;
    assert_eq!(fs::read(main_destination.join("model.bin"))?, b"v2");
    assert_eq!(first.len(), 64);
    assert_eq!(second.len(), 64);
    assert_eq!(
        fs::read(repository.join(".neoengram/metadata/HEAD"))?,
        head_before
    );
    Ok(())
}

#[test]
fn checkout_read_only_does_not_publish_a_partial_directory_on_corrupt_chunk() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init", "--metadata-store", "json"])?;
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
        &[
            "checkout",
            "HEAD",
            "--read-only",
            destination.to_str().unwrap(),
        ],
    )?;
    assert!(!output.status.success());
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn checkout_read_only_works_with_the_default_sqlite_backend() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init"])?;
    fs::write(repository.join("model.bin"), b"sqlite-payload")?;
    commit(&repository, "sqlite")?;

    let destination = fs::canonicalize(outer.path())?.join("sqlite-snapshot");
    run_success(
        &repository,
        &[
            "checkout",
            "HEAD",
            "--read-only",
            destination.to_str().unwrap(),
        ],
    )?;
    assert_eq!(fs::read(destination.join("model.bin"))?, b"sqlite-payload");
    Ok(())
}

#[test]
fn checkout_read_only_drops_staging_when_a_chunk_is_corrupt() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init", "--metadata-store", "json"])?;
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
    let output = run_command(
        &repository,
        &["checkout", "HEAD", "--read-only", destination_arg.as_str()],
    )?;
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
