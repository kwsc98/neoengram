#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
};

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

#[test]
fn hardlink_export_shares_the_verified_whole_file_inode_and_detects_corruption() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init", "--chunking", "whole-file"])?;
    let contents = b"immutable whole-file model";
    fs::write(repository.join("model.bin"), contents)?;
    fs::write(repository.join("empty.bin"), [])?;
    run_success(&repository, &["add", "."])?;
    run_success(&repository, &["commit", "-m", "whole-file"])?;

    let object = loose_object(&repository)?;
    let copy_destination = fs::canonicalize(outer.path())?.join("copied");
    run_success(
        &repository,
        &[
            "export",
            "--mode",
            "copy",
            "HEAD",
            copy_destination.to_str().unwrap(),
        ],
    )?;
    let copied_metadata = fs::metadata(copy_destination.join("model.bin"))?;
    let object_before_link = fs::metadata(&object)?;
    assert!(
        copied_metadata.dev() != object_before_link.dev()
            || copied_metadata.ino() != object_before_link.ino()
    );

    let destination = fs::canonicalize(outer.path())?.join("linked");
    let output = run_success(
        &repository,
        &[
            "export",
            "--mode",
            "hardlink",
            "HEAD",
            destination.to_str().unwrap(),
        ],
    )?;
    assert!(String::from_utf8(output.stdout)?.contains("hard-linked read-only view"));
    assert!(String::from_utf8(output.stderr)?.contains("share content and permissions"));

    let object_metadata = fs::metadata(&object)?;
    let linked = destination.join("model.bin");
    let linked_metadata = fs::metadata(&linked)?;
    assert_eq!(object_metadata.dev(), linked_metadata.dev());
    assert_eq!(object_metadata.ino(), linked_metadata.ino());
    assert_eq!(object_metadata.permissions().mode() & 0o222, 0);
    assert_eq!(linked_metadata.permissions().mode() & 0o222, 0);
    assert_eq!(fs::metadata(destination.join("empty.bin"))?.len(), 0);
    run_success(&repository, &["fsck"])?;

    fs::set_permissions(&linked, fs::Permissions::from_mode(0o600))?;
    fs::write(&linked, vec![b'x'; contents.len()])?;
    let fsck = run_command(&repository, &["fsck"])?;
    assert!(!fsck.status.success());
    assert!(String::from_utf8_lossy(&fsck.stderr).contains("Hash 损坏"));

    let second = fs::canonicalize(outer.path())?.join("linked-again");
    let export = run_command(
        &repository,
        &[
            "export",
            "--mode",
            "hardlink",
            "HEAD",
            second.to_str().unwrap(),
        ],
    )?;
    assert!(!export.status.success());
    assert!(!second.exists());

    fs::set_permissions(&destination, fs::Permissions::from_mode(0o700))?;
    fs::remove_file(linked)?;
    fs::remove_file(destination.join("empty.bin"))?;
    fs::remove_dir(destination)?;
    Ok(())
}

#[test]
fn hardlink_failure_cleanup_preserves_existing_object_permissions() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init", "--chunking", "whole-file"])?;
    let first_contents = b"first whole-file object";
    let second_contents = b"second whole-file object";
    fs::write(repository.join("a.bin"), first_contents)?;
    fs::write(repository.join("b.bin"), second_contents)?;
    run_success(&repository, &["add", "."])?;
    run_success(&repository, &["commit", "-m", "cleanup"])?;

    let first_object = indexed_object_path(&repository, "a.bin")?;
    let second_object = indexed_object_path(&repository, "b.bin")?;
    fs::set_permissions(&first_object, fs::Permissions::from_mode(0o400))?;
    fs::write(&second_object, vec![b'x'; second_contents.len()])?;

    let destination = fs::canonicalize(outer.path())?.join("failed-linked");
    let output = run_command(
        &repository,
        &[
            "export",
            "--mode",
            "hardlink",
            "HEAD",
            destination.to_str().unwrap(),
        ],
    )?;
    assert!(!output.status.success());
    assert!(!destination.exists());
    assert_eq!(fs::read(&first_object)?, first_contents);
    assert_eq!(
        fs::metadata(&first_object)?.permissions().mode() & 0o777,
        0o400
    );
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
fn hardlink_export_rejects_mixed_chunking_without_publishing() -> TestResult {
    let outer = tempfile::tempdir()?;
    let repository = outer.path().join("repository");
    fs::create_dir(&repository)?;
    run_success(&repository, &["init", "--chunking", "mixed"])?;
    fs::write(repository.join("cdc.bin"), b"cdc")?;
    fs::write(repository.join("whole.bin"), b"whole")?;
    run_success(&repository, &["add", "cdc.bin"])?;
    run_success(
        &repository,
        &["add", "--chunking", "whole-file", "whole.bin"],
    )?;
    run_success(&repository, &["commit", "-m", "mixed"])?;

    let destination = fs::canonicalize(outer.path())?.join("mixed-linked");
    let output = run_command(
        &repository,
        &[
            "export",
            "--mode",
            "hardlink",
            "HEAD",
            destination.to_str().unwrap(),
        ],
    )?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("仅支持 WholeFile"));
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
fn explicit_strategy_switch_changes_manifest_and_reports_reused_object() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init", "--chunking", "mixed"])?;
    fs::write(repository.path().join("model.bin"), b"same payload")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "fastcdc"])?;
    let (fastcdc_manifest, fastcdc_strategy) = indexed_manifest(repository.path(), "model.bin")?;
    assert_eq!(fastcdc_strategy, 1);

    run_success(
        repository.path(),
        &["add", "--chunking", "whole-file", "model.bin"],
    )?;
    let (whole_file_manifest, whole_file_strategy) =
        indexed_manifest(repository.path(), "model.bin")?;
    assert_eq!(whole_file_strategy, 2);
    assert_ne!(fastcdc_manifest, whole_file_manifest);

    let status = String::from_utf8(run_success(repository.path(), &["status"])?.stdout)?;
    assert!(status.contains("Changes to be committed:"));
    assert!(status.contains("modified: model.bin"));
    assert!(!status.contains("Changes not staged for commit:"));

    let staged = String::from_utf8(run_success(repository.path(), &["diff", "--staged"])?.stdout)?;
    assert!(staged.contains("chunks reused 1, new 0, removed 0, +0 bytes"));
    let worktree = String::from_utf8(run_success(repository.path(), &["diff"])?.stdout)?;
    assert!(worktree.contains("no differences"));
    Ok(())
}

#[test]
fn add_without_override_preserves_the_tracked_whole_file_strategy() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init", "--chunking", "mixed"])?;
    let model = repository.path().join("model.bin");
    fs::write(&model, b"whole-v1")?;
    run_success(
        repository.path(),
        &["add", "--chunking", "whole-file", "model.bin"],
    )?;
    assert_eq!(indexed_chunking(repository.path(), "model.bin")?, 2);

    fs::write(&model, b"whole-v2-with-new-size")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    assert_eq!(indexed_chunking(repository.path(), "model.bin")?, 2);
    Ok(())
}

fn loose_object(repository: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(fs::read_dir(repository.join(".neoengram/objects"))?
        .filter_map(Result::ok)
        .find(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.len() == 64)
        })
        .ok_or("missing loose object")?
        .path())
}

fn indexed_chunking(repository: &Path, path: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(indexed_manifest(repository, path)?.1)
}

fn indexed_manifest(
    repository: &Path,
    path: &str,
) -> Result<(String, i64), Box<dyn std::error::Error>> {
    let connection = Connection::open(repository.join(".neoengram/metadata/metadata.sqlite3"))?;
    Ok(connection.query_row(
        "SELECT lower(hex(manifests.id)), manifests.chunking FROM workspace_index_files \
         JOIN manifests ON manifests.id = workspace_index_files.manifest_id \
         WHERE workspace_index_files.workspace_id = zeroblob(16) \
           AND workspace_index_files.path = ?1",
        [path],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}

fn indexed_object_path(
    repository: &Path,
    path: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let connection = Connection::open(repository.join(".neoengram/metadata/metadata.sqlite3"))?;
    let id: String = connection.query_row(
        "SELECT lower(hex(manifest_chunks.object_id)) FROM workspace_index_files \
         JOIN manifests ON manifests.id = workspace_index_files.manifest_id \
         JOIN manifest_chunks ON manifest_chunks.set_id = manifests.set_id \
         WHERE workspace_index_files.workspace_id = zeroblob(16) \
           AND workspace_index_files.path = ?1 AND manifest_chunks.ordinal = 0",
        [path],
        |row| row.get(0),
    )?;
    Ok(repository.join(".neoengram/objects").join(id))
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
