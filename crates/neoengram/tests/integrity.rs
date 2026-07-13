use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use neoengram_core::{Chunk, FileNode, Index, INDEX_FORMAT_VERSION};
use serde::Deserialize;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn commit_rejects_a_missing_staged_object_without_publishing_ref() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(
        repository.path().join("model.bin"),
        b"missing object payload",
    )?;
    run_success(repository.path(), &["add", "model.bin"])?;

    let chunk = first_indexed_chunk(repository.path())?;
    let object = object_path(repository.path(), &chunk.hash);
    fs::remove_file(&object)?;

    let commit = command(repository.path())
        .args(["commit", "-m", "must not publish"])
        .output()?;
    assert!(!commit.status.success());
    assert!(String::from_utf8_lossy(&commit.stderr).contains(&chunk.hash));
    assert!(!current_ref(repository.path()).exists());
    Ok(())
}

#[test]
fn commit_rejects_same_size_corruption_without_publishing_ref() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(
        repository.path().join("weights.pt"),
        b"original weights payload",
    )?;
    run_success(repository.path(), &["add", "weights.pt"])?;

    let chunk = first_indexed_chunk(repository.path())?;
    let corrupt = vec![0xa5; usize::try_from(chunk.size)?];
    assert_ne!(blake3::hash(&corrupt).to_hex().as_str(), chunk.hash);
    fs::write(object_path(repository.path(), &chunk.hash), corrupt)?;

    let commit = command(repository.path())
        .args(["commit", "-m", "must not publish"])
        .output()?;
    assert!(!commit.status.success());
    let stderr = String::from_utf8_lossy(&commit.stderr);
    assert!(stderr.contains(&chunk.hash));
    assert!(stderr.contains("Hash"));
    assert!(!current_ref(repository.path()).exists());
    Ok(())
}

#[test]
fn fsck_accepts_a_valid_repository_and_reports_object_corruption() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("dataset.bin"), b"fsck payload")?;
    run_success(repository.path(), &["add", "dataset.bin"])?;
    run_success(repository.path(), &["commit", "-m", "valid snapshot"])?;

    let healthy = command(repository.path()).arg("fsck").output()?;
    assert_success(&healthy);
    assert!(String::from_utf8_lossy(&healthy.stdout).contains("Fsck OK"));

    let chunk = first_indexed_chunk(repository.path())?;
    let corrupt = vec![0x3c; usize::try_from(chunk.size)?];
    assert_ne!(blake3::hash(&corrupt).to_hex().as_str(), chunk.hash);
    fs::write(object_path(repository.path(), &chunk.hash), corrupt)?;

    let damaged = command(repository.path()).arg("fsck").output()?;
    assert!(!damaged.status.success());
    let stderr = String::from_utf8_lossy(&damaged.stderr);
    assert!(stderr.contains(&chunk.hash));
    assert!(stderr.contains("Hash"));
    Ok(())
}

#[test]
fn log_is_newest_first_honors_max_count_and_show_lists_files() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("a.pt"), b"first")?;
    run_success(repository.path(), &["add", "a.pt"])?;
    run_success(repository.path(), &["commit", "-m", "first snapshot"])?;
    let first_id = current_commit_id(repository.path())?;

    fs::create_dir(repository.path().join("nested"))?;
    fs::write(repository.path().join("nested/b.pt"), b"second")?;
    run_success(repository.path(), &["add", "nested/b.pt"])?;
    run_success(repository.path(), &["commit", "-m", "second snapshot"])?;
    let second_id = current_commit_id(repository.path())?;
    assert_ne!(first_id, second_id);

    let log = command(repository.path()).arg("log").output()?;
    assert_success(&log);
    let log = String::from_utf8(log.stdout)?;
    let second_position = log
        .find(&second_id)
        .ok_or("latest commit missing from log")?;
    let first_position = log.find(&first_id).ok_or("root commit missing from log")?;
    assert!(second_position < first_position);
    assert!(log.contains("first snapshot"));
    assert!(log.contains("second snapshot"));

    let limited = command(repository.path())
        .args(["log", "--max-count", "1"])
        .output()?;
    assert_success(&limited);
    let limited = String::from_utf8(limited.stdout)?;
    assert!(limited.contains(&second_id));
    assert!(limited.contains("second snapshot"));
    assert!(!limited.contains(&first_id));
    assert!(!limited.contains("first snapshot"));

    let head = command(repository.path()).args(["show", "HEAD"]).output()?;
    assert_success(&head);
    let head = String::from_utf8(head.stdout)?;
    assert!(head.contains(&second_id));
    assert!(head.contains("a.pt"));
    assert!(head.contains("nested/b.pt"));

    let first = command(repository.path())
        .args(["show", first_id.as_str()])
        .output()?;
    assert_success(&first);
    let first = String::from_utf8(first.stdout)?;
    assert!(first.contains(&first_id));
    assert!(first.contains("a.pt"));
    assert!(!first.contains("nested/b.pt"));
    Ok(())
}

#[test]
fn an_unlocked_stale_lock_file_does_not_block_writers() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let lock = repository.path().join(".neoengram/metadata/write.lock");
    fs::write(&lock, b"stale lock metadata from a terminated process\n")?;

    fs::write(repository.path().join("checkpoint.pt"), b"checkpoint")?;
    run_success(repository.path(), &["add", "checkpoint.pt"])?;
    run_success(repository.path(), &["commit", "-m", "stale lock recovered"])?;

    assert!(current_ref(repository.path()).is_file());
    Ok(())
}

#[cfg(unix)]
#[test]
fn a_symlinked_lock_file_is_rejected_without_touching_its_target() -> TestResult {
    use std::os::unix::fs::symlink;

    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let outside = tempfile::NamedTempFile::new()?;
    fs::write(outside.path(), b"outside sentinel")?;
    symlink(
        outside.path(),
        repository.path().join(".neoengram/metadata/write.lock"),
    )?;
    fs::write(repository.path().join("model.bin"), b"model")?;

    let add = command(repository.path())
        .args(["add", "model.bin"])
        .output()?;
    assert!(!add.status.success());
    assert!(String::from_utf8_lossy(&add.stderr).contains("写锁路径不是普通文件"));
    assert_eq!(fs::read(outside.path())?, b"outside sentinel");
    assert!(read_index(repository.path())?.files.is_empty());
    Ok(())
}

fn initialize(path: &Path) -> TestResult {
    let output = command(path)
        .args(["init", "--metadata-store", "json"])
        .output()?;
    assert_success(&output);
    Ok(())
}

fn run_success(path: &Path, arguments: &[&str]) -> TestResult {
    let output = command(path).args(arguments).output()?;
    assert_success(&output);
    Ok(())
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn command(current_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_neoengram"));
    command.current_dir(current_dir);
    command
}

fn first_indexed_chunk(repository: &Path) -> Result<Chunk, Box<dyn Error>> {
    let index = read_index(repository)?;
    let file = index.files.first().ok_or("index contains no files")?;
    file.chunks
        .first()
        .cloned()
        .ok_or_else(|| "indexed file contains no chunks".into())
}

fn read_index(repository: &Path) -> Result<Index, Box<dyn Error>> {
    let metadata_dir = repository.join(".neoengram/metadata");
    let stored: StoredIndex = serde_json::from_slice(&fs::read(metadata_dir.join("index.json"))?)?;
    let mut files = Vec::with_capacity(stored.files.len());
    for record in stored.files {
        let manifest: StoredManifest = serde_json::from_slice(&fs::read(
            metadata_dir.join(format!("manifests/{}.json", record.manifest_id)),
        )?)?;
        assert_eq!(manifest.total_size, record.total_size);
        assert_eq!(u64::try_from(manifest.chunks.len())?, record.chunk_count);
        files.push(FileNode {
            path: record.path,
            total_size: record.total_size,
            chunks: manifest.chunks,
        });
    }
    Ok(Index {
        format_version: INDEX_FORMAT_VERSION,
        files,
    })
}

#[derive(Deserialize)]
struct StoredIndex {
    files: Vec<StoredFileRecord>,
}

#[derive(Deserialize)]
struct StoredFileRecord {
    path: String,
    total_size: u64,
    chunk_count: u64,
    manifest_id: String,
}

#[derive(Deserialize)]
struct StoredManifest {
    total_size: u64,
    chunks: Vec<Chunk>,
}

fn object_path(repository: &Path, hash: &str) -> PathBuf {
    repository.join(".neoengram/objects").join(hash)
}

fn current_ref(repository: &Path) -> PathBuf {
    repository.join(".neoengram/metadata/refs/heads/main")
}

fn current_commit_id(repository: &Path) -> Result<String, Box<dyn Error>> {
    Ok(fs::read_to_string(current_ref(repository))?
        .trim()
        .to_owned())
}
