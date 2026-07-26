use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use rusqlite::{params, Connection};

mod support;

use support::models::{Chunk, ChunkingStrategy, FileNode, Index, INDEX_FORMAT_VERSION};

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
    assert!(!current_ref_exists(repository.path())?);
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
    assert!(!current_ref_exists(repository.path())?);
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
fn fsck_rejects_a_missing_referenced_object() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(
        repository.path().join("dataset.bin"),
        b"missing fsck payload",
    )?;
    run_success(repository.path(), &["add", "dataset.bin"])?;
    run_success(repository.path(), &["commit", "-m", "missing object"])?;

    let chunk = first_indexed_chunk(repository.path())?;
    fs::remove_file(object_path(repository.path(), &chunk.hash))?;

    let fsck = command(repository.path()).arg("fsck").output()?;
    assert!(!fsck.status.success());
    let stderr = String::from_utf8_lossy(&fsck.stderr);
    assert!(stderr.contains("元数据引用了不存在的 Chunk 对象"));
    assert!(stderr.contains(&chunk.hash));
    Ok(())
}

#[test]
fn fsck_accepts_a_valid_detached_head() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("dataset.bin"), b"detached payload")?;
    run_success(repository.path(), &["add", "dataset.bin"])?;
    run_success(repository.path(), &["commit", "-m", "detached target"])?;
    let commit_id = current_commit_id(repository.path())?;

    run_success(repository.path(), &["checkout", commit_id.as_str()])?;
    assert_eq!(
        read_head(repository.path())?,
        ("detached".to_owned(), commit_id)
    );

    let fsck = command(repository.path()).arg("fsck").output()?;
    assert_success(&fsck);
    assert!(String::from_utf8_lossy(&fsck.stdout).contains("Fsck OK"));
    Ok(())
}

#[test]
fn fsck_rejects_a_dangling_detached_head() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("dataset.bin"), b"valid payload")?;
    run_success(repository.path(), &["add", "dataset.bin"])?;
    run_success(repository.path(), &["commit", "-m", "valid snapshot"])?;

    let dangling = "f".repeat(64);
    let connection = metadata_connection(repository.path())?;
    connection.execute(
        "UPDATE workspaces SET head_kind = 'detached', head_target = ?1, base_commit_id = ?1 \
         WHERE id = zeroblob(16)",
        params![dangling],
    )?;
    let fsck = command(repository.path()).arg("fsck").output()?;
    assert!(!fsck.status.success());
    let stderr = String::from_utf8_lossy(&fsck.stderr);
    assert!(stderr.contains("HEAD"));
    assert!(stderr.contains(&dangling));
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

    assert!(current_ref_exists(repository.path())?);
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
    let output = command(path).arg("init").output()?;
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
    let connection = metadata_connection(repository)?;
    let mut statement = connection.prepare(
        "SELECT path, total_size, chunk_count, lower(hex(manifest_id)) \
         FROM workspace_index_files WHERE workspace_id = zeroblob(16) ORDER BY path",
    )?;
    let records = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut files = Vec::with_capacity(records.len());
    for (path, total_size, chunk_count, manifest_id) in records {
        let manifest_id = blake3::Hash::from_hex(&manifest_id)?;
        let set_id: Vec<u8> = connection.query_row(
            "SELECT set_id FROM manifests WHERE id = ?1",
            params![manifest_id.as_bytes().as_slice()],
            |row| row.get(0),
        )?;
        let mut chunks = connection.prepare(
            "SELECT lower(hex(object_id)), offset, size FROM manifest_chunks \
             WHERE set_id = ?1 ORDER BY ordinal",
        )?;
        let chunks = chunks
            .query_map(params![set_id], |row| {
                Ok(Chunk {
                    hash: row.get(0)?,
                    offset: u64::try_from(row.get::<_, i64>(1)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Integer,
                            error.into(),
                        )
                    })?,
                    size: u64::try_from(row.get::<_, i64>(2)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Integer,
                            error.into(),
                        )
                    })?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(u64::try_from(chunks.len())?, u64::try_from(chunk_count)?);
        files.push(FileNode {
            path,
            total_size: u64::try_from(total_size)?,
            chunking: ChunkingStrategy::FastCdc,
            chunks,
        });
    }
    Ok(Index {
        format_version: INDEX_FORMAT_VERSION,
        files,
    })
}

fn object_path(repository: &Path, hash: &str) -> PathBuf {
    repository.join(".neoengram/objects").join(hash)
}

fn metadata_connection(repository: &Path) -> Result<Connection, Box<dyn Error>> {
    Ok(Connection::open(
        repository.join(".neoengram/metadata/metadata.sqlite3"),
    )?)
}

fn current_ref_exists(repository: &Path) -> Result<bool, Box<dyn Error>> {
    let connection = metadata_connection(repository)?;
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM refs WHERE name = 'refs/heads/main')",
        [],
        |row| row.get(0),
    )?)
}

fn current_commit_id(repository: &Path) -> Result<String, Box<dyn Error>> {
    let connection = metadata_connection(repository)?;
    Ok(connection.query_row(
        "SELECT target FROM refs WHERE name = 'refs/heads/main'",
        [],
        |row| row.get(0),
    )?)
}

fn read_head(repository: &Path) -> Result<(String, String), Box<dyn Error>> {
    let connection = metadata_connection(repository)?;
    Ok(connection.query_row(
        "SELECT head_kind, head_target FROM workspaces WHERE id = zeroblob(16)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}
