use std::{
    error::Error,
    fs,
    path::Path,
    process::{Command, Output},
};

use neoengram_core::Commit;
use serde::Deserialize;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn add_all_stages_and_commits_deletions_with_path_scoping() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::create_dir(repository.path().join("dataset"))?;
    fs::write(repository.path().join("dataset/remove.bin"), b"remove")?;
    fs::write(repository.path().join("dataset/keep.bin"), b"keep")?;
    fs::write(repository.path().join("outside.bin"), b"outside")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    fs::remove_file(repository.path().join("dataset/remove.bin"))?;
    fs::remove_file(repository.path().join("outside.bin"))?;
    run_success(repository.path(), &["add", "-A", "dataset"])?;

    let index = read_index(repository.path())?;
    let paths: Vec<&str> = index.files.iter().map(|file| file.path.as_str()).collect();
    assert_eq!(paths, ["dataset/keep.bin", "outside.bin"]);
    let status = run_success(repository.path(), &["status"])?;
    let status = String::from_utf8(status.stdout)?;
    assert!(status.contains("deleted: dataset/remove.bin"));
    assert!(status.contains("deleted: outside.bin"));
    assert!(status.contains("Changes not staged for commit:"));

    run_success(repository.path(), &["add", "-A", "."])?;
    run_success(repository.path(), &["commit", "-m", "delete files"])?;
    let tree = head_tree(repository.path())?;
    let paths: Vec<&str> = tree.files.iter().map(|file| file.path.as_str()).collect();
    assert_eq!(paths, ["dataset/keep.bin"]);
    Ok(())
}

#[test]
fn rm_protects_dirty_files_and_cached_mode_keeps_the_worktree() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let path = repository.path().join("weights.pt");
    fs::write(&path, b"committed")?;
    run_success(repository.path(), &["add", "weights.pt"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    fs::write(&path, b"local edit")?;
    let refused = command(repository.path())
        .args(["rm", "weights.pt"])
        .output()?;
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("未暂存修改"));
    assert_eq!(fs::read(&path)?, b"local edit");
    assert_eq!(read_index(repository.path())?.files.len(), 1);

    fs::write(&path, b"committed")?;
    run_success(repository.path(), &["rm", "--cached", "weights.pt"])?;
    assert_eq!(fs::read(&path)?, b"committed");
    assert!(read_index(repository.path())?.files.is_empty());
    let status = run_success(repository.path(), &["status"])?;
    let status = String::from_utf8(status.stdout)?;
    assert!(status.contains("deleted: weights.pt"));
    assert!(status.contains("Untracked files:"));

    run_success(repository.path(), &["add", "weights.pt"])?;
    run_success(repository.path(), &["rm", "weights.pt"])?;
    assert!(!path.exists());
    assert!(read_index(repository.path())?.files.is_empty());
    Ok(())
}

#[test]
fn status_separates_staged_unstaged_and_untracked_changes() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    fs::write(repository.path().join("a.bin"), b"a1")?;
    fs::write(repository.path().join("b.bin"), b"b1")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    fs::write(repository.path().join("a.bin"), b"a2")?;
    run_success(repository.path(), &["add", "a.bin"])?;
    fs::remove_file(repository.path().join("b.bin"))?;
    fs::write(repository.path().join("new.bin"), b"new")?;

    let status = run_success(repository.path(), &["status"])?;
    let status = String::from_utf8(status.stdout)?;
    assert!(status.contains("Changes to be committed:"));
    assert!(status.contains("modified: a.bin"));
    assert!(status.contains("Changes not staged for commit:"));
    assert!(status.contains("deleted: b.bin"));
    assert!(status.contains("Untracked files:"));
    assert!(status.contains("new.bin"));

    run_success(repository.path(), &["add", "-A", "."])?;
    let staged = String::from_utf8(run_success(repository.path(), &["status"])?.stdout)?;
    assert!(staged.contains("modified: a.bin"));
    assert!(staged.contains("deleted: b.bin"));
    assert!(staged.contains("added: new.bin"));
    assert!(!staged.contains("Changes not staged for commit:"));
    assert!(!staged.contains("Untracked files:"));
    Ok(())
}

#[test]
fn checkout_force_handles_file_directory_transitions() -> TestResult {
    let repository = tempfile::tempdir()?;
    initialize(repository.path())?;
    let node = repository.path().join("node");
    fs::write(&node, b"file version")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "file"])?;
    let file_commit = current_commit_id(repository.path())?;

    fs::remove_file(&node)?;
    fs::create_dir(&node)?;
    fs::write(node.join("child.bin"), b"directory version")?;
    run_success(repository.path(), &["add", "-A", "."])?;
    run_success(repository.path(), &["commit", "-m", "directory"])?;
    let directory_commit = current_commit_id(repository.path())?;

    let refused = command(repository.path())
        .args(["checkout", file_commit.as_str()])
        .output()?;
    assert!(!refused.status.success());
    assert!(node.is_dir());
    assert_eq!(fs::read(node.join("child.bin"))?, b"directory version");

    run_success(
        repository.path(),
        &["checkout", file_commit.as_str(), "--force"],
    )?;
    assert!(node.is_file());
    assert_eq!(fs::read(&node)?, b"file version");

    run_success(
        repository.path(),
        &["checkout", directory_commit.as_str(), "--force"],
    )?;
    assert!(node.is_dir());
    assert_eq!(fs::read(node.join("child.bin"))?, b"directory version");
    assert_eq!(current_commit_id(repository.path())?, directory_commit);
    Ok(())
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

#[derive(Deserialize)]
struct StoredFileSet {
    files: Vec<StoredFileRecord>,
}

#[derive(Deserialize)]
struct StoredFileRecord {
    path: String,
}

fn read_index(repository: &Path) -> Result<StoredFileSet, Box<dyn Error>> {
    Ok(serde_json::from_slice(&fs::read(
        repository.join(".neoengram/metadata/index.json"),
    )?)?)
}

fn head_tree(repository: &Path) -> Result<StoredFileSet, Box<dyn Error>> {
    let id = current_commit_id(repository)?;
    let commit: Commit = serde_json::from_slice(&fs::read(
        repository.join(format!(".neoengram/metadata/commits/{id}.json")),
    )?)?;
    Ok(serde_json::from_slice(&fs::read(repository.join(
        format!(".neoengram/metadata/trees/{}.json", commit.tree_hash),
    ))?)?)
}

fn current_commit_id(repository: &Path) -> Result<String, Box<dyn Error>> {
    Ok(
        fs::read_to_string(repository.join(".neoengram/metadata/refs/heads/main"))?
            .trim()
            .to_owned(),
    )
}
