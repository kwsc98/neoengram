use std::{
    error::Error,
    fs,
    path::Path,
    process::{Command, Output},
};

use rusqlite::Connection;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn managed_workspaces_share_objects_but_isolate_head_index_and_files() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init"])?;
    fs::write(repository.path().join("base.txt"), b"base")?;
    run_success(repository.path(), &["add", "base.txt"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    run_success(
        repository.path(),
        &["workspace", "create", "experiment", "--from", "HEAD"],
    )?;
    let experiment = repository.path().join("workspaces/experiment");
    assert_eq!(fs::read(experiment.join("base.txt"))?, b"base");
    assert!(experiment.join(".neoengram/locks").is_dir());
    assert!(experiment.join(".neoengram/transactions").is_dir());
    let occupied = command(&experiment).args(["checkout", "main"]).output()?;
    assert!(!occupied.status.success());
    assert!(String::from_utf8_lossy(&occupied.stderr).contains("已被 Workspace main 占用"));

    fs::write(experiment.join("base.txt"), b"experiment")?;
    fs::write(experiment.join("only.txt"), b"only")?;
    run_success(&experiment, &["add", "."])?;
    run_success(&experiment, &["commit", "-m", "experiment"])?;

    let main_status = String::from_utf8(run_success(repository.path(), &["status"])?.stdout)?;
    assert!(main_status.contains("working tree clean"));
    assert!(!main_status.contains("workspaces/experiment"));
    let experiment_status = String::from_utf8(run_success(&experiment, &["status"])?.stdout)?;
    assert!(experiment_status.contains("HEAD detached at"));
    assert!(experiment_status.contains("working tree clean"));

    let listed = String::from_utf8(run_success(&experiment, &["workspace", "list"])?.stdout)?;
    assert!(listed.contains("* experiment"));
    assert!(listed.contains("  main"));

    let connection = Connection::open(
        repository
            .path()
            .join(".neoengram/metadata/metadata.sqlite3"),
    )?;
    let workspace_count: i64 =
        connection.query_row("SELECT COUNT(*) FROM workspaces", [], |row| row.get(0))?;
    let scoped_indexes: i64 = connection.query_row(
        "SELECT COUNT(DISTINCT workspace_id) FROM workspace_index_files",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(workspace_count, 2);
    assert_eq!(scoped_indexes, 2);
    Ok(())
}

#[test]
fn external_workspace_discovers_its_repository_through_the_link() -> TestResult {
    let repository = tempfile::tempdir()?;
    let external_parent = tempfile::tempdir()?;
    let external = external_parent.path().join("external");
    run_success(repository.path(), &["init"])?;
    fs::write(repository.path().join("base.txt"), b"base")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    let external_text = external.to_str().ok_or("external path is not UTF-8")?;
    run_success(
        repository.path(),
        &[
            "workspace",
            "create",
            "external",
            "--from",
            "HEAD",
            "--path",
            external_text,
        ],
    )?;
    assert!(external.join(".neoengram/workspace.json").is_file());
    let status = String::from_utf8(run_success(&external, &["status"])?.stdout)?;
    assert!(status.contains("HEAD detached at"));
    assert!(status.contains("working tree clean"));
    Ok(())
}

#[test]
fn internal_workspace_paths_are_confined_to_the_managed_directory() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init"])?;
    fs::write(repository.path().join("base.txt"), b"base")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    for (name, relative) in [
        ("arbitrary", "custom"),
        ("export-slot", "exports/workspace"),
        ("mount-slot", "mounts/workspace"),
    ] {
        let destination = repository.path().join(relative);
        let output = command(repository.path())
            .args(["workspace", "create", name, "--from", "HEAD", "--path"])
            .arg(&destination)
            .output()?;
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("只能位于受管目录"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!destination.exists());
    }

    let managed = repository.path().join("workspaces/custom-location");
    run_success(
        repository.path(),
        &[
            "workspace",
            "create",
            "managed-custom",
            "--from",
            "HEAD",
            "--path",
            managed.to_str().ok_or("managed path is not UTF-8")?,
        ],
    )?;
    run_success(repository.path(), &["add", "."])?;
    let status = String::from_utf8(run_success(repository.path(), &["status"])?.stdout)?;
    assert!(status.contains("working tree clean"));

    let connection = Connection::open(
        repository
            .path()
            .join(".neoengram/metadata/metadata.sqlite3"),
    )?;
    let names = connection
        .prepare("SELECT name FROM workspaces ORDER BY name")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(names, vec!["main", "managed-custom"]);
    Ok(())
}

#[test]
fn workspace_can_own_and_advance_a_new_branch() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init"])?;
    fs::write(repository.path().join("base.txt"), b"base")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    run_success(
        repository.path(),
        &[
            "workspace",
            "create",
            "feature",
            "--from",
            "HEAD",
            "--branch",
            "feature/data",
        ],
    )?;
    let feature = repository.path().join("workspaces/feature");
    let status = String::from_utf8(run_success(&feature, &["status"])?.stdout)?;
    assert!(status.contains("On branch feature/data"));
    fs::write(feature.join("feature.txt"), b"feature")?;
    run_success(&feature, &["add", "feature.txt"])?;
    run_success(&feature, &["commit", "-m", "feature"])?;

    let connection = Connection::open(
        repository
            .path()
            .join(".neoengram/metadata/metadata.sqlite3"),
    )?;
    let reference: String = connection.query_row(
        "SELECT target FROM refs WHERE name = 'refs/heads/feature/data'",
        [],
        |row| row.get(0),
    )?;
    let base: String = connection.query_row(
        "SELECT base_commit_id FROM workspaces WHERE name = 'feature'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(reference, base);
    Ok(())
}

#[test]
fn failed_workspace_materialization_does_not_publish_a_branch() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init"])?;
    fs::write(repository.path().join("base.txt"), b"base payload")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    let objects = fs::read_dir(repository.path().join(".neoengram/objects"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    let object = objects
        .into_iter()
        .find(|path| path.is_file())
        .ok_or("fixture did not create an object")?;
    fs::write(object, [])?;

    let output = command(repository.path())
        .args([
            "workspace",
            "create",
            "broken",
            "--from",
            "HEAD",
            "--branch",
            "broken",
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("无法物化目标 Commit"));
    assert!(!repository.path().join("workspaces/broken").exists());

    let connection = Connection::open(
        repository
            .path()
            .join(".neoengram/metadata/metadata.sqlite3"),
    )?;
    let workspace_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM workspaces WHERE name = 'broken'",
        [],
        |row| row.get(0),
    )?;
    let reference_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM refs WHERE name = 'refs/heads/broken'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!((workspace_count, reference_count), (0, 0));
    Ok(())
}

#[test]
fn workspace_remove_protects_dirty_files_and_force_removes_atomically() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init"])?;
    fs::write(repository.path().join("base.txt"), b"base")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;
    run_success(
        repository.path(),
        &["workspace", "create", "scratch", "--from", "HEAD"],
    )?;
    let scratch = repository.path().join("workspaces/scratch");
    fs::write(scratch.join("dirty.txt"), b"dirty")?;

    let refused = command(repository.path())
        .args(["workspace", "remove", "scratch"])
        .output()?;
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("未提交或未跟踪修改"));
    assert!(scratch.join("dirty.txt").is_file());

    run_success(
        repository.path(),
        &["workspace", "remove", "scratch", "--force"],
    )?;
    assert!(!scratch.exists());
    let connection = Connection::open(
        repository
            .path()
            .join(".neoengram/metadata/metadata.sqlite3"),
    )?;
    let registered: i64 = connection.query_row(
        "SELECT COUNT(*) FROM workspaces WHERE name = 'scratch'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(registered, 0);
    Ok(())
}

#[test]
fn gc_and_fsck_include_every_workspace_index() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init"])?;
    fs::write(repository.path().join("base.txt"), b"base")?;
    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;
    run_success(
        repository.path(),
        &["workspace", "create", "staged", "--from", "HEAD"],
    )?;
    let staged = repository.path().join("workspaces/staged");
    fs::write(staged.join("staged-only.bin"), b"workspace-only payload")?;
    run_success(&staged, &["add", "staged-only.bin"])?;

    run_success(repository.path(), &["gc"])?;
    run_success(repository.path(), &["fsck"])?;
    run_success(&staged, &["commit", "-m", "after gc"])?;
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
