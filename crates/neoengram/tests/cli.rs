use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use neoengram_core::{Chunk, Commit, FileNode, Index, Tree, INDEX_FORMAT_VERSION};
use rusqlite::{params, Connection};
use serde::de::DeserializeOwned;

#[test]
fn default_init_uses_sqlite_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;

    let first = command(temporary.path()).arg("init").output()?;
    assert!(first.status.success());
    assert!(String::from_utf8(first.stdout)?.contains("Initialized empty NeoEngram repository"));

    let metadata_dir = temporary.path().join(".neoengram/metadata");
    assert!(temporary.path().join(".neoengram/objects").is_dir());
    assert!(temporary
        .path()
        .join(".neoengram/cache/materialized")
        .is_dir());
    assert!(temporary.path().join(".neoengram/staging").is_dir());
    assert!(temporary.path().join(".neoengram/transactions").is_dir());
    assert!(temporary.path().join("workspaces").is_dir());
    assert!(temporary.path().join("mounts").is_dir());
    assert!(temporary.path().join("exports").is_dir());
    let repository_config: serde_json::Value = read_json(&metadata_dir.join("repository.json"))?;
    assert_eq!(repository_config["format_version"], 6);
    assert_eq!(
        repository_config["repository_id"]
            .as_str()
            .expect("repository id")
            .len(),
        64
    );
    assert!(repository_config.get("metadata_store").is_none());
    assert_eq!(repository_config["object_store"], "loose");
    assert!(metadata_dir.join("metadata.sqlite3").is_file());
    assert!(!metadata_dir.join("index.json").exists());

    let second = command(temporary.path()).arg("init").output()?;
    assert!(second.status.success());
    assert!(
        String::from_utf8(second.stdout)?.contains("Reinitialized existing NeoEngram repository")
    );
    let reopened_config: serde_json::Value = read_json(&metadata_dir.join("repository.json"))?;
    assert!(reopened_config.get("metadata_store").is_none());
    assert!(metadata_dir.join("metadata.sqlite3").is_file());
    assert!(!metadata_dir.join("index.json").exists());
    Ok(())
}

#[test]
fn init_creates_a_missing_nested_repository_root() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let output = command(temporary.path())
        .args(["init", "nested/repository"])
        .output()?;
    assert!(
        output.status.success(),
        "nested init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let metadata_dir = temporary
        .path()
        .join("nested/repository/.neoengram/metadata");
    let config: serde_json::Value = read_json(&metadata_dir.join("repository.json"))?;
    assert_eq!(config["format_version"], 6);
    assert!(metadata_dir.join("metadata.sqlite3").is_file());
    Ok(())
}

#[test]
fn crashed_new_init_can_be_retried() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;

    let failed = command(temporary.path())
        .env("NEOENGRAM_TEST_CRASH_AT", "init-before-publish")
        .arg("init")
        .output()?;
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("init-before-publish"));
    assert!(!temporary.path().join(".neoengram").exists());
    assert!(
        fs::read_dir(temporary.path())?.any(|entry| entry.is_ok_and(|entry| entry
            .file_name()
            .to_string_lossy()
            .starts_with(".neoengram-tmp-")))
    );

    let retried = command(temporary.path()).arg("init").output()?;
    assert!(
        retried.status.success(),
        "retry failed: {}",
        String::from_utf8_lossy(&retried.stderr)
    );
    let metadata_dir = temporary.path().join(".neoengram/metadata");
    let config: serde_json::Value = read_json(&metadata_dir.join("repository.json"))?;
    assert_eq!(config["format_version"], 6);
    assert!(metadata_dir.join("metadata.sqlite3").is_file());
    let add = command(temporary.path()).args(["add", "."]).output()?;
    assert!(
        add.status.success(),
        "add after retry failed: {}",
        String::from_utf8_lossy(&add.stderr)
    );
    assert_eq!(read_index(&metadata_dir)?, Index::default());
    Ok(())
}

#[test]
fn concurrent_init_publishes_exactly_one_repository() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let first = command(temporary.path())
        .arg("init")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let second = command(temporary.path())
        .arg("init")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let first = first.wait_with_output()?;
    let second = second.wait_with_output()?;
    assert_ne!(
        first.status.success(),
        second.status.success(),
        "first stderr: {}\nsecond stderr: {}",
        String::from_utf8_lossy(&first.stderr),
        String::from_utf8_lossy(&second.stderr)
    );

    let metadata_dir = temporary.path().join(".neoengram/metadata");
    let config: serde_json::Value = read_json(&metadata_dir.join("repository.json"))?;
    assert_eq!(config["format_version"], 6);
    assert!(metadata_dir.join("metadata.sqlite3").is_file());
    Ok(())
}

#[test]
fn removed_metadata_store_option_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let output = command(temporary.path())
        .args(["init", "--metadata-store", "json"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("unexpected argument '--metadata-store'"));
    assert!(!temporary.path().join(".neoengram").exists());
    Ok(())
}

#[test]
fn mount_rejects_nonempty_and_in_repository_mountpoints() -> Result<(), Box<dyn std::error::Error>>
{
    let (_temporary, repository, mountpoint) = committed_mount_fixture()?;
    fs::write(mountpoint.join("occupied"), b"x")?;
    let nonempty = command(&repository)
        .arg("mount")
        .arg("HEAD")
        .arg(&mountpoint)
        .output()?;
    assert!(!nonempty.status.success());
    assert!(String::from_utf8_lossy(&nonempty.stderr).contains("挂载点必须为空"));

    let inside = repository.join("inside-mount");
    fs::create_dir(&inside)?;
    let nested = command(&repository)
        .arg("mount")
        .arg("HEAD")
        .arg(&inside)
        .output()?;
    assert!(!nested.status.success());
    assert!(String::from_utf8_lossy(&nested.stderr).contains("仓库根目录完全分离"));
    Ok(())
}

#[cfg(not(feature = "fuse-mount"))]
#[test]
fn mount_without_feature_reports_the_required_build_option(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, repository, mountpoint) = committed_mount_fixture()?;
    let output = command(&repository)
        .arg("mount")
        .arg("HEAD")
        .arg(&mountpoint)
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--features fuse-mount"));
    Ok(())
}

#[cfg(not(feature = "fuse-mount"))]
#[test]
fn mount_accepts_a_managed_mountpoint_before_platform_startup(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, repository, _outside) = committed_mount_fixture()?;
    let managed = repository.join("mounts/managed");
    fs::create_dir(&managed)?;
    let output = command(&repository)
        .args(["mount", "HEAD"])
        .arg(&managed)
        .output()?;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("未启用 FUSE"), "{stderr}");
    assert!(!stderr.contains("仓库根目录完全分离"), "{stderr}");
    Ok(())
}

#[test]
fn init_rejects_an_incomplete_existing_repository() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let neoengram_dir = temporary.path().join(".neoengram");
    fs::create_dir_all(neoengram_dir.join("metadata"))?;

    let output = command(temporary.path()).arg("init").output()?;

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("已有目录不是受支持的 NeoEngram 仓库"));
    assert!(!neoengram_dir.join("metadata/repository.json").exists());
    assert!(!neoengram_dir.join("objects").exists());
    assert!(!neoengram_dir.join("files").exists());
    assert!(!neoengram_dir.join("staging").exists());
    assert!(!neoengram_dir.join("transactions").exists());
    Ok(())
}

#[test]
fn init_rejects_the_previous_repository_format() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let metadata_dir = temporary.path().join(".neoengram/metadata");
    fs::create_dir_all(&metadata_dir)?;
    fs::write(
        metadata_dir.join("repository.json"),
        br#"{"format_version":4,"object_store":"loose"}"#,
    )?;

    let output = command(temporary.path()).arg("init").output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("格式版本 4"));
    assert!(stderr.contains("当前支持 6"));
    assert!(!temporary.path().join(".neoengram/objects").exists());
    Ok(())
}

#[test]
fn add_persists_a_normalized_index_and_reuses_objects() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    fs::create_dir_all(temporary.path().join("models"))?;
    fs::write(temporary.path().join("models/weights.pt"), b"model payload")?;

    let first = command(temporary.path())
        .args(["add", "./models/../models/weights.pt"])
        .output()?;
    assert!(first.status.success());
    assert_eq!(
        String::from_utf8(first.stdout)?,
        "Processed: models/weights.pt, 1 chunks created\n"
    );

    let metadata_dir = temporary.path().join(".neoengram/metadata");
    let first_index = read_index(&metadata_dir)?;
    assert_eq!(first_index.files.len(), 1);
    assert_eq!(first_index.files[0].path, "models/weights.pt");
    assert_eq!(first_index.files[0].total_size, 13);
    assert_eq!(first_index.files[0].chunks.len(), 1);

    let second = command(temporary.path())
        .args(["add", "models/weights.pt"])
        .output()?;
    assert!(second.status.success());
    let second_index = read_index(&metadata_dir)?;
    assert_eq!(second_index, first_index);

    let objects = object_entries(&temporary.path().join(".neoengram/objects"))?;
    assert_eq!(objects, 1);
    Ok(())
}

#[test]
fn add_from_a_subdirectory_uses_repository_relative_paths() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let shard = temporary.path().join("dataset/shard");
    fs::create_dir_all(&shard)?;
    fs::write(shard.join("weights.pt"), b"weights")?;

    let output = command(&shard).args(["add", "weights.pt"]).output()?;
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout)?.contains("dataset/shard/weights.pt"));

    let index = read_index(&temporary.path().join(".neoengram/metadata"))?;
    assert_eq!(index.files.len(), 1);
    assert_eq!(index.files[0].path, "dataset/shard/weights.pt");
    assert_eq!(
        object_entries(&temporary.path().join(".neoengram/objects"))?,
        1
    );
    Ok(())
}

#[test]
fn adds_a_directory_but_excludes_neoengram_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    fs::create_dir_all(temporary.path().join("dataset"))?;
    fs::write(temporary.path().join("dataset/a.pt"), b"first")?;
    fs::write(temporary.path().join("dataset/b.pt"), b"second")?;
    fs::write(temporary.path().join(".neoengram/private.bin"), b"internal")?;

    let output = command(temporary.path()).args(["add", "."]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("dataset/a.pt"));
    assert!(stdout.contains("dataset/b.pt"));
    assert!(!stdout.contains("private.bin"));

    let index = read_index(&temporary.path().join(".neoengram/metadata"))?;
    let paths: Vec<&str> = index.files.iter().map(|file| file.path.as_str()).collect();
    assert_eq!(paths, ["dataset/a.pt", "dataset/b.pt"]);
    assert_eq!(
        object_entries(&temporary.path().join(".neoengram/objects"))?,
        2
    );
    Ok(())
}

#[test]
fn creates_linear_single_parent_commits() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    fs::write(temporary.path().join("a.pt"), b"first version")?;
    fs::write(temporary.path().join("b.pt"), b"unchanged")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    let nested_directory = temporary.path().join("nested");
    fs::create_dir(&nested_directory)?;

    // 从子目录提交仍应更新仓库根目录的 HEAD/ref，不能创建嵌套仓库。
    let first_output = command(&nested_directory)
        .args(["commit", "-m", "initial dataset"])
        .output()?;
    assert!(first_output.status.success());

    let metadata_dir = temporary.path().join(".neoengram/metadata");
    let first_id = current_commit_id(&metadata_dir)?;
    assert!(is_hash(&first_id));
    let first = read_commit(&metadata_dir, &first_id)?;
    assert_eq!(first.parent, None);
    assert_eq!(first.message, "initial dataset");
    let first_tree = read_tree(&metadata_dir, &first.tree_hash)?;
    let index = read_index(&metadata_dir)?;
    assert_eq!(first_tree.files, index.files);
    assert_eq!(first_tree.files.len(), 2);

    let unchanged = command(temporary.path())
        .args(["commit", "-m", "duplicate"])
        .output()?;
    assert!(!unchanged.status.success());
    assert!(String::from_utf8(unchanged.stderr)?.contains("没有可提交的变更"));
    assert_eq!(metadata_count(&metadata_dir, "commits")?, 1);

    fs::write(temporary.path().join("a.pt"), b"second version")?;
    assert!(command(temporary.path())
        .args(["add", "a.pt"])
        .output()?
        .status
        .success());
    let second_output = command(temporary.path())
        .args(["commit", "-m", "update a"])
        .output()?;
    assert!(second_output.status.success());

    let second_id = current_commit_id(&metadata_dir)?;
    assert_ne!(second_id, first_id);
    let second = read_commit(&metadata_dir, &second_id)?;
    assert_eq!(second.parent.as_deref(), Some(first_id.as_str()));
    let second_tree = read_tree(&metadata_dir, &second.tree_hash)?;
    let second_paths: Vec<&str> = second_tree
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect();
    assert_eq!(second_paths, ["a.pt", "b.pt"]);
    assert_eq!(metadata_count(&metadata_dir, "commits")?, 2);
    Ok(())
}

#[test]
fn detached_checkout_commits_from_the_target_and_can_reattach_main(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let metadata_dir = temporary.path().join(".neoengram/metadata");

    fs::write(temporary.path().join("a.pt"), b"version one")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "v1"])
        .output()?
        .status
        .success());
    let first_id = current_commit_id(&metadata_dir)?;
    let first = read_commit(&metadata_dir, &first_id)?;
    let first_tree = read_tree(&metadata_dir, &first.tree_hash)?;

    fs::write(temporary.path().join("a.pt"), b"version two")?;
    fs::write(temporary.path().join("b.pt"), b"new in v2")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "v2"])
        .output()?
        .status
        .success());
    let second_id = current_commit_id(&metadata_dir)?;
    let second = read_commit(&metadata_dir, &second_id)?;
    let second_tree = read_tree(&metadata_dir, &second.tree_hash)?;
    fs::write(temporary.path().join("keep.local"), b"untracked")?;

    let checkout = command(temporary.path())
        .args(["checkout", &first_id])
        .output()?;
    assert!(checkout.status.success());
    let checkout_stdout = String::from_utf8(checkout.stdout)?;
    assert!(checkout_stdout.contains("HEAD is now detached at"));
    assert!(checkout_stdout.contains(&first_id));
    assert_eq!(fs::read(temporary.path().join("a.pt"))?, b"version one");
    assert!(!temporary.path().join("b.pt").exists());
    assert_eq!(fs::read(temporary.path().join("keep.local"))?, b"untracked");
    assert_eq!(current_commit_id(&metadata_dir)?, second_id);
    assert_eq!(read_head(&metadata_dir)?, format!("{first_id}\n"));
    let restored_index = read_index(&metadata_dir)?;
    assert_eq!(restored_index.files, first_tree.files);

    let detached_log = command(temporary.path()).arg("log").output()?;
    assert!(detached_log.status.success());
    let detached_log = String::from_utf8(detached_log.stdout)?;
    assert!(detached_log.contains(&first_id));
    assert!(!detached_log.contains(&second_id));

    let detached_show = command(temporary.path()).args(["show", "HEAD"]).output()?;
    assert!(detached_show.status.success());
    assert!(String::from_utf8(detached_show.stdout)?.contains(&first_id));

    let detached_status = command(temporary.path()).arg("status").output()?;
    assert!(detached_status.status.success());
    let detached_status = String::from_utf8(detached_status.stdout)?;
    assert!(detached_status.contains("HEAD detached at"));
    assert!(detached_status.contains(&first_id));

    fs::write(temporary.path().join("a.pt"), b"detached change")?;
    assert!(command(temporary.path())
        .args(["add", "a.pt"])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "detached change"])
        .output()?
        .status
        .success());
    let detached_id = read_head(&metadata_dir)?.trim().to_owned();
    assert!(is_hash(&detached_id));
    assert_ne!(detached_id, first_id);
    assert_eq!(current_commit_id(&metadata_dir)?, second_id);
    let detached = read_commit(&metadata_dir, &detached_id)?;
    assert_eq!(detached.parent.as_deref(), Some(first_id.as_str()));
    assert_ne!(detached.tree_hash, first.tree_hash);

    let detached_log = command(temporary.path()).arg("log").output()?;
    assert!(detached_log.status.success());
    let detached_log = String::from_utf8(detached_log.stdout)?;
    let detached_position = detached_log
        .find(&detached_id)
        .ok_or("detached commit missing from log")?;
    let first_position = detached_log
        .find(&first_id)
        .ok_or("detached parent missing from log")?;
    assert!(detached_position < first_position);
    assert!(!detached_log.contains(&second_id));

    let reattach = command(temporary.path())
        .args(["checkout", "main"])
        .output()?;
    assert!(reattach.status.success());
    let reattach_stdout = String::from_utf8(reattach.stdout)?;
    assert!(reattach_stdout.contains("main"));
    assert_eq!(read_head(&metadata_dir)?, "ref: refs/heads/main\n");
    assert_eq!(current_commit_id(&metadata_dir)?, second_id);
    assert_eq!(fs::read(temporary.path().join("a.pt"))?, b"version two");
    assert_eq!(fs::read(temporary.path().join("b.pt"))?, b"new in v2");
    assert_eq!(read_index(&metadata_dir)?.files, second_tree.files);

    let main_status = command(temporary.path()).arg("status").output()?;
    assert!(main_status.status.success());
    assert!(String::from_utf8(main_status.stdout)?.contains("On branch main"));

    let main_log = command(temporary.path()).arg("log").output()?;
    assert!(main_log.status.success());
    let main_log = String::from_utf8(main_log.stdout)?;
    let second_position = main_log
        .find(&second_id)
        .ok_or("main tip missing from log")?;
    let first_position = main_log
        .find(&first_id)
        .ok_or("main root missing from log")?;
    assert!(second_position < first_position);
    assert!(!main_log.contains(&detached_id));

    let detached_show = command(temporary.path())
        .args(["show", detached_id.as_str()])
        .output()?;
    assert!(detached_show.status.success());
    assert!(String::from_utf8(detached_show.stdout)?.contains(&detached_id));
    Ok(())
}

#[test]
fn checkout_head_preserves_the_current_head_mode() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let metadata_dir = temporary.path().join(".neoengram/metadata");
    let model = temporary.path().join("model.pt");

    fs::write(&model, b"committed")?;
    assert!(command(temporary.path())
        .args(["add", "model.pt"])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "base"])
        .output()?
        .status
        .success());
    let commit_id = current_commit_id(&metadata_dir)?;

    fs::write(&model, b"attached dirty")?;
    assert!(command(temporary.path())
        .args(["checkout", "HEAD", "--force"])
        .output()?
        .status
        .success());
    assert_eq!(fs::read(&model)?, b"committed");
    assert_eq!(read_head(&metadata_dir)?, "ref: refs/heads/main\n");
    let attached_status = command(temporary.path()).arg("status").output()?;
    assert!(attached_status.status.success());
    assert!(String::from_utf8(attached_status.stdout)?.contains("On branch main"));

    assert!(command(temporary.path())
        .args(["checkout", commit_id.as_str()])
        .output()?
        .status
        .success());
    assert_eq!(read_head(&metadata_dir)?, format!("{commit_id}\n"));

    fs::write(&model, b"detached dirty")?;
    assert!(command(temporary.path())
        .args(["checkout", "HEAD", "--force"])
        .output()?
        .status
        .success());
    assert_eq!(fs::read(&model)?, b"committed");
    assert_eq!(read_head(&metadata_dir)?, format!("{commit_id}\n"));
    let detached_status = command(temporary.path()).arg("status").output()?;
    assert!(detached_status.status.success());
    let detached_status = String::from_utf8(detached_status.stdout)?;
    assert!(detached_status.contains("HEAD detached at"));
    assert!(detached_status.contains(&commit_id));
    Ok(())
}

#[test]
fn checkout_refuses_dirty_files_before_writing_and_force_restores_them(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let metadata_dir = temporary.path().join(".neoengram/metadata");

    fs::write(temporary.path().join("a.pt"), b"a-v1")?;
    fs::write(temporary.path().join("z.pt"), b"z-v1")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "v1"])
        .output()?
        .status
        .success());
    let first_id = current_commit_id(&metadata_dir)?;

    fs::write(temporary.path().join("a.pt"), b"a-v2")?;
    fs::write(temporary.path().join("z.pt"), b"z-v2")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "v2"])
        .output()?
        .status
        .success());
    let second_id = current_commit_id(&metadata_dir)?;
    let index_before = read_index(&metadata_dir)?;
    fs::write(temporary.path().join("z.pt"), b"local change")?;

    let refused = command(temporary.path())
        .args(["checkout", &first_id])
        .output()?;
    assert!(!refused.status.success());
    let stderr = String::from_utf8(refused.stderr)?;
    assert!(stderr.contains("z.pt"));
    assert!(stderr.contains("--force"));
    assert_eq!(fs::read(temporary.path().join("a.pt"))?, b"a-v2");
    assert_eq!(fs::read(temporary.path().join("z.pt"))?, b"local change");
    assert_eq!(read_index(&metadata_dir)?, index_before);
    assert_eq!(current_commit_id(&metadata_dir)?, second_id);

    let forced = command(temporary.path())
        .args(["checkout", &first_id, "--force"])
        .output()?;
    assert!(forced.status.success());
    assert_eq!(fs::read(temporary.path().join("a.pt"))?, b"a-v1");
    assert_eq!(fs::read(temporary.path().join("z.pt"))?, b"z-v1");
    assert_eq!(current_commit_id(&metadata_dir)?, second_id);
    Ok(())
}

#[test]
fn checkout_requires_force_for_staged_or_missing_tracked_files(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let metadata_dir = temporary.path().join(".neoengram/metadata");
    let file_path = temporary.path().join("weights.pt");

    fs::write(&file_path, b"committed")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "base"])
        .output()?
        .status
        .success());

    fs::write(&file_path, b"staged")?;
    assert!(command(temporary.path())
        .args(["add", "weights.pt"])
        .output()?
        .status
        .success());
    let staged = command(temporary.path()).args(["checkout"]).output()?;
    assert!(!staged.status.success());
    assert!(String::from_utf8(staged.stderr)?.contains("index"));
    assert_eq!(fs::read(&file_path)?, b"staged");

    assert!(command(temporary.path())
        .args(["checkout", "HEAD", "--force"])
        .output()?
        .status
        .success());
    assert_eq!(fs::read(&file_path)?, b"committed");

    fs::remove_file(&file_path)?;
    let missing = command(temporary.path()).args(["checkout"]).output()?;
    assert!(!missing.status.success());
    assert!(String::from_utf8(missing.stderr)?.contains("缺失"));
    assert!(command(temporary.path())
        .args(["checkout", "HEAD", "--force"])
        .output()?
        .status
        .success());
    assert_eq!(fs::read(&file_path)?, b"committed");

    let head = read_commit(&metadata_dir, &current_commit_id(&metadata_dir)?)?;
    let index = read_index(&metadata_dir)?;
    let tree = read_tree(&metadata_dir, &head.tree_hash)?;
    assert_eq!(index.files, tree.files);
    Ok(())
}

#[test]
fn checkout_detects_chunk_corruption_before_mutating_workspace(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let metadata_dir = temporary.path().join(".neoengram/metadata");

    fs::write(temporary.path().join("a.pt"), b"committed-a")?;
    fs::write(temporary.path().join("z.pt"), b"committed-z")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "base"])
        .output()?
        .status
        .success());
    let head_id = current_commit_id(&metadata_dir)?;
    let head = read_commit(&metadata_dir, &head_id)?;
    let tree = read_tree(&metadata_dir, &head.tree_hash)?;

    fs::write(temporary.path().join("a.pt"), b"staged-a")?;
    fs::write(temporary.path().join("z.pt"), b"staged-z")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    let index_before = read_index(&metadata_dir)?;

    let z_node = tree
        .files
        .iter()
        .find(|file| file.path == "z.pt")
        .ok_or("missing z.pt in tree")?;
    let z_chunk = z_node.chunks.first().ok_or("missing z.pt chunk")?;
    let corrupt = vec![b'x'; usize::try_from(z_chunk.size)?];
    fs::write(
        temporary
            .path()
            .join(".neoengram/objects")
            .join(&z_chunk.hash),
        corrupt,
    )?;

    let output = command(temporary.path())
        .args(["checkout", "HEAD", "--force"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("Hash 损坏"));
    assert_eq!(fs::read(temporary.path().join("a.pt"))?, b"staged-a");
    assert_eq!(fs::read(temporary.path().join("z.pt"))?, b"staged-z");
    assert_eq!(read_index(&metadata_dir)?, index_before);
    assert_eq!(current_commit_id(&metadata_dir)?, head_id);
    Ok(())
}

#[test]
fn checkout_rebuilds_nested_multichunk_and_empty_files() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let metadata_dir = temporary.path().join(".neoengram/metadata");
    let nested = temporary.path().join("nested");
    fs::create_dir(&nested)?;
    let large = vec![0x5a; 5 * 1024 * 1024];
    fs::write(nested.join("model.bin"), &large)?;
    fs::write(temporary.path().join("empty.bin"), [])?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    let index = read_index(&metadata_dir)?;
    let model = index
        .files
        .iter()
        .find(|file| file.path == "nested/model.bin")
        .ok_or("missing model in index")?;
    assert!(model.chunks.len() >= 2);
    assert!(command(temporary.path())
        .args(["commit", "-m", "large"])
        .output()?
        .status
        .success());
    let head_before = current_commit_id(&metadata_dir)?;

    fs::remove_file(nested.join("model.bin"))?;
    fs::remove_dir(&nested)?;
    fs::remove_file(temporary.path().join("empty.bin"))?;
    let runner = temporary.path().join("runner");
    fs::create_dir(&runner)?;
    fs::write(runner.join("keep.local"), b"keep")?;

    let output = command(&runner)
        .args(["checkout", "HEAD", "--force"])
        .output()?;
    assert!(output.status.success());
    assert_eq!(fs::read(temporary.path().join("nested/model.bin"))?, large);
    assert_eq!(fs::metadata(temporary.path().join("empty.bin"))?.len(), 0);
    assert_eq!(fs::read(runner.join("keep.local"))?, b"keep");
    assert_eq!(current_commit_id(&metadata_dir)?, head_before);
    Ok(())
}

#[cfg(unix)]
#[test]
fn checkout_force_replaces_only_a_leaf_symlink() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    initialize(temporary.path())?;
    fs::write(temporary.path().join("model.pt"), b"model")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "model"])
        .output()?
        .status
        .success());

    let sentinel = outside.path().join("sentinel");
    fs::write(&sentinel, b"outside")?;
    fs::remove_file(temporary.path().join("model.pt"))?;
    symlink(&sentinel, temporary.path().join("model.pt"))?;

    let refused = command(temporary.path()).args(["checkout"]).output()?;
    assert!(!refused.status.success());
    let forced = command(temporary.path())
        .args(["checkout", "HEAD", "--force"])
        .output()?;
    assert!(forced.status.success());
    assert_eq!(fs::read(temporary.path().join("model.pt"))?, b"model");
    assert_eq!(fs::read(sentinel)?, b"outside");
    Ok(())
}

#[cfg(unix)]
#[test]
fn checkout_rejects_symlinked_parent_even_with_force() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    initialize(temporary.path())?;
    fs::create_dir(temporary.path().join("nested"))?;
    fs::write(temporary.path().join("nested/model.pt"), b"model")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "nested"])
        .output()?
        .status
        .success());

    fs::remove_file(temporary.path().join("nested/model.pt"))?;
    fs::remove_dir(temporary.path().join("nested"))?;
    fs::write(outside.path().join("sentinel"), b"outside")?;
    symlink(outside.path(), temporary.path().join("nested"))?;

    let output = command(temporary.path())
        .args(["checkout", "HEAD", "--force"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("父级不是普通目录"));
    assert_eq!(fs::read(outside.path().join("sentinel"))?, b"outside");
    assert!(!outside.path().join("model.pt").exists());
    Ok(())
}

#[test]
fn empty_repository_has_nothing_to_commit() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;

    let output = command(temporary.path())
        .args(["commit", "-m", "empty"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("请先运行 `neoengram add`"));

    let metadata_dir = temporary.path().join(".neoengram/metadata");
    assert_eq!(metadata_count(&metadata_dir, "directories")?, 0);
    assert_eq!(metadata_count(&metadata_dir, "commits")?, 0);
    assert_eq!(metadata_count(&metadata_dir, "refs")?, 0);
    Ok(())
}

#[test]
fn rejects_paths_outside_the_repository() -> Result<(), Box<dyn std::error::Error>> {
    let repository = tempfile::tempdir()?;
    let outside = tempfile::NamedTempFile::new()?;
    initialize(repository.path())?;

    let output = command(repository.path())
        .arg("add")
        .arg(outside.path())
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("仓库之外"));

    let index = read_index(&repository.path().join(".neoengram/metadata"))?;
    assert!(index.files.is_empty());
    Ok(())
}

#[test]
fn reports_a_missing_input_path() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;

    let output = command(temporary.path())
        .args(["add", "missing.pt"])
        .output()?;

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("无法访问路径"));
    Ok(())
}

#[test]
fn add_requires_an_initialized_repository() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    fs::write(temporary.path().join("weights.pt"), b"model")?;

    let output = command(temporary.path())
        .args(["add", "weights.pt"])
        .output()?;

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("neoengram init"));
    Ok(())
}

#[test]
fn diff_restore_and_gc_cover_the_local_cleanup_workflow() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let model = temporary.path().join("model.bin");
    fs::write(&model, b"version one")?;
    assert!(command(temporary.path())
        .args(["add", "model.bin"])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "initial"])
        .output()?
        .status
        .success());

    fs::write(&model, b"version two")?;
    let diff = command(temporary.path()).arg("diff").output()?;
    assert!(diff.status.success());
    let diff_stdout = String::from_utf8(diff.stdout)?;
    assert!(diff_stdout.contains("modified: model.bin"));

    assert!(command(temporary.path())
        .args(["add", "model.bin"])
        .output()?
        .status
        .success());
    let staged = command(temporary.path())
        .args(["diff", "--staged"])
        .output()?;
    assert!(staged.status.success());
    assert!(String::from_utf8(staged.stdout)?.contains("modified: model.bin"));

    let restore_staged = command(temporary.path())
        .args(["restore", "--staged", "model.bin"])
        .output()?;
    assert!(
        restore_staged.status.success(),
        "restore --staged failed: {}",
        String::from_utf8_lossy(&restore_staged.stderr)
    );
    let status = command(temporary.path()).arg("status").output()?;
    let status_stdout = String::from_utf8(status.stdout)?;
    assert!(status_stdout.contains("Changes not staged for commit:"));
    assert!(!status_stdout.contains("Changes to be committed:"));

    let restore_worktree = command(temporary.path())
        .args(["restore", "--force", "model.bin"])
        .output()?;
    assert!(restore_worktree.status.success());
    assert_eq!(fs::read(&model)?, b"version one");

    // Removing the index entry leaves its immutable payload unreachable; gc can report and then
    // reclaim it without touching the committed snapshot.
    fs::write(&model, b"orphan payload")?;
    assert!(command(temporary.path())
        .args(["add", "model.bin"])
        .output()?
        .status
        .success());
    let before_gc = object_entries(&temporary.path().join(".neoengram/objects"))?;
    assert!(command(temporary.path())
        .args(["rm", "--cached", "model.bin"])
        .output()?
        .status
        .success());
    let dry_run = command(temporary.path())
        .args(["gc", "--dry-run"])
        .output()?;
    assert!(dry_run.status.success());
    assert!(String::from_utf8(dry_run.stdout)?.contains("unreferenced objects"));
    let collected = command(temporary.path()).arg("gc").output()?;
    assert!(collected.status.success());
    assert!(object_entries(&temporary.path().join(".neoengram/objects"))? < before_gc);
    Ok(())
}

#[test]
fn cleanup_commands_work_with_the_default_sqlite_backend() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let initialized = command(temporary.path()).arg("init").output()?;
    assert!(initialized.status.success());
    let model = temporary.path().join("weights.bin");
    fs::write(&model, b"sqlite model")?;
    for arguments in [
        vec!["add", "weights.bin"],
        vec!["commit", "-m", "sqlite"],
        vec!["diff", "--staged"],
        vec!["gc", "--dry-run"],
    ] {
        let output = command(temporary.path()).args(arguments).output()?;
        assert!(
            output.status.success(),
            "command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn gc_preserves_payloads_for_unreachable_immutable_history(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let model = temporary.path().join("model.bin");

    fs::write(&model, b"root")?;
    assert!(command(temporary.path())
        .args(["add", "model.bin"])
        .output()?
        .status
        .success());
    let first = command(temporary.path())
        .args(["commit", "-m", "root"])
        .output()?;
    assert!(first.status.success());
    let first_id = committed_id(&first.stdout)?;

    fs::write(&model, b"main tip")?;
    assert!(command(temporary.path())
        .args(["add", "model.bin"])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "main"])
        .output()?
        .status
        .success());

    // Create a divergent detached commit, then reattach main so it is no longer a named/root
    // history.  Its immutable Tree must still remain fsck-valid until metadata GC exists.
    assert!(command(temporary.path())
        .args(["checkout", &first_id])
        .output()?
        .status
        .success());
    fs::write(&model, b"detached tip")?;
    assert!(command(temporary.path())
        .args(["add", "model.bin"])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["commit", "-m", "detached"])
        .output()?
        .status
        .success());
    assert!(command(temporary.path())
        .args(["checkout", "main"])
        .output()?
        .status
        .success());

    let collected = command(temporary.path()).arg("gc").output()?;
    assert!(collected.status.success());
    let fsck = command(temporary.path()).arg("fsck").output()?;
    assert!(
        fsck.status.success(),
        "fsck after gc failed: {}",
        String::from_utf8_lossy(&fsck.stderr)
    );
    Ok(())
}

fn committed_id(stdout: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let text = String::from_utf8(stdout.to_vec())?;
    let line = text
        .lines()
        .find(|line| line.starts_with("Committed "))
        .ok_or("commit output did not contain an id")?;
    Ok(line["Committed ".len()..].trim().to_owned())
}

fn initialize(path: &Path) -> std::io::Result<()> {
    let output = command(path).arg("init").output()?;
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn committed_mount_fixture(
) -> Result<(tempfile::TempDir, PathBuf, PathBuf), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir_in(std::env::current_dir()?)?;
    let repository = temporary.path().join("repository");
    let mountpoint = temporary.path().join("mount");
    fs::create_dir(&repository)?;
    fs::create_dir(&mountpoint)?;
    initialize(&repository)?;
    fs::write(repository.join("payload.bin"), b"payload")?;
    let add = command(&repository).args(["add", "payload.bin"]).output()?;
    assert!(
        add.status.success(),
        "add failed: {}",
        String::from_utf8_lossy(&add.stderr)
    );
    let commit = command(&repository)
        .args(["commit", "-m", "mount fixture"])
        .output()?;
    assert!(
        commit.status.success(),
        "commit failed: {}",
        String::from_utf8_lossy(&commit.stderr)
    );
    Ok((temporary, repository, mountpoint))
}

fn command(current_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_neoengram"));
    command.current_dir(current_dir);
    command
}

fn read_index(metadata_dir: &Path) -> Result<Index, Box<dyn std::error::Error>> {
    let connection = Connection::open(metadata_dir.join("metadata.sqlite3"))?;
    let mut statement = connection.prepare(
        "SELECT path, total_size, chunk_count, lower(hex(manifest_id)) \
         FROM workspace_index_files WHERE workspace_id = zeroblob(16) ORDER BY path",
    )?;
    let stored = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut files = Vec::with_capacity(stored.len());
    for (path, total_size, chunk_count, manifest_id) in stored {
        let chunks = read_manifest(&connection, &manifest_id)?;
        assert_eq!(u64::try_from(chunks.len())?, u64::try_from(chunk_count)?);
        files.push(FileNode {
            path,
            total_size: u64::try_from(total_size)?,
            chunks,
        });
    }
    Ok(Index {
        format_version: INDEX_FORMAT_VERSION,
        files,
    })
}

fn read_tree(metadata_dir: &Path, id: &str) -> Result<Tree, Box<dyn std::error::Error>> {
    let connection = Connection::open(metadata_dir.join("metadata.sqlite3"))?;
    let mut files = Vec::new();
    read_directory(&connection, id, "", &mut files)?;
    Ok(Tree { files })
}

fn read_directory(
    connection: &Connection,
    id: &str,
    prefix: &str,
    files: &mut Vec<FileNode>,
) -> Result<(), Box<dyn std::error::Error>> {
    let id = blake3::Hash::from_hex(id)?;
    let set_id: Vec<u8> = connection.query_row(
        "SELECT set_id FROM directories WHERE id = ?1",
        params![id.as_bytes().as_slice()],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT name, kind, lower(hex(target_id)), total_size \
         FROM directory_entries WHERE set_id = ?1 ORDER BY ordinal",
    )?;
    let entries = statement
        .query_map(params![set_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for (name, kind, target_id, total_size) in entries {
        let path = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        if kind == 1 {
            files.push(FileNode {
                path,
                total_size: u64::try_from(total_size)?,
                chunks: read_manifest(connection, &target_id)?,
            });
        } else {
            read_directory(connection, &target_id, &path, files)?;
        }
    }
    Ok(())
}

fn read_manifest(
    connection: &Connection,
    id: &str,
) -> Result<Vec<Chunk>, Box<dyn std::error::Error>> {
    let id = blake3::Hash::from_hex(id)?;
    let set_id: Vec<u8> = connection.query_row(
        "SELECT set_id FROM manifests WHERE id = ?1",
        params![id.as_bytes().as_slice()],
        |row| row.get(0),
    )?;
    let mut statement = connection.prepare(
        "SELECT lower(hex(object_id)), offset, size FROM manifest_chunks \
         WHERE set_id = ?1 ORDER BY ordinal",
    )?;
    let chunks = statement
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
    Ok(chunks)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    let value = serde_json::from_slice(&bytes)?;
    Ok(value)
}

fn object_entries(path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(fs::read_dir(path)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_file())
                && entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.len() == 64)
        })
        .count())
}

fn current_commit_id(metadata_dir: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let connection = Connection::open(metadata_dir.join("metadata.sqlite3"))?;
    Ok(connection.query_row(
        "SELECT target FROM refs WHERE name = 'refs/heads/main'",
        [],
        |row| row.get(0),
    )?)
}

fn read_head(metadata_dir: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let connection = Connection::open(metadata_dir.join("metadata.sqlite3"))?;
    let (kind, target): (String, String) = connection.query_row(
        "SELECT head_kind, head_target FROM workspaces WHERE id = zeroblob(16)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if kind == "attached" {
        Ok(format!("ref: {target}\n"))
    } else {
        Ok(format!("{target}\n"))
    }
}

fn read_commit(metadata_dir: &Path, id: &str) -> Result<Commit, Box<dyn std::error::Error>> {
    let connection = Connection::open(metadata_dir.join("metadata.sqlite3"))?;
    let id = blake3::Hash::from_hex(id)?;
    let payload: Vec<u8> = connection.query_row(
        "SELECT payload FROM commits WHERE id = ?1",
        params![id.as_bytes().as_slice()],
        |row| row.get(0),
    )?;
    Ok(serde_json::from_slice(&payload)?)
}

fn metadata_count(metadata_dir: &Path, table: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let table = match table {
        "directories" => "directories",
        "commits" => "commits",
        "refs" => "refs",
        _ => return Err("unsupported metadata table".into()),
    };
    let connection = Connection::open(metadata_dir.join("metadata.sqlite3"))?;
    let count: i64 = connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })?;
    Ok(usize::try_from(count)?)
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
