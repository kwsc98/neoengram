use std::{fs, path::Path, process::Command};

use serde::{de::DeserializeOwned, Deserialize};
use synapse::{Chunk, Commit, FileNode, Index, Tree, INDEX_FORMAT_VERSION};

#[test]
fn initializes_repository_layout_idempotently() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;

    let first = command(temporary.path()).arg("init").output()?;
    assert!(first.status.success());
    assert!(String::from_utf8(first.stdout)?.contains("Initialized empty Synapse repository"));

    let metadata_dir = temporary.path().join(".synapse/metadata");
    assert!(temporary.path().join(".synapse/objects").is_dir());
    assert!(temporary.path().join(".synapse/files").is_dir());
    assert!(temporary.path().join(".synapse/staging").is_dir());
    assert!(temporary.path().join(".synapse/transactions").is_dir());
    assert!(metadata_dir.join("trees").is_dir());
    assert!(metadata_dir.join("manifests").is_dir());
    assert!(metadata_dir.join("commits").is_dir());
    assert!(metadata_dir.join("refs/heads").is_dir());
    assert_eq!(
        fs::read_to_string(metadata_dir.join("HEAD"))?,
        "ref: refs/heads/main\n"
    );
    let index = read_index(&metadata_dir)?;
    assert_eq!(index, Index::default());
    let repository_config: serde_json::Value = read_json(&metadata_dir.join("repository.json"))?;
    assert_eq!(repository_config["format_version"], 3);
    assert_eq!(repository_config["metadata_store"], "json");
    assert_eq!(repository_config["object_store"], "loose");

    let original_index = fs::read(metadata_dir.join("index.json"))?;
    let second = command(temporary.path()).arg("init").output()?;
    assert!(second.status.success());
    assert!(String::from_utf8(second.stdout)?.contains("Reinitialized existing Synapse repository"));
    assert_eq!(fs::read(metadata_dir.join("index.json"))?, original_index);
    Ok(())
}

#[test]
fn init_rejects_an_incomplete_existing_repository() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let synapse_dir = temporary.path().join(".synapse");
    fs::create_dir_all(synapse_dir.join("metadata"))?;

    let output = command(temporary.path()).arg("init").output()?;

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("已有目录不是受支持的 Synapse 仓库"));
    assert!(!synapse_dir.join("metadata/repository.json").exists());
    assert!(!synapse_dir.join("objects").exists());
    assert!(!synapse_dir.join("files").exists());
    assert!(!synapse_dir.join("staging").exists());
    assert!(!synapse_dir.join("transactions").exists());
    Ok(())
}

#[test]
fn init_rejects_the_previous_repository_format() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let metadata_dir = temporary.path().join(".synapse/metadata");
    fs::create_dir_all(&metadata_dir)?;
    fs::write(
        metadata_dir.join("repository.json"),
        br#"{"format_version":2,"metadata_store":"json","object_store":"loose"}"#,
    )?;

    let output = command(temporary.path()).arg("init").output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("格式版本 2"));
    assert!(stderr.contains("当前支持 3"));
    assert!(!temporary.path().join(".synapse/objects").exists());
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

    let metadata_dir = temporary.path().join(".synapse/metadata");
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

    let objects = object_entries(&temporary.path().join(".synapse/objects"))?;
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

    let index = read_index(&temporary.path().join(".synapse/metadata"))?;
    assert_eq!(index.files.len(), 1);
    assert_eq!(index.files[0].path, "dataset/shard/weights.pt");
    assert_eq!(
        object_entries(&temporary.path().join(".synapse/objects"))?,
        1
    );
    Ok(())
}

#[test]
fn adds_a_directory_but_excludes_synapse_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    fs::create_dir_all(temporary.path().join("dataset"))?;
    fs::write(temporary.path().join("dataset/a.pt"), b"first")?;
    fs::write(temporary.path().join("dataset/b.pt"), b"second")?;
    fs::write(temporary.path().join(".synapse/private.bin"), b"internal")?;

    let output = command(temporary.path()).args(["add", "."]).output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("dataset/a.pt"));
    assert!(stdout.contains("dataset/b.pt"));
    assert!(!stdout.contains("private.bin"));

    let index = read_index(&temporary.path().join(".synapse/metadata"))?;
    let paths: Vec<&str> = index.files.iter().map(|file| file.path.as_str()).collect();
    assert_eq!(paths, ["dataset/a.pt", "dataset/b.pt"]);
    assert_eq!(
        object_entries(&temporary.path().join(".synapse/objects"))?,
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

    let metadata_dir = temporary.path().join(".synapse/metadata");
    let first_id = current_commit_id(&metadata_dir)?;
    assert!(is_hash(&first_id));
    let first: Commit = read_json(&metadata_dir.join(format!("commits/{first_id}.json")))?;
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
    assert_eq!(directory_entries(&metadata_dir.join("commits"))?, 1);

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
    let second: Commit = read_json(&metadata_dir.join(format!("commits/{second_id}.json")))?;
    assert_eq!(second.parent.as_deref(), Some(first_id.as_str()));
    let second_tree = read_tree(&metadata_dir, &second.tree_hash)?;
    let second_paths: Vec<&str> = second_tree
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect();
    assert_eq!(second_paths, ["a.pt", "b.pt"]);
    assert_eq!(directory_entries(&metadata_dir.join("commits"))?, 2);
    Ok(())
}

#[test]
fn checkout_old_commit_restores_snapshot_without_rewinding_main(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let metadata_dir = temporary.path().join(".synapse/metadata");

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
    let first: Commit = read_json(&metadata_dir.join(format!("commits/{first_id}.json")))?;
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
    fs::write(temporary.path().join("keep.local"), b"untracked")?;

    let checkout = command(temporary.path())
        .args(["checkout", &first_id])
        .output()?;
    assert!(checkout.status.success());
    assert_eq!(fs::read(temporary.path().join("a.pt"))?, b"version one");
    assert!(!temporary.path().join("b.pt").exists());
    assert_eq!(fs::read(temporary.path().join("keep.local"))?, b"untracked");
    assert_eq!(current_commit_id(&metadata_dir)?, second_id);
    let restored_index = read_index(&metadata_dir)?;
    assert_eq!(restored_index.files, first_tree.files);

    // checkout 老快照后再 commit，会在原 main tip 后追加一个单父回滚提交。
    assert!(command(temporary.path())
        .args(["commit", "-m", "restore v1"])
        .output()?
        .status
        .success());
    let restored_id = current_commit_id(&metadata_dir)?;
    let restored: Commit = read_json(&metadata_dir.join(format!("commits/{restored_id}.json")))?;
    assert_eq!(restored.parent.as_deref(), Some(second_id.as_str()));
    assert_eq!(restored.tree_hash, first.tree_hash);
    Ok(())
}

#[test]
fn checkout_refuses_dirty_files_before_writing_and_force_restores_them(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let metadata_dir = temporary.path().join(".synapse/metadata");

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
    let index_before = fs::read(metadata_dir.join("index.json"))?;
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
    assert_eq!(fs::read(metadata_dir.join("index.json"))?, index_before);
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
    let metadata_dir = temporary.path().join(".synapse/metadata");
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

    let head: Commit = read_json(&metadata_dir.join(format!(
        "commits/{}.json",
        current_commit_id(&metadata_dir)?
    )))?;
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
    let metadata_dir = temporary.path().join(".synapse/metadata");

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
    let head: Commit = read_json(&metadata_dir.join(format!("commits/{head_id}.json")))?;
    let tree = read_tree(&metadata_dir, &head.tree_hash)?;

    fs::write(temporary.path().join("a.pt"), b"staged-a")?;
    fs::write(temporary.path().join("z.pt"), b"staged-z")?;
    assert!(command(temporary.path())
        .args(["add", "."])
        .output()?
        .status
        .success());
    let index_before = fs::read(metadata_dir.join("index.json"))?;

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
            .join(".synapse/objects")
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
    assert_eq!(fs::read(metadata_dir.join("index.json"))?, index_before);
    assert_eq!(current_commit_id(&metadata_dir)?, head_id);
    Ok(())
}

#[test]
fn checkout_rebuilds_nested_multichunk_and_empty_files() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    initialize(temporary.path())?;
    let metadata_dir = temporary.path().join(".synapse/metadata");
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
    let head_before = fs::read(metadata_dir.join("refs/heads/main"))?;

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
    assert_eq!(fs::read(metadata_dir.join("refs/heads/main"))?, head_before);
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
    assert!(String::from_utf8(output.stderr)?.contains("请先运行 `synapse add`"));

    let metadata_dir = temporary.path().join(".synapse/metadata");
    assert_eq!(directory_entries(&metadata_dir.join("trees"))?, 0);
    assert_eq!(directory_entries(&metadata_dir.join("commits"))?, 0);
    assert!(!metadata_dir.join("refs/heads/main").exists());
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

    let index = read_index(&repository.path().join(".synapse/metadata"))?;
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
    assert!(String::from_utf8(output.stderr)?.contains("synapse init"));
    Ok(())
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

fn command(current_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_synapse"));
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
    total_size: u64,
    chunk_count: u64,
    manifest_id: String,
}

#[derive(Deserialize)]
struct StoredManifest {
    total_size: u64,
    chunks: Vec<Chunk>,
}

fn read_index(metadata_dir: &Path) -> Result<Index, Box<dyn std::error::Error>> {
    Ok(Index {
        format_version: INDEX_FORMAT_VERSION,
        files: read_file_set(metadata_dir, &metadata_dir.join("index.json"))?,
    })
}

fn read_tree(metadata_dir: &Path, id: &str) -> Result<Tree, Box<dyn std::error::Error>> {
    Ok(Tree {
        files: read_file_set(metadata_dir, &metadata_dir.join(format!("trees/{id}.json")))?,
    })
}

fn read_file_set(
    metadata_dir: &Path,
    path: &Path,
) -> Result<Vec<FileNode>, Box<dyn std::error::Error>> {
    let stored: StoredFileSet = read_json(path)?;
    let mut files = Vec::with_capacity(stored.files.len());
    for record in stored.files {
        let manifest: StoredManifest =
            read_json(&metadata_dir.join(format!("manifests/{}.json", record.manifest_id)))?;
        assert_eq!(manifest.total_size, record.total_size);
        assert_eq!(u64::try_from(manifest.chunks.len())?, record.chunk_count);
        files.push(FileNode {
            path: record.path,
            total_size: record.total_size,
            chunks: manifest.chunks,
        });
    }
    Ok(files)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    let value = serde_json::from_slice(&bytes)?;
    Ok(value)
}

fn directory_entries(path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    Ok(entries.len())
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
    Ok(fs::read_to_string(metadata_dir.join("refs/heads/main"))?
        .trim()
        .to_owned())
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
