use std::{error::Error, fs, path::Path, process::Command};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn ignore_rules_apply_consistently_to_add_and_status() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init", "--metadata-store", "json"])?;
    fs::write(
        repository.path().join(".neoengramignore"),
        "*.bin\ncache/\n!important.bin\n!cache/README.txt\n",
    )?;
    fs::write(repository.path().join("ignored.bin"), b"ignored")?;
    fs::write(repository.path().join("important.bin"), b"kept")?;
    fs::create_dir(repository.path().join("cache"))?;
    fs::write(repository.path().join("cache/model.bin"), b"ignored")?;
    fs::write(repository.path().join("cache/README.txt"), b"kept")?;
    fs::write(repository.path().join("notes.txt"), b"tracked")?;

    run_success(repository.path(), &["add", "."])?;
    let index = read_index_paths(repository.path())?;
    assert!(index.iter().any(|path| path == ".neoengramignore"));
    assert!(index.iter().any(|path| path == "important.bin"));
    assert!(index.iter().any(|path| path == "cache/README.txt"));
    assert!(index.iter().any(|path| path == "notes.txt"));
    assert!(!index.iter().any(|path| path == "ignored.bin"));
    assert!(!index.iter().any(|path| path == "cache/model.bin"));

    let status = run_success(repository.path(), &["status"])?;
    let status = String::from_utf8(status.stdout)?;
    assert!(status.contains("Changes to be committed:"));
    assert!(!status.contains("Untracked files:"));
    assert!(!status.contains("ignored.bin"));
    assert!(!status.contains("cache/model.bin"));
    Ok(())
}

#[test]
fn ignored_tracked_files_still_update_and_delete_with_add_all() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init", "--metadata-store", "json"])?;
    let tracked = repository.path().join("model.bin");
    fs::write(&tracked, b"v1")?;
    run_success(repository.path(), &["add", "model.bin"])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;

    fs::write(repository.path().join(".neoengramignore"), "model.bin\n")?;
    fs::write(&tracked, b"v2")?;
    run_success(repository.path(), &["add", "-A", "."])?;
    let status = String::from_utf8(run_success(repository.path(), &["status"])?.stdout)?;
    assert!(status.contains("modified: model.bin"));

    fs::remove_file(&tracked)?;
    run_success(repository.path(), &["add", "-A", "."])?;
    let index = read_index_paths(repository.path())?;
    assert!(!index.iter().any(|path| path == "model.bin"));
    Ok(())
}

#[test]
fn ignore_rules_work_with_the_default_sqlite_backend() -> TestResult {
    let repository = tempfile::tempdir()?;
    run_success(repository.path(), &["init"])?;
    fs::write(repository.path().join(".neoengramignore"), "*.tmp\n")?;
    fs::write(repository.path().join("ignored.tmp"), b"ignored")?;
    fs::write(repository.path().join("kept.txt"), b"kept")?;

    run_success(repository.path(), &["add", "."])?;
    run_success(repository.path(), &["commit", "-m", "base"])?;
    fs::write(repository.path().join("kept.txt"), b"changed")?;
    let status = String::from_utf8(run_success(repository.path(), &["status"])?.stdout)?;
    assert!(!status.contains("ignored.tmp"));
    assert!(status.contains("kept.txt"));
    Ok(())
}

fn read_index_paths(repository: &Path) -> Result<Vec<String>, Box<dyn Error>> {
    let index: serde_json::Value = serde_json::from_slice(&fs::read(
        repository.join(".neoengram/metadata/index.json"),
    )?)?;
    Ok(index["files"]
        .as_array()
        .ok_or("index files is not an array")?
        .iter()
        .map(|file| {
            file["path"]
                .as_str()
                .map(str::to_owned)
                .ok_or("index path is not a string")
        })
        .collect::<Result<Vec<_>, _>>()?)
}

fn run_success(
    repository: &Path,
    arguments: &[&str],
) -> Result<std::process::Output, Box<dyn Error>> {
    let output = command(repository).args(arguments).output()?;
    if !output.status.success() {
        return Err(format!(
            "neoengram {} failed:\n{}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output)
}

fn command(current_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_neoengram"));
    command.current_dir(current_dir);
    command
}
