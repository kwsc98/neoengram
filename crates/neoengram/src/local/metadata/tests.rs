use std::fs;

use anyhow::{ensure, Result};
use neoengram_core::{Chunk, Commit, FileNode, Index, INDEX_FORMAT_VERSION};

use super::{
    describe_manifest, file_manifest_id, legacy_tree_records_id, tree_records_id, FileRecord,
    FileSetReader, HeadCas, HeadState, IndexTxn, JsonMetadataStore, MetadataStore, PageRequest,
    ReferenceCas, SqliteMetadataStore, StoredReference, TreeWriter, MAX_PAGE_SIZE,
};

#[test]
fn manifest_ids_are_stable() -> Result<()> {
    let chunks = [
        Chunk {
            hash: "1".repeat(64),
            offset: 0,
            size: 1,
        },
        Chunk {
            hash: "2".repeat(64),
            offset: 1,
            size: 2,
        },
    ];
    assert_eq!(
        (file_manifest_id(0, &[])?, file_manifest_id(3, &chunks)?),
        (
            "ef41bc2c060f2113b2c52bdab58fadbce1cf93c48b4400fcdb82977bf164960e".to_owned(),
            "7f5d26a848edc1d6161c15a5d40e331f27a5781ed384c82a58bc19b07f35f9f1".to_owned()
        )
    );
    Ok(())
}

#[test]
fn streaming_tree_ids_match_legacy_serialization() -> Result<()> {
    let records = vec![
        FileRecord {
            path: "models/a%_quoted.bin".to_owned(),
            total_size: 3,
            chunk_count: 2,
            manifest_id: "1".repeat(64),
        },
        FileRecord {
            path: "weights/z.bin".to_owned(),
            total_size: u64::MAX,
            chunk_count: u64::MAX,
            manifest_id: "f".repeat(64),
        },
    ];

    assert_eq!(
        tree_records_id(&records)?,
        legacy_tree_records_id(&records)?
    );
    Ok(())
}

#[test]
fn json_backend_satisfies_metadata_store_contract() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let store = JsonMetadataStore::new(root.clone());
    let (tree_id, commit_id) = exercise_contract(&store)?;
    for path in [
        root.join("index.json"),
        root.join(format!("trees/{tree_id}.json")),
    ] {
        let json = fs::read_to_string(path)?;
        assert!(json.contains("\"manifest_id\""));
        assert!(!json.contains("\"chunks\""));
    }

    // 新实例模拟进程重启；所有状态必须来自持久后端，而不是内存缓存。
    let reopened = JsonMetadataStore::new(root);
    reopened.validate_layout()?;
    assert_eq!(
        collect_files(&reopened, reopened.read_index()?.as_ref())?[0].path,
        "dir/model.bin"
    );
    let snapshot = reopened.snapshot()?;
    assert_eq!(
        snapshot.scan_tree_ids(&PageRequest::first(1)?)?.items,
        vec![tree_id]
    );
    assert_eq!(
        snapshot.scan_commit_ids(&PageRequest::first(1)?)?.items,
        vec![commit_id.clone()]
    );
    assert_eq!(snapshot.get_reference("refs/heads/main")?, Some(commit_id));
    assert_eq!(
        snapshot.read_head_state()?,
        HeadState::Detached {
            commit_id: "b".repeat(64)
        }
    );
    Ok(())
}

#[test]
fn sqlite_backend_satisfies_metadata_store_contract() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let store = SqliteMetadataStore::new(root.clone());
    let (tree_id, commit_id) = exercise_contract(&store)?;

    // 新实例模拟进程重启；所有状态必须来自 SQLite，而不是连接内缓存。
    let reopened = SqliteMetadataStore::new(root);
    reopened.validate_layout()?;
    assert_eq!(
        collect_files(&reopened, reopened.read_index()?.as_ref())?[0].path,
        "dir/model.bin"
    );
    let snapshot = reopened.snapshot()?;
    assert_eq!(
        snapshot.scan_tree_ids(&PageRequest::first(1)?)?.items,
        vec![tree_id]
    );
    assert_eq!(
        snapshot.scan_commit_ids(&PageRequest::first(1)?)?.items,
        vec![commit_id.clone()]
    );
    assert_eq!(snapshot.get_reference("refs/heads/main")?, Some(commit_id));
    assert_eq!(
        snapshot.read_head_state()?,
        HeadState::Detached {
            commit_id: "b".repeat(64)
        }
    );
    Ok(())
}

#[test]
fn json_backend_preserves_fixed_views_and_literal_prefixes() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let store = JsonMetadataStore::new(temporary.path().join("metadata"));
    exercise_fixed_views_and_literal_prefixes(&store)
}

#[test]
fn sqlite_backend_preserves_fixed_views_and_literal_prefixes() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let store = SqliteMetadataStore::new(temporary.path().join("metadata"));
    exercise_fixed_views_and_literal_prefixes(&store)
}

#[test]
fn sqlite_backend_rejects_unknown_schema_version() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let store = SqliteMetadataStore::new(root.clone());
    store.initialize("refs/heads/main")?;

    let connection = rusqlite::Connection::open(root.join("metadata.sqlite3"))?;
    connection.pragma_update(None, "user_version", 999_i64)?;
    drop(connection);

    let reopened = SqliteMetadataStore::new(root);
    assert!(reopened.validate_layout().is_err());
    assert!(reopened.read_index().is_err());
    Ok(())
}

#[test]
fn sqlite_backend_rejects_foreign_database_without_mutating_it() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let database = root.join("metadata.sqlite3");
    fs::create_dir(&root)?;

    let connection = rusqlite::Connection::open(&database)?;
    connection.execute_batch("CREATE VIEW user_view AS SELECT 1 AS value")?;
    let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    assert_eq!(journal_mode, "delete");
    drop(connection);
    let before = fs::read(&database)?;

    let store = SqliteMetadataStore::new(root.clone());
    let error = store
        .initialize("refs/heads/main")
        .expect_err("已有外部 SQLite 对象时必须拒绝初始化");
    assert!(error
        .to_string()
        .contains("拒绝把已有 SQLite 数据库初始化为 NeoEngram 元数据库"));
    assert_eq!(fs::read(&database)?, before);
    assert!(!root.join("metadata.sqlite3-wal").exists());
    assert!(!root.join("metadata.sqlite3-shm").exists());

    let connection = rusqlite::Connection::open(database)?;
    let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    assert_eq!(journal_mode, "delete");
    let view_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'view' AND name = 'user_view'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(view_count, 1);
    Ok(())
}

#[test]
fn sqlite_tree_writer_drop_and_duplicate_finish_clean_staging_sets() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let database = root.join("metadata.sqlite3");
    let store = SqliteMetadataStore::new(root);
    store.initialize("refs/heads/main")?;

    let writer = store.begin_tree_write()?;
    let connection = rusqlite::Connection::open(&database)?;
    let staging_count: i64 =
        connection.query_row("SELECT COUNT(*) FROM tree_sets", [], |row| row.get(0))?;
    assert_eq!(staging_count, 1);
    drop(writer);
    let staging_count: i64 =
        connection.query_row("SELECT COUNT(*) FROM tree_sets", [], |row| row.get(0))?;
    assert_eq!(staging_count, 0);

    let first_id = store.begin_tree_write()?.finish()?;
    let duplicate_id = store.begin_tree_write()?.finish()?;
    assert_eq!(duplicate_id, first_id);
    let published_set_count: i64 =
        connection.query_row("SELECT COUNT(*) FROM tree_sets", [], |row| row.get(0))?;
    assert_eq!(published_set_count, 1);
    Ok(())
}

#[cfg(unix)]
#[test]
fn sqlite_backend_rejects_a_symlinked_database() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let database = root.join("metadata.sqlite3");
    let outside = temporary.path().join("outside.sqlite3");
    let store = SqliteMetadataStore::new(root.clone());
    store.initialize("refs/heads/main")?;
    fs::copy(&database, &outside)?;
    let outside_before = fs::read(&outside)?;
    fs::remove_file(&database)?;
    symlink(&outside, &database)?;

    let reopened = SqliteMetadataStore::new(root);
    assert!(reopened.validate_layout().is_err());
    assert!(reopened.read_index().is_err());
    assert_eq!(fs::read(outside)?, outside_before);
    Ok(())
}

fn exercise_fixed_views_and_literal_prefixes(store: &dyn MetadataStore) -> Result<()> {
    store.initialize("refs/heads/main")?;
    let fixed_index = store.read_index()?;
    let fixed_index_version = fixed_index.version().clone();
    let fixed_snapshot = store.snapshot()?;

    let chunks = [
        Chunk {
            hash: "3".repeat(64),
            offset: 0,
            size: 1,
        },
        Chunk {
            hash: "4".repeat(64),
            offset: 1,
            size: 2,
        },
    ];
    let mut chunk_input = chunks.iter().cloned().map(Ok);
    let manifest = store.put_manifest(3, &mut chunk_input)?;
    ensure!(
        chunk_input.next().is_none(),
        "Manifest 写入必须消费完整 Iterator"
    );
    let manifest_reader = store
        .open_manifest(&manifest.id)?
        .expect("刚发布的 Manifest 应存在");
    let first_chunk_page = manifest_reader.scan_chunks(&PageRequest::first(1)?)?;
    assert_eq!(first_chunk_page.items, vec![chunks[0].clone()]);
    let second_chunk_page =
        manifest_reader.scan_chunks(&PageRequest::new(first_chunk_page.next, 1)?)?;
    assert_eq!(second_chunk_page.items, vec![chunks[1].clone()]);
    assert_eq!(second_chunk_page.next, None);
    assert!(manifest_reader
        .scan_chunks(&PageRequest::new(Some("not-a-cursor".to_owned()), 1)?)
        .is_err());
    assert!(manifest_reader
        .scan_chunks(&PageRequest::new(Some("2".to_owned()), 1)?)
        .is_err());

    let mut records = [
        "percent%",
        "percent%/child",
        "percentX/other",
        "under_",
        "under_/child",
        "underX/other",
    ]
    .into_iter()
    .map(|path| FileRecord::from_manifest(path.to_owned(), 3, &manifest))
    .collect::<Vec<_>>();
    records.sort_unstable_by(|left, right| left.path.cmp(&right.path));

    let mut transaction = store.begin_index_transaction(Some(&fixed_index_version))?;
    for record in &records {
        transaction.upsert_file(record)?;
    }
    assert_eq!(
        collect_record_paths(transaction.as_ref(), Some("percent%"))?,
        vec!["percent%".to_owned(), "percent%/child".to_owned()]
    );
    assert_eq!(
        collect_record_paths(transaction.as_ref(), Some("under_"))?,
        vec!["under_".to_owned(), "under_/child".to_owned()]
    );
    let current_version = transaction.commit()?;
    assert_ne!(current_version, fixed_index_version);

    assert_eq!(fixed_index.version(), &fixed_index_version);
    assert!(fixed_index
        .scan_files(None, &PageRequest::first(1)?)?
        .items
        .is_empty());
    let current_index = store.read_index()?;
    assert_eq!(
        collect_record_paths(current_index.as_ref(), Some("percent%"))?,
        vec!["percent%".to_owned(), "percent%/child".to_owned()]
    );
    assert_eq!(
        collect_record_paths(current_index.as_ref(), Some("under_"))?,
        vec!["under_".to_owned(), "under_/child".to_owned()]
    );

    let dropped_tree_id = tree_records_id(&records[..1])?;
    let mut dropped_writer = store.begin_tree_write()?;
    dropped_writer.append_file(&records[0])?;
    drop(dropped_writer);
    assert!(store.snapshot()?.open_tree(&dropped_tree_id)?.is_none());

    let mut unordered_writer = store.begin_tree_write()?;
    unordered_writer.append_file(records.last().expect("records 非空"))?;
    assert!(unordered_writer.append_file(&records[0]).is_err());
    drop(unordered_writer);
    assert!(store
        .snapshot()?
        .scan_tree_ids(&PageRequest::first(1)?)?
        .items
        .is_empty());

    let mut writer = store.begin_tree_write()?;
    for record in &records {
        writer.append_file(record)?;
    }
    let tree_id = writer.finish()?;
    let commit_id = "d".repeat(64);
    let commit = Commit {
        tree_hash: tree_id.clone(),
        parent: None,
        message: "fixed snapshot boundary".to_owned(),
        created_at_unix_ms: 2,
    };
    store.put_commit(&commit_id, &commit)?;

    assert!(fixed_snapshot
        .scan_manifest_ids(&PageRequest::first(1)?)?
        .items
        .is_empty());
    assert!(fixed_snapshot.open_manifest(&manifest.id)?.is_none());
    assert!(fixed_snapshot
        .scan_tree_ids(&PageRequest::first(1)?)?
        .items
        .is_empty());
    assert!(fixed_snapshot.open_tree(&tree_id)?.is_none());
    assert!(fixed_snapshot
        .scan_commit_ids(&PageRequest::first(1)?)?
        .items
        .is_empty());
    assert_eq!(fixed_snapshot.get_commit(&commit_id)?, None);

    let current_snapshot = store.snapshot()?;
    assert_eq!(
        current_snapshot
            .scan_manifest_ids(&PageRequest::first(1)?)?
            .items,
        vec![manifest.id]
    );
    assert_eq!(
        current_snapshot
            .scan_tree_ids(&PageRequest::first(1)?)?
            .items,
        vec![tree_id]
    );
    assert_eq!(
        current_snapshot
            .scan_commit_ids(&PageRequest::first(1)?)?
            .items,
        vec![commit_id]
    );
    Ok(())
}

fn collect_record_paths(reader: &dyn FileSetReader, prefix: Option<&str>) -> Result<Vec<String>> {
    let mut request = PageRequest::first(1)?;
    let mut paths = Vec::new();
    loop {
        let page = reader.scan_files(prefix, &request)?;
        paths.extend(page.items.into_iter().map(|record| record.path));
        let Some(next) = page.next else {
            break;
        };
        request.after = Some(next);
    }
    Ok(paths)
}

fn exercise_contract(store: &dyn MetadataStore) -> Result<(String, String)> {
    let initial = Index::default();
    store.initialize("refs/heads/main")?;
    store.initialize("refs/heads/main")?;
    store.validate_layout()?;
    store.verify_integrity()?;
    let initial_reader = store.read_index()?;
    assert_eq!(
        collect_files(store, initial_reader.as_ref())?,
        initial.files
    );
    let initial_version = initial_reader.version().clone();
    let initial_snapshot = store.snapshot()?;
    assert_eq!(
        initial_snapshot.read_head_state()?,
        HeadState::Attached {
            reference: "refs/heads/main".to_owned()
        }
    );
    assert_eq!(initial_snapshot.get_reference("refs/heads/main")?, None);
    assert!(initial_snapshot.open_tree("../escape").is_err());
    assert!(store
        .compare_exchange_reference("../escape", None, Some("target"))
        .is_err());
    assert!(PageRequest::new(None, MAX_PAGE_SIZE + 1).is_err());

    let valid_chunk = Chunk {
        hash: "f".repeat(64),
        offset: 0,
        size: 1,
    };
    let mut interrupted = vec![
        Ok(valid_chunk.clone()),
        Err(anyhow::anyhow!("injected manifest input failure")),
    ]
    .into_iter();
    assert!(store.put_manifest(1, &mut interrupted).is_err());
    let mut wrong_size = vec![Ok(valid_chunk)].into_iter();
    assert!(store.put_manifest(2, &mut wrong_size).is_err());
    assert!(store
        .snapshot()?
        .scan_manifest_ids(&PageRequest::first(1)?)?
        .items
        .is_empty());

    let index = Index {
        format_version: INDEX_FORMAT_VERSION,
        files: vec![
            FileNode {
                path: "dir/model.bin".to_owned(),
                total_size: 3,
                chunks: vec![
                    Chunk {
                        hash: "1".repeat(64),
                        offset: 0,
                        size: 1,
                    },
                    Chunk {
                        hash: "2".repeat(64),
                        offset: 1,
                        size: 2,
                    },
                ],
            },
            empty_node("notes.txt"),
        ],
    };

    let mut transaction = store.begin_index_transaction(Some(&initial_version))?;
    let missing_manifest = FileRecord {
        path: "missing.bin".to_owned(),
        total_size: 0,
        chunk_count: 0,
        manifest_id: "a".repeat(64),
    };
    assert!(transaction.upsert_file(&missing_manifest).is_err());
    for file in &index.files {
        upsert_file(store, transaction.as_mut(), file)?;
    }
    let valid_record = record_from_node(&index.files[0])?;
    let mismatched_size = FileRecord {
        path: "bad-size.bin".to_owned(),
        total_size: valid_record.total_size + 1,
        ..valid_record.clone()
    };
    let mismatched_count = FileRecord {
        path: "bad-count.bin".to_owned(),
        chunk_count: valid_record.chunk_count + 1,
        ..valid_record
    };
    assert!(transaction.upsert_file(&mismatched_size).is_err());
    assert!(transaction.upsert_file(&mismatched_count).is_err());
    let populated_version = transaction.commit()?;
    assert_ne!(populated_version, initial_version);
    assert!(store
        .begin_index_transaction(Some(&initial_version))
        .is_err());
    let reader = store.read_index()?;
    assert_eq!(collect_files(store, reader.as_ref())?, index.files);
    let first_page = reader.scan_files(None, &PageRequest::first(1)?)?;
    assert_eq!(first_page.items.len(), 1);
    ensure!(
        first_page.next.is_some(),
        "两项 Index 的第一页必须有 cursor"
    );
    assert_eq!(
        reader
            .scan_files(None, &PageRequest::new(first_page.next.clone(), 1)?,)?
            .items[0]
            .path,
        "notes.txt"
    );
    assert_eq!(
        reader
            .scan_files(Some("dir"), &PageRequest::first(1)?)?
            .items
            .len(),
        1
    );

    let mut expected_manifest_ids = index
        .files
        .iter()
        .map(record_from_node)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|file| file.manifest_id)
        .collect::<Vec<_>>();
    expected_manifest_ids.sort();
    let manifest_snapshot = store.snapshot()?;
    let first_manifest_page = manifest_snapshot.scan_manifest_ids(&PageRequest::first(1)?)?;
    assert_eq!(
        first_manifest_page.items,
        vec![expected_manifest_ids[0].clone()]
    );
    let manifest_cursor = first_manifest_page
        .next
        .expect("两个 Manifest 的第一页必须有 cursor");
    assert_eq!(manifest_cursor, expected_manifest_ids[0]);
    let second_manifest_page =
        manifest_snapshot.scan_manifest_ids(&PageRequest::new(Some(manifest_cursor), 1)?)?;
    assert_eq!(
        second_manifest_page.items,
        vec![expected_manifest_ids[1].clone()]
    );
    assert_eq!(second_manifest_page.next, None);

    let mut transaction = store.begin_index_transaction(Some(&populated_version))?;
    assert_eq!(transaction.delete_prefix("")?, 2);
    let empty_version = transaction.commit()?;
    assert_ne!(empty_version, populated_version);
    assert!(collect_files(store, store.read_index()?.as_ref())?.is_empty());

    let mut transaction = store.begin_index_transaction(Some(&empty_version))?;
    for file in &index.files {
        upsert_file(store, transaction.as_mut(), file)?;
    }
    let restored_version = transaction.commit()?;
    assert_ne!(
        restored_version, populated_version,
        "内容恢复为旧值后 revision 仍必须前进，避免 ABA"
    );
    assert_eq!(
        collect_files(store, store.read_index()?.as_ref())?,
        index.files
    );

    let mut abandoned = store.begin_index_transaction(Some(&restored_version))?;
    abandoned.delete_prefix("")?;
    drop(abandoned);
    assert_eq!(store.read_index()?.version(), &restored_version);
    assert_eq!(
        collect_files(store, store.read_index()?.as_ref())?,
        index.files
    );

    let expected_tree_id = tree_records_id(
        &index
            .files
            .iter()
            .map(record_from_node)
            .collect::<Result<Vec<_>>>()?,
    )?;
    let mut writer = store.begin_tree_write()?;
    assert!(writer.append_file(&missing_manifest).is_err());
    assert!(writer.append_file(&mismatched_size).is_err());
    assert!(writer.append_file(&mismatched_count).is_err());
    for file in &index.files {
        append_file(writer.as_mut(), file)?;
    }
    let tree_id = writer.finish()?;
    assert_eq!(
        tree_id, expected_tree_id,
        "TreeWriter 必须返回 Repository 使用的规范 Tree ID"
    );
    let mut writer = store.begin_tree_write()?;
    for file in &index.files {
        append_file(writer.as_mut(), file)?;
    }
    assert_eq!(writer.finish()?, tree_id);
    let snapshot = store.snapshot()?;
    let tree = snapshot.open_tree(&tree_id)?.expect("Tree 应存在");
    assert_eq!(tree.file_count(), 2);
    assert_eq!(collect_files(store, tree.as_ref())?, index.files);
    assert_eq!(
        snapshot.scan_tree_ids(&PageRequest::first(1)?)?.items,
        vec![tree_id.clone()]
    );
    let commit_id = "b".repeat(64);
    let commit = Commit {
        tree_hash: tree_id.clone(),
        parent: None,
        message: "snapshot".to_owned(),
        created_at_unix_ms: 1,
    };
    assert_eq!(store.snapshot()?.get_commit(&commit_id)?, None);
    store.put_commit(&commit_id, &commit)?;
    store.put_commit(&commit_id, &commit)?;
    let snapshot = store.snapshot()?;
    assert_eq!(snapshot.get_commit(&commit_id)?, Some(commit.clone()));
    assert_eq!(
        snapshot.scan_commit_ids(&PageRequest::first(1)?)?.items,
        vec![commit_id.clone()]
    );
    let conflicting = Commit {
        message: "different".to_owned(),
        ..commit.clone()
    };
    assert!(store.put_commit(&commit_id, &conflicting).is_err());
    assert_eq!(store.snapshot()?.get_commit(&commit_id)?, Some(commit));

    assert_eq!(
        store.compare_exchange_reference("refs/heads/main", None, Some(&commit_id))?,
        ReferenceCas::Updated
    );

    let attached = HeadState::Attached {
        reference: "refs/heads/main".to_owned(),
    };
    let detached = HeadState::Detached {
        commit_id: commit_id.clone(),
    };
    let fixed_head_snapshot = store.snapshot()?;
    assert_eq!(
        store.compare_exchange_head(&attached, &detached)?,
        HeadCas::Updated
    );
    assert_eq!(store.read_head_state()?, detached);
    assert_eq!(fixed_head_snapshot.read_head_state()?, attached);
    assert_eq!(
        store.compare_exchange_head(
            &HeadState::Attached {
                reference: "refs/heads/main".to_owned(),
            },
            &HeadState::Detached {
                commit_id: "c".repeat(64),
            },
        )?,
        HeadCas::Mismatch {
            actual: HeadState::Detached {
                commit_id: commit_id.clone(),
            }
        }
    );
    assert_eq!(
        store.compare_exchange_head(
            &HeadState::Detached {
                commit_id: commit_id.clone(),
            },
            &HeadState::Attached {
                reference: "refs/heads/main".to_owned(),
            },
        )?,
        HeadCas::Updated
    );
    assert_eq!(
        store.compare_exchange_head(
            &HeadState::Attached {
                reference: "refs/heads/main".to_owned(),
            },
            &HeadState::Detached {
                commit_id: commit_id.clone(),
            },
        )?,
        HeadCas::Updated
    );
    assert!(store
        .compare_exchange_head(
            &HeadState::Detached {
                commit_id: commit_id.clone(),
            },
            &HeadState::Detached {
                commit_id: "invalid".to_owned(),
            },
        )
        .is_err());
    let replacement_id = "c".repeat(64);
    assert_eq!(
        store.compare_exchange_reference("refs/heads/main", None, Some(&replacement_id))?,
        ReferenceCas::Mismatch {
            actual: Some(commit_id.clone())
        }
    );
    let fixed_snapshot = store.snapshot()?;
    assert_eq!(
        store.compare_exchange_reference(
            "refs/heads/main",
            Some(&commit_id),
            Some(&replacement_id),
        )?,
        ReferenceCas::Updated
    );
    assert_eq!(
        fixed_snapshot.get_reference("refs/heads/main")?,
        Some(commit_id.clone())
    );
    assert_eq!(
        store.compare_exchange_reference("refs/heads/main", Some(&replacement_id), None,)?,
        ReferenceCas::Updated
    );
    assert_eq!(store.snapshot()?.get_reference("refs/heads/main")?, None);
    assert_eq!(
        store.compare_exchange_reference("refs/heads/main", None, Some(&commit_id))?,
        ReferenceCas::Updated
    );

    // initialize 必须幂等，已有的可变和不可变状态都不能被默认值覆盖。
    store.initialize("refs/heads/main")?;
    assert_eq!(
        store.read_head_state()?,
        HeadState::Detached {
            commit_id: commit_id.clone()
        }
    );
    assert_eq!(
        collect_files(store, store.read_index()?.as_ref())?,
        index.files
    );
    let snapshot = store.snapshot()?;
    assert!(snapshot.open_tree(&tree_id)?.is_some());
    assert!(snapshot.get_commit(&commit_id)?.is_some());
    assert_eq!(
        snapshot
            .scan_references("refs/heads", &PageRequest::first(1)?)?
            .items,
        vec![StoredReference {
            name: "refs/heads/main".to_owned(),
            target: commit_id.clone(),
        }]
    );
    store.verify_integrity()?;
    Ok((tree_id, commit_id))
}

#[cfg(unix)]
#[test]
fn json_backend_rejects_a_symlinked_index() -> Result<()> {
    use std::{fs, os::unix::fs::symlink};

    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let outside = temporary.path().join("outside.json");
    let store = JsonMetadataStore::new(root.clone());
    store.initialize("refs/heads/main")?;
    fs::write(&outside, b"outside sentinel")?;
    fs::remove_file(root.join("index.json"))?;
    symlink(&outside, root.join("index.json"))?;

    assert!(store.read_index().is_err());
    assert!(store.begin_index_transaction(None).is_err());
    assert_eq!(fs::read(outside)?, b"outside sentinel");
    Ok(())
}

#[test]
fn json_backend_ignores_unpublished_atomic_temp_files() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let store = JsonMetadataStore::new(root.clone());
    store.initialize("refs/heads/main")?;

    for path in [
        root.join("trees/.neoengram-tmp-orphan"),
        root.join("commits/.neoengram-tmp-orphan"),
        root.join("refs/heads/.neoengram-tmp-orphan"),
    ] {
        fs::write(path, b"unpublished")?;
    }

    let snapshot = store.snapshot()?;
    assert!(snapshot
        .scan_tree_ids(&PageRequest::first(1)?)?
        .items
        .is_empty());
    assert!(snapshot
        .scan_commit_ids(&PageRequest::first(1)?)?
        .items
        .is_empty());
    assert!(snapshot
        .scan_references("refs", &PageRequest::first(1)?)?
        .items
        .is_empty());
    assert!(store
        .compare_exchange_reference(
            "refs/heads/.neoengram-tmp-reserved",
            None,
            Some(&"a".repeat(64)),
        )
        .is_err());
    store.verify_integrity()?;
    Ok(())
}

#[test]
fn json_backend_does_not_hide_non_file_temp_entries() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let store = JsonMetadataStore::new(root.clone());
    store.initialize("refs/heads/main")?;
    fs::create_dir(root.join("trees/.neoengram-tmp-not-a-file"))?;

    assert!(store.snapshot().is_err());
    assert!(store.verify_integrity().is_err());
    Ok(())
}

#[test]
fn json_backend_rejects_unsorted_file_sets_before_paging() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let store = JsonMetadataStore::new(root.clone());
    store.initialize("refs/heads/main")?;
    let unsorted_records = vec![empty_record("z.bin")?, empty_record("a.bin")?];

    fs::write(
        root.join("index.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": INDEX_FORMAT_VERSION,
            "files": &unsorted_records,
            "revision": 0
        }))?,
    )?;
    assert!(store.read_index().is_err());
    fs::write(
        root.join("index.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": INDEX_FORMAT_VERSION,
            "files": [],
            "revision": 1
        }))?,
    )?;

    let tree_id = tree_records_id(&unsorted_records)?;
    fs::write(
        root.join(format!("trees/{tree_id}.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "files": &unsorted_records
        }))?,
    )?;
    assert!(store.snapshot()?.open_tree(&tree_id).is_err());
    Ok(())
}

#[test]
fn json_backend_rejects_previous_index_format_and_legacy_chunks() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("metadata");
    let store = JsonMetadataStore::new(root.clone());
    store.initialize("refs/heads/main")?;

    fs::write(
        root.join("index.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": INDEX_FORMAT_VERSION - 1,
            "revision": 0,
            "files": []
        }))?,
    )?;
    assert!(store.read_index().is_err());

    let record = empty_record("legacy.bin")?;
    fs::write(
        root.join("index.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": INDEX_FORMAT_VERSION,
            "revision": 0,
            "files": [{
                "path": record.path,
                "total_size": record.total_size,
                "chunk_count": record.chunk_count,
                "manifest_id": record.manifest_id,
                "chunks": []
            }]
        }))?,
    )?;
    assert!(store.read_index().is_err());
    Ok(())
}

fn collect_files<S: MetadataStore + ?Sized>(
    store: &S,
    reader: &dyn FileSetReader,
) -> Result<Vec<FileNode>> {
    let mut request = PageRequest::first(1)?;
    let mut files = Vec::new();
    loop {
        let page = reader.scan_files(None, &request)?;
        for record in page.items {
            let manifest = store
                .open_manifest(&record.manifest_id)?
                .expect("Manifest 应存在");
            assert_eq!(manifest.total_size(), record.total_size);
            assert_eq!(manifest.chunk_count(), record.chunk_count);
            let mut chunk_request = PageRequest::first(1)?;
            let mut chunks = Vec::new();
            loop {
                let chunk_page = manifest.scan_chunks(&chunk_request)?;
                chunks.extend(chunk_page.items);
                let Some(next) = chunk_page.next else {
                    break;
                };
                chunk_request.after = Some(next);
            }
            assert_eq!(u64::try_from(chunks.len())?, record.chunk_count);
            files.push(FileNode {
                path: record.path,
                total_size: record.total_size,
                chunks,
            });
        }
        let Some(next) = page.next else {
            break;
        };
        request.after = Some(next);
    }
    Ok(files)
}

fn empty_node(path: &str) -> FileNode {
    FileNode {
        path: path.to_owned(),
        total_size: 0,
        chunks: Vec::new(),
    }
}

fn empty_record(path: &str) -> Result<FileRecord> {
    record_from_node(&empty_node(path))
}

fn upsert_file(
    store: &dyn MetadataStore,
    transaction: &mut dyn IndexTxn,
    file: &FileNode,
) -> Result<()> {
    let mut chunks = file.chunks.iter().cloned().map(Ok);
    let manifest = store.put_manifest(file.total_size, &mut chunks)?;
    ensure!(
        chunks.next().is_none(),
        "Manifest 写入必须消费完整 Iterator"
    );
    let record = FileRecord::from_manifest(file.path.clone(), file.total_size, &manifest);
    ensure!(
        record == record_from_node(file)?,
        "Manifest 写入返回了不一致的内容引用"
    );
    transaction.upsert_file(&record)
}

fn append_file(writer: &mut dyn TreeWriter, file: &FileNode) -> Result<()> {
    let record = record_from_node(file)?;
    writer.append_file(&record)
}

fn record_from_node(file: &FileNode) -> Result<FileRecord> {
    let manifest = describe_manifest(file.total_size, &file.chunks)?;
    Ok(FileRecord::from_manifest(
        file.path.clone(),
        file.total_size,
        &manifest,
    ))
}
