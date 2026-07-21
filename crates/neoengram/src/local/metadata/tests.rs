use std::{fs, num::NonZeroU32};

use anyhow::{anyhow, Result};
use neoengram_core::{Chunk, Commit, DirectoryEntry, DirectoryEntryKind};
use rusqlite::Connection;

use super::{
    describe_manifest, DirectoryHasher, FileRecord, HeadState, MetadataReader, MetadataStore,
    PageRequest, ReferenceCas, SqliteMetadataStore,
};

fn initialized_store() -> Result<(tempfile::TempDir, SqliteMetadataStore)> {
    let temporary = tempfile::tempdir()?;
    let store = SqliteMetadataStore::new(temporary.path().join("metadata"));
    store.initialize("refs/heads/main")?;
    Ok((temporary, store))
}

#[test]
fn canonical_ids_are_stable() -> Result<()> {
    let empty = describe_manifest(0, &[])?;
    assert_eq!(
        empty.id,
        "9eff55768acf310e3bb2537318fad275e4436d05b8ed2200efba65c4699cff04"
    );

    let chunks = vec![Chunk {
        hash: blake3::hash(b"abc").to_hex().to_string(),
        offset: 0,
        size: 3,
    }];
    let manifest = describe_manifest(3, &chunks)?;
    assert_eq!(
        manifest.id,
        "44d8d8d722506be91dee47bd6830686cd2dc77b335b0f5bafbd0cb0277bec6ae"
    );

    let mut hasher = DirectoryHasher::new();
    hasher.push(&DirectoryEntry {
        ordinal: 0,
        name: "payload.bin".to_owned(),
        kind: DirectoryEntryKind::File,
        target_id: manifest.id,
        total_size: 3,
    })?;
    assert_eq!(
        hasher.finish().0,
        "1c71f3be7c6ea6f3b80bd7817e4297d33b6277cf02323f0468e31e8ed81f95fd"
    );
    Ok(())
}

#[test]
fn directory_names_must_be_strictly_sorted() -> Result<()> {
    let target = describe_manifest(0, &[])?.id;
    let mut hasher = DirectoryHasher::new();
    hasher.push(&DirectoryEntry {
        ordinal: 0,
        name: "z".to_owned(),
        kind: DirectoryEntryKind::File,
        target_id: target.clone(),
        total_size: 0,
    })?;
    assert!(hasher
        .push(&DirectoryEntry {
            ordinal: 1,
            name: "a".to_owned(),
            kind: DirectoryEntryKind::File,
            target_id: target,
            total_size: 0,
        })
        .is_err());
    Ok(())
}

#[test]
fn sqlite_directory_and_manifest_ranges_are_paged() -> Result<()> {
    let (_temporary, store) = initialized_store()?;
    let first = Chunk {
        hash: blake3::hash(b"abc").to_hex().to_string(),
        offset: 0,
        size: 3,
    };
    let second = Chunk {
        hash: blake3::hash(b"def").to_hex().to_string(),
        offset: 3,
        size: 3,
    };
    let manifest = store.put_manifest(6, &mut vec![Ok(first), Ok(second.clone())].into_iter())?;
    let reader = store.open_manifest(&manifest.id)?.expect("manifest");
    let page = reader.scan_chunks_from_offset(4, NonZeroU32::new(1).expect("nonzero"))?;
    assert_eq!(page.items, vec![second]);
    let first_page = reader.scan_chunks_from_offset(0, NonZeroU32::new(1).expect("nonzero"))?;
    assert_eq!(first_page.items[0].offset, 0);
    assert_eq!(first_page.next.as_deref(), Some("3"));
    let second_page = reader.scan_chunks_from_offset(
        first_page.next.expect("range continuation").parse()?,
        NonZeroU32::new(1).expect("nonzero"),
    )?;
    assert_eq!(second_page.items[0].offset, 3);

    let mut writer = store.begin_directory_write()?;
    writer.append_entry(&DirectoryEntry {
        ordinal: 0,
        name: "payload.bin".to_owned(),
        kind: DirectoryEntryKind::File,
        target_id: manifest.id,
        total_size: 6,
    })?;
    let directory = writer.finish()?;
    assert_eq!(directory.file_count, 1);
    assert_eq!(directory.total_size, 6);
    let reader = store.open_directory(&directory.id)?.expect("directory");
    assert_eq!(reader.scan_entries(&PageRequest::first(1)?)?.items.len(), 1);
    assert!(reader.get_entry("payload.bin")?.is_some());
    Ok(())
}

#[test]
fn dropped_directory_writer_does_not_publish() -> Result<()> {
    let (_temporary, store) = initialized_store()?;
    drop(store.begin_directory_write()?);
    store.verify_integrity()
}

#[test]
fn duplicate_directory_publication_is_idempotent_and_cleans_staging() -> Result<()> {
    let (temporary, store) = initialized_store()?;
    let first = store.begin_directory_write()?.finish()?;
    let second = store.begin_directory_write()?.finish()?;
    assert_eq!(first, second);

    let connection = Connection::open(temporary.path().join("metadata/metadata.sqlite3"))?;
    let directories: i64 =
        connection.query_row("SELECT COUNT(*) FROM directories", [], |row| row.get(0))?;
    let unfinished: i64 = connection.query_row(
        "SELECT COUNT(*) FROM directory_sets WHERE entry_count IS NULL",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(directories, 1);
    assert_eq!(unfinished, 0);
    Ok(())
}

#[test]
fn manifest_iterator_failure_rolls_back_staging() -> Result<()> {
    let (temporary, store) = initialized_store()?;
    let mut chunks = vec![Err(anyhow!("injected iterator failure"))].into_iter();
    assert!(store.put_manifest(1, &mut chunks).is_err());

    let connection = Connection::open(temporary.path().join("metadata/metadata.sqlite3"))?;
    let sets: i64 =
        connection.query_row("SELECT COUNT(*) FROM manifest_sets", [], |row| row.get(0))?;
    let manifests: i64 =
        connection.query_row("SELECT COUNT(*) FROM manifests", [], |row| row.get(0))?;
    assert_eq!((sets, manifests), (0, 0));
    Ok(())
}

#[test]
fn index_transaction_drop_rolls_back_and_stale_cas_is_rejected() -> Result<()> {
    let (_temporary, store) = initialized_store()?;
    let manifest = store.put_manifest(0, &mut std::iter::empty())?;
    let file = FileRecord::from_manifest("empty".to_owned(), 0, &manifest);
    let initial = store.read_index()?;
    let initial_version = initial.version().clone();
    drop(initial);

    let mut dropped = store.begin_index_transaction(Some(&initial_version))?;
    dropped.upsert_file(&file)?;
    drop(dropped);
    assert!(store.read_index()?.get_file("empty")?.is_none());
    assert_eq!(store.read_index()?.version(), &initial_version);

    let mut committed = store.begin_index_transaction(Some(&initial_version))?;
    committed.upsert_file(&file)?;
    let committed_version = committed.commit()?;
    assert_ne!(committed_version, initial_version);
    assert_eq!(store.read_index()?.get_file("empty")?, Some(file));
    assert!(store
        .begin_index_transaction(Some(&initial_version))
        .is_err());
    Ok(())
}

#[test]
fn metadata_snapshot_keeps_a_fixed_reference_view() -> Result<()> {
    let (_temporary, store) = initialized_store()?;
    let first = "a".repeat(64);
    let second = "b".repeat(64);
    assert_eq!(
        store.compare_exchange_reference("refs/heads/main", None, Some(&first))?,
        ReferenceCas::Updated
    );
    let snapshot = store.snapshot()?;
    assert_eq!(
        store.compare_exchange_reference("refs/heads/main", Some(&first), Some(&second))?,
        ReferenceCas::Updated
    );
    assert_eq!(snapshot.get_reference("refs/heads/main")?, Some(first));
    assert_eq!(store.get_reference("refs/heads/main")?, Some(second));
    Ok(())
}

#[test]
fn directory_lookup_and_ordinal_paging_are_consistent() -> Result<()> {
    let (_temporary, store) = initialized_store()?;
    let manifest = store.put_manifest(0, &mut std::iter::empty())?;
    let mut writer = store.begin_directory_write()?;
    for (ordinal, name) in ["a", "b", "c"].into_iter().enumerate() {
        writer.append_entry(&DirectoryEntry {
            ordinal: ordinal as u64,
            name: name.to_owned(),
            kind: DirectoryEntryKind::File,
            target_id: manifest.id.clone(),
            total_size: 0,
        })?;
    }
    let directory = writer.finish()?;
    let reader = store.open_directory(&directory.id)?.expect("directory");
    let first = reader.scan_entries(&PageRequest::first(2)?)?;
    assert_eq!(
        first
            .items
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert_eq!(first.next.as_deref(), Some("1"));
    let second = reader.scan_entries(&PageRequest::new(first.next, 2)?)?;
    assert_eq!(second.items[0], reader.get_entry("c")?.expect("c"));
    assert!(second.next.is_none());
    Ok(())
}

#[test]
fn integrity_check_rejects_corrupt_references() -> Result<()> {
    let (temporary, store) = initialized_store()?;
    assert_eq!(
        store.compare_exchange_reference("refs/heads/main", None, Some(&"a".repeat(64)))?,
        ReferenceCas::Updated
    );
    let connection = Connection::open(temporary.path().join("metadata/metadata.sqlite3"))?;
    connection.execute(
        "UPDATE refs SET target = 'not-an-id' WHERE name = 'refs/heads/main'",
        [],
    )?;
    drop(connection);
    assert!(store.verify_integrity().is_err());
    Ok(())
}

#[test]
fn unknown_sqlite_schema_version_is_rejected() -> Result<()> {
    let (temporary, store) = initialized_store()?;
    let database = temporary.path().join("metadata/metadata.sqlite3");
    let connection = Connection::open(&database)?;
    connection.pragma_update(None, "user_version", 999_i64)?;
    drop(connection);
    let reopened = SqliteMetadataStore::new(temporary.path().join("metadata"));
    assert!(reopened.validate_layout().is_err());
    assert!(reopened.read_index().is_err());
    drop(store);
    Ok(())
}

#[cfg(unix)]
#[test]
fn symlinked_sqlite_database_is_rejected_without_touching_target() -> Result<()> {
    use std::os::unix::fs::symlink;

    let (temporary, store) = initialized_store()?;
    let database = temporary.path().join("metadata/metadata.sqlite3");
    let outside = temporary.path().join("outside.sqlite3");
    fs::copy(&database, &outside)?;
    let before = fs::read(&outside)?;
    drop(store);
    fs::remove_file(&database)?;
    symlink(&outside, &database)?;

    let reopened = SqliteMetadataStore::new(temporary.path().join("metadata"));
    assert!(reopened.validate_layout().is_err());
    assert!(reopened.read_index().is_err());
    assert_eq!(fs::read(outside)?, before);
    Ok(())
}

#[test]
fn immutable_commit_publication_rejects_same_id_with_different_content() -> Result<()> {
    let (_temporary, store) = initialized_store()?;
    let id = "c".repeat(64);
    let first = Commit {
        tree_hash: "d".repeat(64),
        parent: None,
        message: "first".to_owned(),
        created_at_unix_ms: 1,
    };
    store.put_commit(&id, &first)?;
    store.put_commit(&id, &first)?;
    let mut second = first;
    second.message = "second".to_owned();
    assert!(store.put_commit(&id, &second).is_err());
    Ok(())
}

#[test]
fn head_state_round_trips_through_sqlite() -> Result<()> {
    let (_temporary, store) = initialized_store()?;
    assert_eq!(
        store.read_head_state()?,
        HeadState::Attached {
            reference: "refs/heads/main".to_owned()
        }
    );
    Ok(())
}
