use std::{fs, num::NonZeroU32};

use anyhow::{anyhow, Result};
use rusqlite::Connection;

use crate::local::model::{Chunk, ChunkingStrategy, Commit, DirectoryEntry, DirectoryEntryKind};

use super::{
    canonical_commit_id, core_file_record, describe_manifest, DirectoryHasher, FileRecord,
    HeadState, ImmutableCatalogStore, MetadataReader, PageRequest, RefStore, ReferenceCas,
    RepositoryMetadataStore, SqliteMetadataStore, WorkspaceIndexStore, WorkspaceRegistry,
};

fn initialized_store() -> Result<(tempfile::TempDir, SqliteMetadataStore)> {
    let temporary = tempfile::tempdir()?;
    let store = SqliteMetadataStore::new(temporary.path().join("metadata"));
    store.initialize("refs/heads/main")?;
    Ok((temporary, store))
}

#[test]
fn canonical_ids_are_stable() -> Result<()> {
    let empty = describe_manifest(0, ChunkingStrategy::FastCdc, &[])?;
    let core_empty = neoengram_core::Manifest::new(0, ChunkingStrategy::FastCdc, Vec::new())?;
    assert_eq!(empty.id, core_empty.canonical_id()?.to_string());
    assert_eq!(
        empty.id,
        "6d7f506996a38fa34522281302731a4ff1b3b1ac21b74d22a2d07134e5f54fb1"
    );

    let chunks = vec![Chunk {
        hash: blake3::hash(b"abc").to_hex().to_string(),
        offset: 0,
        size: 3,
    }];
    let manifest = describe_manifest(3, ChunkingStrategy::FastCdc, &chunks)?;
    let core_manifest = neoengram_core::Manifest::new(
        3,
        ChunkingStrategy::FastCdc,
        vec![neoengram_core::ChunkRef::new(
            neoengram_core::ObjectId::for_bytes(b"abc"),
            0,
            3,
        )?],
    )?;
    assert_eq!(manifest.id, core_manifest.canonical_id()?.to_string());
    assert_eq!(
        manifest.id,
        "6fa68822b099d8087dd775d6e482d0cecbae85f6d905cdc7592ceb092cbad7de"
    );

    let local_entry = DirectoryEntry {
        ordinal: 0,
        name: "payload.bin".to_owned(),
        kind: DirectoryEntryKind::File,
        target_id: manifest.id.clone(),
        total_size: 3,
    };
    let mut hasher = DirectoryHasher::new();
    hasher.push(&local_entry)?;
    let local_directory_id = hasher.finish().0;
    let core_directory =
        neoengram_core::Directory::new(vec![neoengram_core::DirectoryEntry::file(
            0,
            neoengram_core::PathComponent::parse("payload.bin")?,
            core_manifest.canonical_id()?,
            3,
        )])?;
    assert_eq!(
        local_directory_id,
        core_directory.canonical_id()?.to_string()
    );
    assert_eq!(
        local_directory_id,
        "816bf224efcd6eba9a93a7c04bf59b9728cb09cd4f2c123233743ffc46a6df22"
    );
    Ok(())
}

#[test]
fn chunking_strategy_is_part_of_the_manifest_identity() -> Result<()> {
    let payload = b"single chunk";
    let chunks = vec![Chunk {
        hash: blake3::hash(payload).to_hex().to_string(),
        offset: 0,
        size: payload.len() as u64,
    }];
    let fastcdc = describe_manifest(payload.len() as u64, ChunkingStrategy::FastCdc, &chunks)?;
    let whole_file = describe_manifest(payload.len() as u64, ChunkingStrategy::WholeFile, &chunks)?;

    assert_ne!(fastcdc.id, whole_file.id);
    assert_eq!(fastcdc.chunking, ChunkingStrategy::FastCdc);
    assert_eq!(whole_file.chunking, ChunkingStrategy::WholeFile);
    Ok(())
}

#[test]
fn directory_names_must_be_strictly_sorted() -> Result<()> {
    let target = describe_manifest(0, ChunkingStrategy::FastCdc, &[])?.id;
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
    let manifest = store.put_manifest(
        6,
        ChunkingStrategy::FastCdc,
        &mut vec![Ok(first), Ok(second.clone())].into_iter(),
    )?;
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
    assert!(store
        .put_manifest(1, ChunkingStrategy::FastCdc, &mut chunks)
        .is_err());

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
    let manifest = store.put_manifest(0, ChunkingStrategy::FastCdc, &mut std::iter::empty())?;
    let file = FileRecord::from_manifest("empty".to_owned(), 0, &manifest);
    let initial = store.read_index()?;
    let initial_version = *initial.version();
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
fn sqlite_index_version_matches_core_snapshot() -> Result<()> {
    let (_temporary, store) = initialized_store()?;
    let initial_version = *store.read_index()?.version();
    assert_eq!(
        initial_version,
        neoengram_core::IndexVersion::from_snapshot(0, &[])?
    );

    let manifest = store.put_manifest(0, ChunkingStrategy::FastCdc, &mut std::iter::empty())?;
    let first = FileRecord::from_manifest("a.bin".to_owned(), 0, &manifest);
    let second = FileRecord::from_manifest("z.bin".to_owned(), 0, &manifest);
    let mut transaction = store.begin_index_transaction(Some(&initial_version))?;
    transaction.upsert_file(&second)?;
    transaction.upsert_file(&first)?;
    let committed_version = transaction.commit()?;

    let core_records = vec![core_file_record(&first)?, core_file_record(&second)?];
    assert_eq!(
        committed_version,
        neoengram_core::IndexVersion::from_snapshot(1, &core_records)?
    );
    assert_eq!(store.read_index()?.version(), &committed_version);

    let mut transaction = store.begin_index_transaction(Some(&committed_version))?;
    assert_eq!(transaction.delete_prefix("a.bin")?, 1);
    let deleted_version = transaction.commit()?;
    assert_eq!(
        deleted_version,
        neoengram_core::IndexVersion::from_snapshot(2, &[core_file_record(&second)?])?
    );
    assert_eq!(store.read_index()?.version(), &deleted_version);
    store.verify_integrity()
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
    let manifest = store.put_manifest(0, ChunkingStrategy::FastCdc, &mut std::iter::empty())?;
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
    let first = Commit {
        root_directory_id: "d".repeat(64),
        parent: None,
        message: "first".to_owned(),
        created_at_unix_ms: 1,
    };
    let id = canonical_commit_id(&first)?;
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

#[test]
fn workspaces_have_independent_head_and_index_state() -> Result<()> {
    let (temporary, main) = initialized_store()?;
    let directory = main.begin_directory_write()?.finish()?;
    let commit = Commit {
        root_directory_id: directory.id,
        parent: None,
        message: "workspace base".to_owned(),
        created_at_unix_ms: 1,
    };
    let commit_id = canonical_commit_id(&commit)?;
    main.put_commit(&commit_id, &commit)?;
    let experiment = main.create_workspace("experiment", "workspaces/experiment", &commit_id)?;
    let experiment_store =
        SqliteMetadataStore::for_workspace(temporary.path().join("metadata"), experiment.id);
    assert_eq!(
        experiment_store.read_head_state()?,
        HeadState::Detached {
            commit_id: commit_id.clone(),
        }
    );
    assert!(main.get_reference("refs/heads/experiment")?.is_none());
    main.attach_workspace_to_branch(experiment.id, &commit_id, "refs/heads/experiment")?;
    assert_eq!(
        main.get_reference("refs/heads/experiment")?.as_deref(),
        Some(commit_id.as_str())
    );

    let manifest = main.put_manifest(0, ChunkingStrategy::FastCdc, &mut std::iter::empty())?;
    let main_file = FileRecord::from_manifest("main.bin".to_owned(), 0, &manifest);
    let main_version = *main.read_index()?.version();
    let mut main_txn = main.begin_index_transaction(Some(&main_version))?;
    main_txn.upsert_file(&main_file)?;
    main_txn.commit()?;

    assert!(experiment_store
        .read_index()?
        .get_file("main.bin")?
        .is_none());
    let experiment_file = FileRecord::from_manifest("experiment.bin".to_owned(), 0, &manifest);
    let experiment_version = *experiment_store.read_index()?.version();
    let mut experiment_txn = experiment_store.begin_index_transaction(Some(&experiment_version))?;
    experiment_txn.upsert_file(&experiment_file)?;
    experiment_txn.commit()?;

    assert!(main.read_index()?.get_file("experiment.bin")?.is_none());
    assert_eq!(
        experiment_store.read_head_state()?,
        HeadState::Attached {
            reference: "refs/heads/experiment".to_owned(),
        }
    );
    let duplicate =
        main.create_workspace("duplicate-main", "workspaces/duplicate-main", &commit_id)?;
    assert!(main
        .attach_workspace_to_branch(duplicate.id, &commit_id, "refs/heads/main")
        .is_err());
    Ok(())
}
