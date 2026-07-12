use std::{fs, io::Cursor, num::NonZeroUsize, sync::Arc};

use synapse::{
    chunk_file, Chunk, FileNode, LooseObjectStore, ObjectCheck, ObjectSpec, ObjectStore,
    PutOutcome, Tree,
};

#[test]
fn models_round_trip_through_json() -> Result<(), Box<dyn std::error::Error>> {
    let tree = Tree {
        files: vec![FileNode {
            path: "models/weights.pt".to_owned(),
            total_size: 4,
            chunks: vec![Chunk {
                hash: "abcd".to_owned(),
                offset: 0,
                size: 4,
            }],
        }],
    };

    let json = serde_json::to_string(&tree)?;
    let decoded: Tree = serde_json::from_str(&json)?;

    assert_eq!(decoded, tree);
    Ok(())
}

#[tokio::test]
async fn chunker_persists_and_reuses_content_addressed_objects(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let first_path = temporary.path().join("first.pt");
    let second_path = temporary.path().join("second.pt");
    let objects_dir = temporary.path().join("objects");
    let staging_dir = temporary.path().join("staging");
    let store: Arc<dyn ObjectStore> = Arc::new(LooseObjectStore::new(objects_dir.clone()));
    let contents = b"shared model payload";
    fs::write(&first_path, contents)?;
    fs::write(&second_path, contents)?;

    let first = chunk_file(first_path, Arc::clone(&store), staging_dir.clone()).await?;
    let second = chunk_file(second_path, Arc::clone(&store), staging_dir).await?;

    assert_eq!(first.total_size, u64::try_from(contents.len())?);
    assert_eq!(first.chunks.len(), 1);
    assert_eq!(first.chunks, second.chunks);

    let object_entries = fs::read_dir(&objects_dir)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter_map(|entry| match entry.file_type() {
            Ok(file_type) if file_type.is_file() => Some(Ok(entry)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(object_entries.len(), 1);

    let mut restored = Vec::new();
    for chunk in &first.chunks {
        restored.extend(fs::read(objects_dir.join(&chunk.hash))?);
    }
    assert_eq!(restored, contents);
    Ok(())
}

#[tokio::test]
async fn empty_file_has_no_payload_chunks() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("empty.bin");
    let objects_dir = temporary.path().join("objects");
    let staging_dir = temporary.path().join("staging");
    let store: Arc<dyn ObjectStore> = Arc::new(LooseObjectStore::new(objects_dir.clone()));
    fs::write(&source, [])?;

    let file_node = chunk_file(source, store, staging_dir).await?;

    assert_eq!(file_node.total_size, 0);
    assert!(file_node.chunks.is_empty());
    assert!(objects_dir.is_dir());
    assert!(objects_dir.join(".tmp").is_dir());
    assert_eq!(
        fs::read_dir(objects_dir)?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|entry| entry.path().is_file())
            .count(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn chunker_rejects_same_size_corrupt_existing_object(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("model.bin");
    let objects_dir = temporary.path().join("objects");
    let staging_dir = temporary.path().join("staging");
    let store: Arc<dyn ObjectStore> = Arc::new(LooseObjectStore::new(objects_dir.clone()));
    let contents = b"original model payload";
    fs::write(&source, contents)?;

    let first = chunk_file(source.clone(), Arc::clone(&store), staging_dir.clone()).await?;
    let chunk = first.chunks.first().ok_or("missing chunk")?;
    fs::write(objects_dir.join(&chunk.hash), vec![b'x'; contents.len()])?;

    let error = match chunk_file(source, store, staging_dir).await {
        Ok(_) => return Err("same-size corrupt object was accepted".into()),
        Err(error) => error,
    };
    assert!(format!("{error:#}").contains("Hash 损坏"));
    Ok(())
}

#[test]
fn loose_object_store_satisfies_streaming_and_paging_contract(
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let store = LooseObjectStore::new(temporary.path().join("objects"));
    store.initialize()?;
    store.validate_layout()?;

    let mut expected_ids = Vec::new();
    for payload in [b"charlie".as_slice(), b"alpha", b"bravo"] {
        let spec = ObjectSpec::new(
            blake3::hash(payload).to_hex().to_string(),
            u64::try_from(payload.len())?,
        )?;
        assert_eq!(
            store.put_from(&spec, &mut Cursor::new(payload))?,
            PutOutcome::Created
        );
        assert_eq!(
            store.put_from(&spec, &mut Cursor::new(payload))?,
            PutOutcome::AlreadyPresent
        );
        store.verify(&spec)?;

        let mut restored = Vec::new();
        store.copy_to(&spec, &mut restored)?;
        assert_eq!(restored, payload);
        assert_eq!(store.stat(&spec.id)?.map(|meta| meta.size), Some(spec.size));
        expected_ids.push(spec.id);
    }
    store.durability_barrier()?;

    let present = ObjectSpec::new(expected_ids[0].clone(), 7)?;
    let wrong_size = ObjectSpec::new(expected_ids[1].clone(), 999)?;
    let missing = ObjectSpec::new(blake3::hash(b"missing").to_hex().to_string(), 7)?;
    let checks = store.check_many(&[present, wrong_size.clone(), missing.clone()])?;
    assert!(matches!(checks[0], ObjectCheck::Present(_)));
    assert_eq!(
        checks[1],
        ObjectCheck::SizeMismatch {
            expected: wrong_size,
            actual_size: 5,
        }
    );
    assert_eq!(checks[2], ObjectCheck::Missing(missing));

    expected_ids.sort_unstable();
    let mut listed_ids = Vec::new();
    let mut cursor = None;
    loop {
        let page = store.list_page(cursor.as_deref(), NonZeroUsize::new(1).expect("non-zero"))?;
        listed_ids.extend(page.objects.into_iter().map(|meta| meta.id));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    assert_eq!(listed_ids, expected_ids);

    let payload = b"valid prefix";
    let rejected = ObjectSpec::new(
        blake3::hash(payload).to_hex().to_string(),
        u64::try_from(payload.len())?,
    )?;
    let mut trailing = payload.to_vec();
    trailing.extend_from_slice(b" trailing");
    assert!(store
        .put_from(&rejected, &mut Cursor::new(trailing))
        .is_err());
    assert_eq!(store.stat(&rejected.id)?, None);
    Ok(())
}

#[tokio::test]
async fn chunker_can_target_an_abstract_object_store() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let source = temporary.path().join("dataset.bin");
    let objects = temporary.path().join("objects");
    let staging = temporary.path().join("staging");
    let contents = b"payload through object store";
    fs::write(&source, contents)?;

    let store: Arc<dyn ObjectStore> = Arc::new(LooseObjectStore::new(objects));
    let file = chunk_file(source, Arc::clone(&store), staging.clone()).await?;

    assert_eq!(file.total_size, u64::try_from(contents.len())?);
    assert!(staging.is_dir());
    let spec = ObjectSpec::new(file.chunks[0].hash.clone(), file.chunks[0].size)?;
    let mut restored = Vec::new();
    store.copy_to(&spec, &mut restored)?;
    assert_eq!(restored, contents);
    Ok(())
}
