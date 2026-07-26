use std::str::FromStr;

use neoengram_core::{
    canonical, validate_index_mutation_paths, validate_index_snapshot, validate_path_set, ChunkRef,
    ChunkingStrategy, Commit, ContentDigest, Directory, DirectoryEntry, DirectoryEntryKind,
    DirectoryEntryTarget, FileRecord, IndexDelta, IndexDeltaPage, IndexMutation, IndexVersion,
    LogicalPath, Manifest, ObjectId, ObjectSpec, PathComponent, ValidationErrorKind,
    INDEX_FORMAT_VERSION, MAX_INDEX_MUTATIONS_PER_PAGE,
};

fn empty_manifest() -> Manifest {
    Manifest::new(0, ChunkingStrategy::FastCdc, Vec::new()).expect("valid empty Manifest")
}

#[derive(Clone)]
struct AddPublicationFixture {
    base: IndexVersion,
    delta: IndexDelta,
    manifests: Vec<Manifest>,
    objects: Vec<ObjectSpec>,
}

impl AddPublicationFixture {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let first_chunks = [b"alpha-".as_slice(), b"payload".as_slice()];
        let first = Manifest::new(
            first_chunks.iter().map(|chunk| chunk.len() as u64).sum(),
            ChunkingStrategy::FastCdc,
            vec![
                ChunkRef::new(ObjectId::for_bytes(first_chunks[0]), 0, 6)?,
                ChunkRef::new(ObjectId::for_bytes(first_chunks[1]), 6, 7)?,
            ],
        )?;
        let second_bytes = b"beta";
        let second = Manifest::new(
            second_bytes.len() as u64,
            ChunkingStrategy::WholeFile,
            vec![ChunkRef::new(
                ObjectId::for_bytes(second_bytes),
                0,
                second_bytes.len() as u64,
            )?],
        )?;
        let base = IndexVersion::new(41, ContentDigest::from_bytes([0x41; 32]));
        let delta = IndexDelta::new(
            base,
            vec![
                IndexMutation::Delete {
                    path: LogicalPath::parse("legacy.bin")?,
                },
                IndexMutation::Upsert {
                    record: FileRecord::from_manifest(
                        LogicalPath::parse("models/alpha.bin")?,
                        &first,
                    )?,
                },
                IndexMutation::Upsert {
                    record: FileRecord::from_manifest(
                        LogicalPath::parse("models/beta.bin")?,
                        &second,
                    )?,
                },
            ],
            ContentDigest::from_bytes([0x52; 32]),
        )?;
        let objects = first
            .chunks
            .iter()
            .chain(&second.chunks)
            .copied()
            .map(ChunkRef::object_spec)
            .collect();
        Ok(Self {
            base,
            delta,
            manifests: vec![first, second],
            objects,
        })
    }

    fn digest(&self) -> Result<ContentDigest, neoengram_core::ValidationError> {
        canonical::add_publication_digest(
            "tenant-1",
            "project-1",
            "artifact-1",
            "playground-1",
            "job-1",
            &self.base,
            &self.delta,
            &self.manifests,
            &self.objects,
        )
    }
}

#[test]
fn typed_ids_use_canonical_lowercase_hex_json() -> Result<(), Box<dyn std::error::Error>> {
    let object_id = ObjectId::for_bytes(b"abc");
    assert_eq!(
        object_id.to_string(),
        "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
    );
    let json = serde_json::to_string(&object_id)?;
    assert_eq!(json, format!("\"{object_id}\""));
    assert_eq!(serde_json::from_str::<ObjectId>(&json)?, object_id);

    let uppercase = object_id.to_string().to_ascii_uppercase();
    let error = ObjectId::from_str(&uppercase).expect_err("uppercase must be rejected");
    assert_eq!(error.kind(), ValidationErrorKind::InvalidDigest);
    Ok(())
}

#[test]
fn logical_paths_enforce_nfc_portability_and_prefix_rules() -> Result<(), Box<dyn std::error::Error>>
{
    let valid = LogicalPath::parse("caf\u{e9}/model.bin")?;
    assert_eq!(valid.as_str(), "caf\u{e9}/model.bin");
    assert_eq!(valid.parent().expect("parent").as_str(), "caf\u{e9}");

    for invalid in [
        "",
        "/absolute.bin",
        "C:drive-relative.bin",
        "dir\\windows.bin",
        "dir/../escape.bin",
        "CON.txt",
        "aux",
        "LPT9.log",
        "COM\u{b9}.txt",
        "COM\u{b2}",
        "COM\u{b3}.log",
        "lpt\u{b9}",
        "LPT\u{b2}.txt",
        "lpt\u{b3}.log",
        "name.",
        "name ",
        "bad?.bin",
        ".NeoEngram/private.bin",
        ".neoengram-tmp-orphan/private.bin",
        "cafe\u{301}.bin",
    ] {
        assert!(LogicalPath::parse(invalid).is_err(), "accepted {invalid}");
    }

    let case_collision = [
        LogicalPath::parse("Model.bin")?,
        LogicalPath::parse("model.bin")?,
    ];
    assert!(validate_path_set(case_collision.iter()).is_err());
    let final_sigma_collision = [
        LogicalPath::parse("\u{3a3}")?,
        LogicalPath::parse("\u{3c2}")?,
    ];
    assert_eq!(final_sigma_collision[0].portable_key(), "\u{3c3}");
    assert_eq!(final_sigma_collision[1].portable_key(), "\u{3c3}");
    assert!(validate_path_set(final_sigma_collision.iter()).is_err());
    let full_fold_collision = [
        LogicalPath::parse("MASSE")?,
        LogicalPath::parse("Ma\u{df}e")?,
    ];
    assert!(validate_path_set(full_fold_collision.iter()).is_err());
    let prefix_collision = [
        LogicalPath::parse("a")?,
        LogicalPath::parse("a-b")?,
        LogicalPath::parse("a/child")?,
    ];
    assert!(validate_path_set(prefix_collision.iter()).is_err());
    Ok(())
}

#[test]
fn manifest_v4_golden_ids_are_stable() -> Result<(), Box<dyn std::error::Error>> {
    let empty = empty_manifest();
    assert_eq!(
        empty.canonical_id()?.to_string(),
        "6d7f506996a38fa34522281302731a4ff1b3b1ac21b74d22a2d07134e5f54fb1"
    );

    let payload = b"abc";
    let manifest = Manifest::new(
        payload.len() as u64,
        ChunkingStrategy::FastCdc,
        vec![ChunkRef::new(
            ObjectId::for_bytes(payload),
            0,
            payload.len() as u64,
        )?],
    )?;
    assert_eq!(
        manifest.canonical_id()?.to_string(),
        "6fa68822b099d8087dd775d6e482d0cecbae85f6d905cdc7592ceb092cbad7de"
    );
    assert_ne!(
        manifest.canonical_id()?,
        Manifest::new(
            payload.len() as u64,
            ChunkingStrategy::WholeFile,
            manifest.chunks.clone(),
        )?
        .canonical_id()?
    );
    Ok(())
}

#[test]
fn directory_v1_and_commit_v3_golden_ids_are_stable() -> Result<(), Box<dyn std::error::Error>> {
    let empty_directory = Directory::default();
    let empty_id = empty_directory.canonical_id()?;
    assert_eq!(
        empty_id.to_string(),
        "be674f5161f4ba55cc9028fdca4e8e219e8470191ee749afacb0bac8ab53be99"
    );

    let manifest_id = Manifest::new(
        3,
        ChunkingStrategy::FastCdc,
        vec![ChunkRef::new(ObjectId::for_bytes(b"abc"), 0, 3)?],
    )?
    .canonical_id()?;
    let directory = Directory::new(vec![DirectoryEntry::file(
        0,
        PathComponent::parse("payload.bin")?,
        manifest_id,
        3,
    )])?;
    assert_eq!(
        directory.canonical_id()?.to_string(),
        "816bf224efcd6eba9a93a7c04bf59b9728cb09cd4f2c123233743ffc46a6df22"
    );

    let commit = Commit::new(empty_id, None, "snapshot", 1)?;
    assert_eq!(
        commit.canonical_id()?.to_string(),
        "720575956eb8d66cc4b0bc4e038013b1df17f2e170a6ad844184efefcfe2aaf6"
    );
    Ok(())
}

#[test]
fn directory_target_type_and_order_are_validated() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_id = empty_manifest().canonical_id()?;
    let mismatched = DirectoryEntry {
        ordinal: 0,
        name: PathComponent::parse("file")?,
        kind: DirectoryEntryKind::Directory,
        target_id: DirectoryEntryTarget::Manifest(manifest_id),
        total_size: 0,
    };
    assert!(mismatched.validate().is_err());

    let out_of_order = Directory {
        entries: vec![
            DirectoryEntry::file(0, PathComponent::parse("z")?, manifest_id, 0),
            DirectoryEntry::file(1, PathComponent::parse("a")?, manifest_id, 0),
        ],
    };
    assert!(out_of_order.validate().is_err());
    Ok(())
}

#[test]
fn index_v8_digest_and_delta_are_canonical() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(INDEX_FORMAT_VERSION, 8);
    let manifest_id = empty_manifest().canonical_id()?;
    let records = vec![
        FileRecord::new(LogicalPath::parse("a.bin")?, manifest_id, 0, 0)?,
        FileRecord::new(LogicalPath::parse("models/z.bin")?, manifest_id, 0, 0)?,
    ];
    validate_index_snapshot(&records)?;

    let version = IndexVersion::from_snapshot(7, &records)?;
    assert_eq!(
        version.digest.to_string(),
        "816284ddeec14b48b043d179c781256cafff02490d8b1a9f17a75187f7a03b73"
    );
    let record_digest = canonical::index_record_digest(&records[0])?;
    assert_eq!(
        record_digest.to_string(),
        "349df1d25af4576c2778668c28c4beaffaf00e2355d681abd976a748d167f45f"
    );

    let result_digest = ContentDigest::hash(b"resulting snapshot");
    let delta = IndexDelta::new(
        version,
        vec![
            IndexMutation::Upsert {
                record: FileRecord::new(LogicalPath::parse("a.bin")?, manifest_id, 0, 0)?,
            },
            IndexMutation::Delete {
                path: LogicalPath::parse("models/z.bin")?,
            },
        ],
        result_digest,
    )?;
    assert_eq!(
        delta.canonical_digest()?.to_string(),
        "f889171725266df10d46f1e34e7940defb0539c68d30eeceb7aa738e8c80c169"
    );
    Ok(())
}

#[test]
fn add_publication_digest_is_stable_across_set_order_and_page_boundaries(
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = AddPublicationFixture::new()?;
    let digest = fixture.digest()?;
    assert_eq!(
        digest.to_string(),
        "b0de65d270078fe89cb0005e13bcb8049e2308682c01555abab519bf3d70cf6e"
    );

    let mut reordered = fixture.clone();
    reordered.manifests.reverse();
    reordered.objects.reverse();
    assert_eq!(reordered.digest()?, digest);

    let mutations = fixture.delta.iter().cloned().collect::<Vec<_>>();
    let repaged = IndexDelta::from_pages(
        fixture.base,
        vec![
            IndexDeltaPage::new(0, mutations[..1].to_vec())?,
            IndexDeltaPage::new(1, mutations[1..].to_vec())?,
        ],
        fixture.delta.result_digest,
    )?;
    assert_eq!(
        canonical::add_publication_digest(
            "tenant-1",
            "project-1",
            "artifact-1",
            "playground-1",
            "job-1",
            &fixture.base,
            &repaged,
            &fixture.manifests,
            &fixture.objects,
        )?,
        digest
    );

    let reversed_mutations = mutations.into_iter().rev().collect();
    let error = IndexDelta::new(
        fixture.base,
        reversed_mutations,
        fixture.delta.result_digest,
    )
    .expect_err("Index mutation order is canonical, not set-like");
    assert_eq!(error.kind(), ValidationErrorKind::InvalidIndex);
    Ok(())
}

#[test]
fn add_publication_digest_binds_scope_base_result_and_content(
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = AddPublicationFixture::new()?;
    let digest = fixture.digest()?;
    let scope = [
        "tenant-1",
        "project-1",
        "artifact-1",
        "playground-1",
        "job-1",
    ];
    for position in 0..scope.len() {
        let mut changed = scope;
        changed[position] = "changed";
        assert_ne!(
            canonical::add_publication_digest(
                changed[0],
                changed[1],
                changed[2],
                changed[3],
                changed[4],
                &fixture.base,
                &fixture.delta,
                &fixture.manifests,
                &fixture.objects,
            )?,
            digest
        );

        changed[position] = "";
        let error = canonical::add_publication_digest(
            changed[0],
            changed[1],
            changed[2],
            changed[3],
            changed[4],
            &fixture.base,
            &fixture.delta,
            &fixture.manifests,
            &fixture.objects,
        )
        .expect_err("an empty scope component must be rejected");
        assert_eq!(error.kind(), ValidationErrorKind::InvalidPublication);
    }

    let changed_base = IndexVersion::new(42, ContentDigest::from_bytes([0x42; 32]));
    let mut changed_base_delta = fixture.delta.clone();
    changed_base_delta.base_version = changed_base;
    assert_ne!(
        canonical::add_publication_digest(
            scope[0],
            scope[1],
            scope[2],
            scope[3],
            scope[4],
            &changed_base,
            &changed_base_delta,
            &fixture.manifests,
            &fixture.objects,
        )?,
        digest
    );
    let error = canonical::add_publication_digest(
        scope[0],
        scope[1],
        scope[2],
        scope[3],
        scope[4],
        &changed_base,
        &fixture.delta,
        &fixture.manifests,
        &fixture.objects,
    )
    .expect_err("a mismatched declared base must be rejected");
    assert_eq!(error.kind(), ValidationErrorKind::InvalidPublication);

    let mut changed_result = fixture.clone();
    changed_result.delta.result_digest = ContentDigest::from_bytes([0x53; 32]);
    assert_ne!(changed_result.digest()?, digest);

    let mut changed_content = fixture.clone();
    let replacement_bytes = b"BETA";
    let replacement = Manifest::new(
        replacement_bytes.len() as u64,
        ChunkingStrategy::WholeFile,
        vec![ChunkRef::new(
            ObjectId::for_bytes(replacement_bytes),
            0,
            replacement_bytes.len() as u64,
        )?],
    )?;
    changed_content.manifests[1] = replacement.clone();
    changed_content.objects[2] = ObjectSpec::for_bytes(replacement_bytes);
    let IndexMutation::Upsert { record } = &mut changed_content.delta.pages[0].mutations[2] else {
        panic!("fixture mutation must remain an upsert");
    };
    *record = FileRecord::from_manifest(record.path.clone(), &replacement)?;
    assert_ne!(changed_content.digest()?, digest);
    Ok(())
}

#[test]
fn add_publication_digest_rejects_open_or_inconsistent_closures(
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = AddPublicationFixture::new()?;
    let assert_invalid = |manifests: &[Manifest], objects: &[ObjectSpec]| {
        let error = canonical::add_publication_digest(
            "tenant-1",
            "project-1",
            "artifact-1",
            "playground-1",
            "job-1",
            &fixture.base,
            &fixture.delta,
            manifests,
            objects,
        )
        .expect_err("an open publication graph must be rejected");
        assert_eq!(error.kind(), ValidationErrorKind::InvalidPublication);
    };

    assert_invalid(&fixture.manifests[..1], &fixture.objects);

    let mut extra_manifests = fixture.manifests.clone();
    extra_manifests.push(empty_manifest());
    assert_invalid(&extra_manifests, &fixture.objects);

    assert_invalid(&fixture.manifests, &fixture.objects[..2]);
    let mut extra_objects = fixture.objects.clone();
    extra_objects.push(ObjectSpec::for_bytes(b"unreferenced"));
    assert_invalid(&fixture.manifests, &extra_objects);

    let mut wrong_size = fixture.objects.clone();
    wrong_size[0].size += 1;
    assert_invalid(&fixture.manifests, &wrong_size);
    Ok(())
}

#[test]
fn index_delta_pages_arbitrary_mutation_sequences_without_changing_identity(
) -> Result<(), Box<dyn std::error::Error>> {
    let base_version = IndexVersion::new(9, ContentDigest::from_bytes([9; 32]));
    let result_digest = ContentDigest::from_bytes([10; 32]);
    let mutations = (0..=MAX_INDEX_MUTATIONS_PER_PAGE)
        .map(|index| {
            LogicalPath::parse(format!("files/file-{index:05}.bin"))
                .map(|path| IndexMutation::Delete { path })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let delta = IndexDelta::new(base_version, mutations.clone(), result_digest)?;
    assert_eq!(delta.mutation_count(), MAX_INDEX_MUTATIONS_PER_PAGE + 1);
    assert_eq!(delta.page_count(), 2);
    assert_eq!(delta.pages[0].page_number, 0);
    assert_eq!(delta.pages[0].mutations.len(), MAX_INDEX_MUTATIONS_PER_PAGE);
    assert_eq!(delta.pages[1].page_number, 1);
    assert_eq!(delta.pages[1].mutations.len(), 1);
    assert_eq!(delta.iter().count(), MAX_INDEX_MUTATIONS_PER_PAGE + 1);

    let alternate_boundaries = IndexDelta::from_pages(
        base_version,
        vec![
            IndexDeltaPage::new(0, mutations[..1].to_vec())?,
            IndexDeltaPage::new(1, mutations[1..].to_vec())?,
        ],
        result_digest,
    )?;
    assert_eq!(
        alternate_boundaries.mutation_count(),
        delta.mutation_count()
    );
    assert_eq!(
        alternate_boundaries.canonical_digest()?,
        delta.canonical_digest()?
    );
    assert_eq!(
        delta.canonical_digest()?.to_string(),
        "e1fbc877ea3e82b5ee2669cbd06cfed8eb4ab73240c37bc76de6fb4c471a24c0"
    );

    let empty = IndexDelta::new(base_version, Vec::new(), result_digest)?;
    assert_eq!(empty.page_count(), 0);
    assert_eq!(empty.mutation_count(), 0);
    empty.validate()?;
    Ok(())
}

#[test]
fn index_delta_rejects_invalid_pages_and_cross_page_ordering(
) -> Result<(), Box<dyn std::error::Error>> {
    let base_version = IndexVersion::new(9, ContentDigest::from_bytes([9; 32]));
    let result_digest = ContentDigest::from_bytes([10; 32]);
    assert!(IndexDeltaPage::new(0, Vec::new()).is_err());

    let oversized = (0..=MAX_INDEX_MUTATIONS_PER_PAGE)
        .map(|index| {
            LogicalPath::parse(format!("oversized/file-{index:05}.bin"))
                .map(|path| IndexMutation::Delete { path })
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert!(IndexDeltaPage::new(0, oversized).is_err());

    let one = IndexMutation::Delete {
        path: LogicalPath::parse("one.bin")?,
    };
    let skipped_zero = IndexDelta::from_pages(
        base_version,
        vec![IndexDeltaPage::new(1, vec![one])?],
        result_digest,
    );
    assert!(skipped_zero.is_err());

    let out_of_order = IndexDelta::from_pages(
        base_version,
        vec![
            IndexDeltaPage::new(
                0,
                vec![IndexMutation::Delete {
                    path: LogicalPath::parse("b.bin")?,
                }],
            )?,
            IndexDeltaPage::new(
                1,
                vec![IndexMutation::Delete {
                    path: LogicalPath::parse("a.bin")?,
                }],
            )?,
        ],
        result_digest,
    );
    assert!(out_of_order.is_err());

    let portable_collision = IndexDelta::from_pages(
        base_version,
        vec![
            IndexDeltaPage::new(
                0,
                vec![IndexMutation::Delete {
                    path: LogicalPath::parse("A.bin")?,
                }],
            )?,
            IndexDeltaPage::new(
                1,
                vec![IndexMutation::Delete {
                    path: LogicalPath::parse("a.bin")?,
                }],
            )?,
        ],
        result_digest,
    );
    assert!(portable_collision.is_err());
    Ok(())
}

#[test]
fn index_delta_allows_cross_page_file_directory_transitions_but_snapshots_do_not(
) -> Result<(), Box<dyn std::error::Error>> {
    let base_version = IndexVersion::new(9, ContentDigest::from_bytes([9; 32]));
    let result_digest = ContentDigest::from_bytes([10; 32]);
    let manifest_id = empty_manifest().canonical_id()?;
    let file = LogicalPath::parse("a")?;
    let child = LogicalPath::parse("a/b")?;

    validate_index_mutation_paths([&file, &child])?;
    assert!(validate_path_set([&file, &child]).is_err());

    let expanded = IndexDelta::from_pages(
        base_version,
        vec![
            IndexDeltaPage::new(0, vec![IndexMutation::Delete { path: file.clone() }])?,
            IndexDeltaPage::new(
                1,
                vec![IndexMutation::Upsert {
                    record: FileRecord::new(child.clone(), manifest_id, 0, 0)?,
                }],
            )?,
        ],
        result_digest,
    )?;
    assert_eq!(expanded.page_count(), 2);

    let collapsed = IndexDelta::from_pages(
        base_version,
        vec![
            IndexDeltaPage::new(
                0,
                vec![IndexMutation::Upsert {
                    record: FileRecord::new(file.clone(), manifest_id, 0, 0)?,
                }],
            )?,
            IndexDeltaPage::new(
                1,
                vec![IndexMutation::Delete {
                    path: child.clone(),
                }],
            )?,
        ],
        result_digest,
    )?;
    assert_eq!(collapsed.page_count(), 2);

    let invalid_final_snapshot = vec![
        FileRecord::new(file, manifest_id, 0, 0)?,
        FileRecord::new(child, manifest_id, 0, 0)?,
    ];
    assert!(validate_index_snapshot(&invalid_final_snapshot).is_err());
    Ok(())
}

#[test]
fn invalid_models_are_rejected_before_hashing() -> Result<(), Box<dyn std::error::Error>> {
    let object_id = ObjectId::for_bytes(b"x");
    assert!(ChunkRef::new(object_id, 0, 0).is_err());
    assert!(Manifest::new(
        2,
        ChunkingStrategy::FastCdc,
        vec![ChunkRef::new(object_id, 1, 1)?],
    )
    .is_err());
    assert!(Commit::new(Directory::default().canonical_id()?, None, " ", 0).is_err());
    Ok(())
}
