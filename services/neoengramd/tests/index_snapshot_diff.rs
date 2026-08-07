use neoengram_core::{FileRecord, LogicalPath, ManifestId};
use neoengramd::{diff_index_snapshots, IndexSnapshotChangeKind};

#[test]
fn index_snapshot_diff_classifies_incremental_changes() {
    let base = vec![
        record("dataset/delete.bin", 1, 20),
        record("dataset/modify.bin", 2, 10),
        record("dataset/old-name.bin", 3, 5),
    ];
    let candidate = vec![
        record("dataset/add.bin", 4, 7),
        record("dataset/modify.bin", 5, 15),
        record("dataset/new-name.bin", 3, 5),
    ];

    let diff = diff_index_snapshots(&base, &candidate).unwrap();

    assert_eq!(diff.summary.files_added, 1);
    assert_eq!(diff.summary.files_modified, 1);
    assert_eq!(diff.summary.files_deleted, 1);
    assert_eq!(diff.summary.files_renamed, 1);
    assert_eq!(diff.summary.bytes_added, 12);
    assert_eq!(diff.summary.bytes_removed, 20);
    assert_eq!(
        diff.changes
            .iter()
            .map(|change| (change.path.as_str(), change.kind))
            .collect::<Vec<_>>(),
        vec![
            ("dataset/add.bin", IndexSnapshotChangeKind::Added),
            ("dataset/delete.bin", IndexSnapshotChangeKind::Deleted),
            ("dataset/modify.bin", IndexSnapshotChangeKind::Modified),
            ("dataset/new-name.bin", IndexSnapshotChangeKind::Renamed),
        ]
    );
    assert_eq!(
        diff.changes.last().unwrap().previous_path.as_ref(),
        Some(&LogicalPath::parse("dataset/old-name.bin").unwrap())
    );
}

#[test]
fn ambiguous_content_moves_remain_additions_and_deletions() {
    let base = vec![
        record("dataset/old-a.bin", 7, 5),
        record("dataset/old-b.bin", 7, 5),
    ];
    let candidate = vec![record("dataset/new.bin", 7, 5)];

    let diff = diff_index_snapshots(&base, &candidate).unwrap();

    assert_eq!(diff.summary.files_added, 1);
    assert_eq!(diff.summary.files_deleted, 2);
    assert_eq!(diff.summary.files_renamed, 0);
}

fn record(path: &str, manifest: u8, total_size: u64) -> FileRecord {
    FileRecord::new(
        LogicalPath::parse(path).unwrap(),
        ManifestId::from_bytes([manifest; 32]),
        total_size,
        u64::from(total_size > 0),
    )
    .unwrap()
}
