use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

use neoengram_core::{
    validate_index_snapshot, Commit, CommitId, Directory, DirectoryEntry, DirectoryId, FileRecord,
    IndexVersion, ManifestId, PathComponent,
};
use serde::{Deserialize, Serialize};

use crate::{
    EngineError, EngineResult, ErrorCode, IndexSnapshotReader, PageRequest, ProgressEvent,
    ProgressPhase, ProgressSink, ProgressUnit, MAX_PAGE_SIZE,
};

/// Input to the deterministic Index-to-Commit graph builder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildCommitGraphRequest {
    pub expected_index_version: IndexVersion,
    pub parent: Option<CommitId>,
    pub message: String,
    pub created_at_unix_ms: u64,
}

/// One immutable Directory and its canonical identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryObject {
    pub id: DirectoryId,
    pub directory: Directory,
}

/// Deterministic graph candidate. Building it does not publish metadata or a mutable ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildCommitGraphResult {
    pub source_index_version: IndexVersion,
    /// Deduplicated child-first Directory objects, with the root object last.
    pub directories: Vec<DirectoryObject>,
    pub commit_id: CommitId,
    pub commit: Commit,
    pub file_count: u64,
}

/// Final publication request. The adapter owns the mutable compare-and-swap boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishCommitRequest {
    pub graph: BuildCommitGraphResult,
    pub expected_parent: Option<CommitId>,
}

/// Result returned only after the publisher has made the Commit authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishCommitResult {
    pub commit_id: CommitId,
}

/// Standalone and managed runtimes provide distinct authoritative publishers.
pub trait CommitPublisher: Debug + Send + Sync {
    fn publish(&self, request: &PublishCommitRequest) -> EngineResult<PublishCommitResult>;
}

/// Builds a canonical Directory DAG and Commit without publishing either one.
pub fn build_commit_graph(
    request: &BuildCommitGraphRequest,
    index: &dyn IndexSnapshotReader,
    progress: &dyn ProgressSink,
) -> EngineResult<BuildCommitGraphResult> {
    if index.version() != &request.expected_index_version {
        return Err(EngineError::new(
            ErrorCode::IndexVersionMismatch,
            "Index snapshot does not match the requested Commit base",
        )
        .with_context(
            "expected_revision",
            request.expected_index_version.revision.to_string(),
        )
        .with_context("actual_revision", index.version().revision.to_string()));
    }

    let mut files = Vec::new();
    let mut page_request = PageRequest::first(MAX_PAGE_SIZE)?;
    loop {
        let page = index.scan_files(None, &page_request)?;
        if page.items.len() > page_request.limit as usize {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "Index reader returned more records than requested",
            ));
        }
        files.extend(page.items);
        progress.emit(&ProgressEvent::new(
            ProgressPhase::BuildingCandidate,
            ProgressUnit::Files,
            u64::try_from(files.len()).map_err(|_| {
                EngineError::new(ErrorCode::Internal, "Index file count exceeds u64")
            })?,
        ))?;
        let Some(next) = page.next else {
            break;
        };
        if page_request.after.as_ref() == Some(&next) {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "Index reader returned a non-advancing cursor",
            ));
        }
        page_request = PageRequest::new(Some(next), MAX_PAGE_SIZE)?;
    }

    validate_index_snapshot(&files)?;
    let file_count = u64::try_from(files.len())
        .map_err(|_| EngineError::new(ErrorCode::Internal, "Index file count exceeds u64"))?;
    let mut root = PendingDirectory::default();
    for file in &files {
        root.insert(file)?;
    }

    let mut directories = Vec::new();
    let mut published = BTreeMap::new();
    let root_directory_id = root.seal(&mut directories, &mut published)?;
    let commit = Commit::new(
        root_directory_id,
        request.parent,
        request.message.clone(),
        request.created_at_unix_ms,
    )?;
    let commit_id = commit.canonical_id()?;
    progress.emit(&ProgressEvent::new(
        ProgressPhase::Prepared,
        ProgressUnit::Files,
        file_count,
    ))?;
    Ok(BuildCommitGraphResult {
        source_index_version: request.expected_index_version,
        directories,
        commit_id,
        commit,
        file_count,
    })
}

/// Delegates the final compare-and-swap to an explicit publisher after validating the graph.
pub fn publish_commit(
    request: &PublishCommitRequest,
    publisher: &dyn CommitPublisher,
) -> EngineResult<PublishCommitResult> {
    request.graph.commit.validate()?;
    if request.graph.commit.canonical_id()? != request.graph.commit_id {
        return Err(EngineError::new(
            ErrorCode::IntegrityViolation,
            "Commit graph ID does not match its canonical Commit",
        ));
    }
    if request.graph.commit.parent != request.expected_parent {
        return Err(EngineError::new(
            ErrorCode::Conflict,
            "Commit graph parent does not match publication precondition",
        ));
    }
    let mut directory_ids = BTreeSet::new();
    for object in &request.graph.directories {
        object.directory.validate()?;
        if object.directory.canonical_id()? != object.id {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "Directory graph object does not match its canonical ID",
            )
            .with_context("directory_id", object.id.to_string()));
        }
        if !directory_ids.insert(object.id) {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "Commit graph contains a duplicate Directory object",
            )
            .with_context("directory_id", object.id.to_string()));
        }
    }
    if !directory_ids.contains(&request.graph.commit.root_directory_id) {
        return Err(EngineError::new(
            ErrorCode::IntegrityViolation,
            "Commit graph does not contain its canonical root Directory",
        ));
    }
    for object in &request.graph.directories {
        for entry in &object.directory.entries {
            if let neoengram_core::DirectoryEntryTarget::Directory(child_id) = entry.target_id {
                if !directory_ids.contains(&child_id) {
                    return Err(EngineError::new(
                        ErrorCode::IntegrityViolation,
                        "Commit graph references an absent child Directory",
                    )
                    .with_context("directory_id", child_id.to_string()));
                }
            }
        }
    }
    let result = publisher.publish(request)?;
    if result.commit_id != request.graph.commit_id {
        return Err(EngineError::new(
            ErrorCode::IntegrityViolation,
            "Commit publisher returned a different Commit ID",
        ));
    }
    Ok(result)
}

#[derive(Debug, Default)]
struct PendingDirectory {
    entries: BTreeMap<PathComponent, PendingEntry>,
}

#[derive(Debug)]
enum PendingEntry {
    File {
        manifest_id: ManifestId,
        total_size: u64,
    },
    Directory(PendingDirectory),
}

impl PendingDirectory {
    fn insert(&mut self, file: &FileRecord) -> EngineResult<()> {
        let components = file
            .path
            .components()
            .map(PathComponent::parse)
            .collect::<Result<Vec<_>, _>>()?;
        self.insert_components(&components, file)
    }

    fn insert_components(
        &mut self,
        components: &[PathComponent],
        file: &FileRecord,
    ) -> EngineResult<()> {
        let (first, remaining) = components.split_first().ok_or_else(|| {
            EngineError::new(
                ErrorCode::InvalidPath,
                "Index contains an empty logical path",
            )
        })?;
        if remaining.is_empty() {
            if self
                .entries
                .insert(
                    first.clone(),
                    PendingEntry::File {
                        manifest_id: file.manifest_id,
                        total_size: file.total_size,
                    },
                )
                .is_some()
            {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "Index contains a duplicate or prefix-conflicting path",
                ));
            }
            return Ok(());
        }

        let entry = self
            .entries
            .entry(first.clone())
            .or_insert_with(|| PendingEntry::Directory(Self::default()));
        match entry {
            PendingEntry::Directory(directory) => directory.insert_components(remaining, file),
            PendingEntry::File { .. } => Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "Index file path is also an ancestor of another path",
            )),
        }
    }

    fn seal(
        self,
        output: &mut Vec<DirectoryObject>,
        published: &mut BTreeMap<DirectoryId, Directory>,
    ) -> EngineResult<DirectoryId> {
        let mut entries = Vec::with_capacity(self.entries.len());
        for (ordinal, (name, entry)) in self.entries.into_iter().enumerate() {
            let ordinal = u64::try_from(ordinal).map_err(|_| {
                EngineError::new(ErrorCode::Internal, "Directory entry count exceeds u64")
            })?;
            entries.push(match entry {
                PendingEntry::File {
                    manifest_id,
                    total_size,
                } => DirectoryEntry::file(ordinal, name, manifest_id, total_size),
                PendingEntry::Directory(directory) => {
                    let id = directory.seal(output, published)?;
                    DirectoryEntry::directory(ordinal, name, id)
                }
            });
        }
        let directory = Directory::new(entries)?;
        let id = directory.canonical_id()?;
        if let Some(existing) = published.get(&id) {
            if existing != &directory {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "canonical Directory ID collision",
                ));
            }
        } else {
            published.insert(id, directory.clone());
            output.push(DirectoryObject { id, directory });
        }
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use neoengram_core::{ContentDigest, LogicalPath};

    use super::*;
    use crate::{IndexEntry, Page, PageCursor};

    #[derive(Debug)]
    struct TestIndex {
        version: IndexVersion,
        files: Vec<FileRecord>,
    }

    impl IndexSnapshotReader for TestIndex {
        fn version(&self) -> &IndexVersion {
            &self.version
        }

        fn get_file(&self, path: &LogicalPath) -> EngineResult<Option<IndexEntry>> {
            Ok(self.files.iter().find(|file| &file.path == path).cloned())
        }

        fn scan_files(
            &self,
            _prefix: Option<&LogicalPath>,
            request: &PageRequest,
        ) -> EngineResult<Page<IndexEntry>> {
            request.validate()?;
            let start = request.after.as_ref().map_or(0, |cursor| {
                self.files
                    .partition_point(|file| file.path.as_str() <= cursor.as_str())
            });
            let end = start
                .saturating_add(request.limit as usize)
                .min(self.files.len());
            let items = self.files[start..end].to_vec();
            let next = (end < self.files.len())
                .then(|| items.last().expect("non-empty page"))
                .map(|file| PageCursor::new(file.path.as_str()))
                .transpose()?;
            Ok(Page { items, next })
        }
    }

    #[derive(Debug, Default)]
    struct RecordingPublisher(Mutex<Vec<CommitId>>);

    impl CommitPublisher for RecordingPublisher {
        fn publish(&self, request: &PublishCommitRequest) -> EngineResult<PublishCommitResult> {
            self.0.lock().unwrap().push(request.graph.commit_id);
            Ok(PublishCommitResult {
                commit_id: request.graph.commit_id,
            })
        }
    }

    fn index() -> TestIndex {
        let manifest_id = ManifestId::from_bytes([3; 32]);
        let files = ["left/file", "right/file"]
            .map(|path| {
                FileRecord::new(LogicalPath::parse(path).unwrap(), manifest_id, 0, 0).unwrap()
            })
            .to_vec();
        let version = IndexVersion::from_snapshot(7, &files).unwrap();
        TestIndex { version, files }
    }

    #[test]
    fn graph_builder_deduplicates_equal_subdirectories_and_separates_publication() {
        let index = index();
        let graph = build_commit_graph(
            &BuildCommitGraphRequest {
                expected_index_version: index.version,
                parent: None,
                message: "initial".to_owned(),
                created_at_unix_ms: 123,
            },
            &index,
            &crate::NoopProgressSink,
        )
        .unwrap();
        assert_eq!(graph.directories.len(), 2);
        let root = graph.directories.last().unwrap();
        assert_eq!(
            root.directory.entries[0].target_id,
            root.directory.entries[1].target_id
        );

        let publisher = RecordingPublisher::default();
        let result = publish_commit(
            &PublishCommitRequest {
                expected_parent: None,
                graph: graph.clone(),
            },
            &publisher,
        )
        .unwrap();
        assert_eq!(result.commit_id, graph.commit_id);
        assert_eq!(publisher.0.lock().unwrap().as_slice(), &[graph.commit_id]);
    }

    #[test]
    fn graph_builder_rejects_a_stale_snapshot() {
        let index = index();
        let error = build_commit_graph(
            &BuildCommitGraphRequest {
                expected_index_version: IndexVersion::new(8, ContentDigest::from_bytes([8; 32])),
                parent: None,
                message: "stale".to_owned(),
                created_at_unix_ms: 123,
            },
            &index,
            &crate::NoopProgressSink,
        )
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::IndexVersionMismatch);
    }
}
