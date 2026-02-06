use crate::Hash;
use futures::future::join_all;
use rogue_logging::Failure;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error as ThisError;
use tokio::fs::{copy, create_dir_all, read_dir};
use tracing::{debug, trace};

/// File storage table with chunked directories.
///
/// - Files are stored by key of type `Hash<K>`
/// - Chunk directories are determined by truncating the key to a `Hash<C>`
/// - Files are copied into the storage directory
pub struct FileTable<const K: usize, const C: usize> {
    /// Directory for storing the files.
    pub(crate) directory: PathBuf,
    /// File extension for stored files.
    pub(crate) extension: String,
}

impl<const K: usize, const C: usize> FileTable<K, C> {
    /// Create a new [`FileTable`].
    #[must_use]
    pub fn new(directory: impl Into<PathBuf>, extension: impl Into<String>) -> Self {
        Self {
            directory: directory.into(),
            extension: extension.into(),
        }
    }

    /// Get the path to the file.
    fn get_path(&self, hash: Hash<K>) -> PathBuf {
        let chunk_hash: Hash<C> = get_chunk_hash(hash);
        self.directory
            .join(chunk_hash.to_hex())
            .join(format!("{hash}.{}", self.extension))
    }
}

impl<const K: usize, const C: usize> FileTable<K, C> {
    /// Get file path by hash.
    ///
    /// Returns `None` if the item is not found.
    #[must_use]
    pub fn get(&self, hash: Hash<K>) -> Option<PathBuf> {
        let path = self.get_path(hash);
        let found = path.is_file();
        trace!(hash = %hash, found, "Get file");
        found.then_some(path)
    }

    /// Get all file paths.
    ///
    /// Items are unsorted.
    pub async fn get_all(&self) -> Result<BTreeMap<Hash<K>, PathBuf>, Failure<FileTableAction>> {
        let mut paths = BTreeMap::new();
        let dir_path = self.directory.clone();
        let mut parent_dir = read_dir(&self.directory)
            .await
            .map_err(Failure::wrap_with_path(FileTableAction::ReadDir, &dir_path))?;
        while let Some(entry) = parent_dir
            .next_entry()
            .await
            .map_err(Failure::wrap(FileTableAction::ReadEntry))?
        {
            let path = entry.path();
            if !path.is_dir() {
                trace!("Skipping non-chunk directory: {}", path.display());
                continue;
            }
            let chunk_path = path.clone();
            let mut chunk_dir = read_dir(&path).await.map_err(Failure::wrap_with_path(
                FileTableAction::ReadChunkDir,
                &chunk_path,
            ))?;
            while let Some(entry) = chunk_dir
                .next_entry()
                .await
                .map_err(Failure::wrap(FileTableAction::ReadChunkEntry))?
            {
                let path = entry.path();
                let extension = path
                    .extension()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                if !path.is_file() || extension != self.extension {
                    trace!("Skipping non-chunk file: {}", path.display());
                    continue;
                }
                let Some(stem) = path.file_stem() else {
                    trace!("File does not have a stem: {}", path.display());
                    continue;
                };
                let Ok(hash) = Hash::from_string(stem.to_string_lossy().as_ref()) else {
                    trace!("File stem is not a hash: {}", path.display());
                    continue;
                };
                paths.insert(hash, path);
            }
        }
        trace!(count = paths.len(), "Get all files");
        Ok(paths)
    }
}

impl<const K: usize, const C: usize> FileTable<K, C> {
    /// Copy a file into storage.
    pub async fn set(
        &self,
        hash: Hash<K>,
        path: impl AsRef<Path>,
    ) -> Result<(), Failure<FileTableAction>> {
        let path = path.as_ref();
        let stored_path = self.get_path(hash);
        let stored_dir = stored_path
            .parent()
            .expect("stored path should have a parent");
        if !stored_dir.exists() {
            trace!(path = %stored_dir.display(), "Creating chunk directory");
            create_dir_all(stored_dir)
                .await
                .map_err(Failure::wrap_with_path(
                    FileTableAction::CreateDir,
                    stored_dir,
                ))
                .map_err(Failure::wrap(FileTableAction::Set))?;
        }
        debug!(hash = %hash, from = %path.display(), to = %stored_path.display(), "Copying file");
        copy(path, &stored_path)
            .await
            .map_err(Failure::wrap_with_path(
                FileTableAction::CopyFile,
                &stored_path,
            ))
            .map_err(Failure::wrap(FileTableAction::Set))?;
        Ok(())
    }

    /// Copy multiple files into storage.
    ///
    /// Existing files are replaced.
    pub async fn set_many(
        &self,
        items: BTreeMap<Hash<K>, PathBuf>,
    ) -> Result<(), Failure<FileTableAction>> {
        let count = items.len();
        trace!(count, "Set many files");
        let tasks: Vec<_> = items
            .into_iter()
            .map(|(hash, path)| self.set(hash, path))
            .collect();
        let results = join_all(tasks).await;
        let (successes, errors): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
        if errors.is_empty() {
            trace!(succeeded = count, "Set many files complete");
            Ok(())
        } else {
            let ok_count = successes.len();
            let error_count = errors.len();
            trace!(
                succeeded = ok_count,
                failed = error_count,
                "Set many files complete"
            );
            let mut failure = Failure::from_action(FileTableAction::SetMany)
                .with("succeeded", ok_count.to_string())
                .with("failed", error_count.to_string());
            for error in errors.into_iter().filter_map(Result::err) {
                failure = failure.with_related(error);
            }
            Err(failure)
        }
    }
}

/// Get the chunk hash from [`hash`]
fn get_chunk_hash<const K: usize, const C: usize>(hash: Hash<K>) -> Hash<C> {
    hash.truncate::<C>().expect("should be able to truncate")
}

/// Action being performed when a [`Failure<FileTableAction>`] occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum FileTableAction {
    #[error("set file")]
    Set,
    #[error("read directory")]
    ReadDir,
    #[error("read entry")]
    ReadEntry,
    #[error("read chunk directory")]
    ReadChunkDir,
    #[error("read chunk entry")]
    ReadChunkEntry,
    #[error("create directory")]
    CreateDir,
    #[error("copy file")]
    CopyFile,
    #[error("set files")]
    SetMany,
}
