use crate::Hash;
use futures::future::join_all;
use miette::Diagnostic;
use std::collections::BTreeMap;
use std::error::Error;
use std::path::PathBuf;
use std::{fmt, io};
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
    pub fn new(directory: PathBuf, extension: String) -> Self {
        Self {
            directory,
            extension,
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
    pub async fn get_all(&self) -> Result<BTreeMap<Hash<K>, PathBuf>, FileTableError> {
        let mut paths = BTreeMap::new();
        let dir_path = self.directory.clone();
        let mut parent_dir = read_dir(&self.directory).await.map_err(|source| {
            FileTableError::new(FileTableOperation::ReadDir, Some(dir_path), source)
        })?;
        while let Some(entry) = parent_dir.next_entry().await.map_err(|source| {
            FileTableError::new(
                FileTableOperation::ReadEntry,
                Some(self.directory.clone()),
                source,
            )
        })? {
            let path = entry.path();
            if !path.is_dir() {
                trace!("Skipping non-chunk directory: {}", path.display());
                continue;
            }
            let chunk_path = path.clone();
            let mut chunk_dir = read_dir(&path).await.map_err(|source| {
                FileTableError::new(FileTableOperation::ReadChunkDir, Some(chunk_path), source)
            })?;
            while let Some(entry) = chunk_dir.next_entry().await.map_err(|source| {
                FileTableError::new(
                    FileTableOperation::ReadChunkEntry,
                    Some(path.clone()),
                    source,
                )
            })? {
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
    pub async fn set(&self, hash: Hash<K>, path: PathBuf) -> Result<(), FileTableError> {
        let stored_path = self.get_path(hash);
        let stored_dir = stored_path
            .parent()
            .expect("stored path should have a parent");
        if !stored_dir.exists() {
            trace!(path = %stored_dir.display(), "Creating chunk directory");
            create_dir_all(stored_dir).await.map_err(|source| {
                FileTableError::new(
                    FileTableOperation::CreateDir,
                    Some(stored_dir.to_path_buf()),
                    source,
                )
            })?;
        }
        debug!(hash = %hash, from = %path.display(), to = %stored_path.display(), "Copying file");
        copy(&path, &stored_path).await.map_err(|source| {
            FileTableError::new(FileTableOperation::CopyFile, Some(path), source)
        })?;
        Ok(())
    }

    /// Copy multiple files into storage.
    ///
    /// Existing files are replaced.
    pub async fn set_many(&self, items: BTreeMap<Hash<K>, PathBuf>) -> Result<(), FileTableError> {
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
            let inner_errors: Vec<_> = errors.into_iter().filter_map(Result::err).collect();
            Err(FileTableError::new_batch(
                ok_count,
                error_count,
                inner_errors,
            ))
        }
    }
}

/// Get the chunk hash from [`hash`]
fn get_chunk_hash<const K: usize, const C: usize>(hash: Hash<K>) -> Hash<C> {
    hash.truncate::<C>().expect("should be able to truncate")
}

/// Operation being performed when a [`FileTableError`] occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum FileTableOperation {
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

/// Errors returned by [`FileTable`] operations.
#[derive(Debug)]
pub struct FileTableError {
    pub operation: FileTableOperation,
    pub path: Option<PathBuf>,
    source: ErrorSource,
}

#[derive(Debug)]
enum ErrorSource {
    Io(io::Error),
    Batch {
        succeeded: usize,
        failed: usize,
        errors: Vec<FileTableError>,
    },
}

impl FileTableError {
    fn new(operation: FileTableOperation, path: Option<PathBuf>, source: io::Error) -> Self {
        Self {
            operation,
            path,
            source: ErrorSource::Io(source),
        }
    }

    fn new_batch(succeeded: usize, failed: usize, errors: Vec<FileTableError>) -> Self {
        Self {
            operation: FileTableOperation::SetMany,
            path: None,
            source: ErrorSource::Batch {
                succeeded,
                failed,
                errors,
            },
        }
    }
}

impl fmt::Display for FileTableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Failed to {}", self.operation)?;
        if let Some(path) = &self.path {
            write!(f, "\nPath: {}", path.display())?;
        }
        if let ErrorSource::Batch {
            succeeded, failed, ..
        } = &self.source
        {
            write!(f, "\n{succeeded} succeeded, {failed} failed")?;
        }
        Ok(())
    }
}

impl Error for FileTableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.source {
            ErrorSource::Io(e) => Some(e),
            ErrorSource::Batch { .. } => None,
        }
    }
}

impl Diagnostic for FileTableError {
    fn code<'a>(&'a self) -> Option<Box<dyn fmt::Display + 'a>> {
        Some(Box::new(format!(
            "{}::FileTable::{:?}",
            env!("CARGO_PKG_NAME"),
            self.operation
        )))
    }

    fn related<'a>(&'a self) -> Option<Box<dyn Iterator<Item = &'a dyn Diagnostic> + 'a>> {
        match &self.source {
            ErrorSource::Batch { errors, .. } => {
                #[expect(clippy::as_conversions)]
                let iter = errors.iter().map(|e| e as &dyn Diagnostic);
                Some(Box::new(iter))
            }
            ErrorSource::Io(_) => None,
        }
    }
}
