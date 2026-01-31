use crate::Hash;
use futures::future::join_all;
use log::trace;
use rogue_logging::Error;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tokio::fs::{copy, create_dir_all, read_dir};

/// File storage table with chunked directories.
///
/// - Files are stored by key of type [`Hash<K>`]
/// - Chunk directories are determined by truncating the key to a [`Hash<C>`]
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
    pub fn get(&self, hash: Hash<K>) -> Result<Option<PathBuf>, Error> {
        let path = self.get_path(hash);
        if path.is_file() {
            Ok(Some(path))
        } else {
            Ok(None)
        }
    }

    /// Get all file paths.
    ///
    /// Items are unsorted.
    pub async fn get_all(&self) -> Result<BTreeMap<Hash<K>, PathBuf>, Error> {
        let mut paths = BTreeMap::new();
        let mut parent_dir = read_dir(&self.directory).await.map_err(|e| Error {
            action: "read directory".to_owned(),
            message: e.to_string(),
            domain: Some("file system".to_owned()),
            ..Error::default()
        })?;
        while let Some(entry) = parent_dir.next_entry().await.map_err(|e| Error {
            action: "read entry".to_owned(),
            message: e.to_string(),
            domain: Some("file system".to_owned()),
            ..Error::default()
        })? {
            let path = entry.path();
            if !path.is_dir() {
                trace!("Skipping non-chunk directory: {}", path.display());
                continue;
            }
            let mut chunk_dir = read_dir(path).await.map_err(|e| Error {
                action: "read chunk directory".to_owned(),
                message: e.to_string(),
                domain: Some("file system".to_owned()),
                ..Error::default()
            })?;
            while let Some(entry) = chunk_dir.next_entry().await.map_err(|e| Error {
                action: "read chunk entry".to_owned(),
                message: e.to_string(),
                domain: Some("file system".to_owned()),
                ..Error::default()
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
        Ok(paths)
    }
}

#[allow(dead_code)]
impl<const K: usize, const C: usize> FileTable<K, C> {
    /// Copy a file into storage.
    pub async fn set(&self, hash: Hash<K>, path: PathBuf) -> Result<(), Error> {
        let stored_path = self.get_path(hash);
        let stored_dir = stored_path
            .parent()
            .expect("stored path should have a parent");
        if !stored_dir.exists() {
            create_dir_all(stored_dir).await.map_err(|e| Error {
                action: "create directory".to_owned(),
                message: format!("{}\n{e}", stored_dir.display()),
                domain: Some("file system".to_owned()),
                ..Error::default()
            })?;
        }
        copy(path, stored_path).await.map_err(|e| Error {
            action: "copy file".to_owned(),
            message: e.to_string(),
            domain: Some("file system".to_owned()),
            ..Error::default()
        })?;
        Ok(())
    }

    /// Copy multiple files into storage.
    ///
    /// Existing files are replaced.
    pub async fn set_many(&self, items: BTreeMap<Hash<K>, PathBuf>) -> Result<(), Error> {
        let tasks: Vec<_> = items
            .into_iter()
            .map(|(hash, path)| self.set(hash, path))
            .collect();
        let results = join_all(tasks).await;
        let (successes, errors): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
        if errors.is_empty() {
            Ok(())
        } else {
            let ok_count = successes.len();
            let error_count = errors.len();
            let error_messages = errors
                .into_iter()
                .fold(String::new(), |mut output, result| {
                    if let Err(e) = result {
                        output.push_str(&e.display());
                        output.push('\n');
                    }
                    output
                });
            Err(Error {
                action: "set many files".to_owned(),
                message: format!(
                    "{ok_count} succeeded and {error_count} failed:\n{error_messages}",
                ),
                domain: Some("file system".to_owned()),
                ..Error::default()
            })
        }
    }
}

/// Get the chunk hash from [`hash`]
fn get_chunk_hash<const K: usize, const C: usize>(hash: Hash<K>) -> Hash<C> {
    hash.truncate::<C>().expect("should be able to truncate")
}
