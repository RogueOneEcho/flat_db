use crate::Hash;
use futures::future;
use miette::Diagnostic;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::error::Error;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{fmt, io};
use thiserror::Error as ThisError;
use tokio::fs::{OpenOptions, read, read_dir, remove_file, write};
use tokio::task;
use tokio::time::sleep;
use tracing::{debug, trace};

const CHUNK_FILE_EXTENSION: &str = "yml";
const LOCK_ACQUIRE_SLEEP_MILLIS: u64 = 50;
const LOCK_ACQUIRE_TIMEOUT: u64 = 2;
const LOCK_FILE_EXTENSION: &str = "lock";

/// Key-value table with chunked file storage.
///
/// - Items of type `T` are stored by key of type `Hash<K>`
/// - Get and set operations are performed directly on the file system
/// - Chunks are determined by truncating the key to a `Hash<C>`
/// - All items in a chunk are serialized to a single YAML file
/// - Write operations are protected by lock files
pub struct Table<const K: usize, const C: usize, T> {
    /// Directory for storing the data.
    pub(crate) directory: PathBuf,
    /// Marker for the item type.
    pub phantom: PhantomData<T>,
}

impl<const K: usize, const C: usize, T> Table<K, C, T> {
    /// Create a new [`Table`]
    #[must_use]
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            phantom: PhantomData,
        }
    }

    /// Get the path to the chunk file.
    fn get_chunk_path(&self, hash: Hash<C>) -> PathBuf {
        self.directory
            .join(format!("{hash}.{CHUNK_FILE_EXTENSION}"))
    }
}

impl<const K: usize, const C: usize, T> Default for Table<K, C, T> {
    fn default() -> Self {
        Self::new(PathBuf::new())
    }
}

impl<const K: usize, const C: usize, T> Table<K, C, T>
where
    T: Clone + DeserializeOwned,
{
    /// Get an item by hash.
    ///
    /// Returns `None` if the item is not found.
    pub async fn get(&self, hash: Hash<K>) -> Result<Option<T>, TableError> {
        let chunk_path = self.get_chunk_path(get_chunk_hash(hash));
        if chunk_path.exists() {
            let chunk = read_chunk::<K, C, T>(&chunk_path).await?;
            let item = chunk.get(&hash).cloned();
            trace!(hash = %hash, found = item.is_some(), "Get item");
            Ok(item)
        } else {
            trace!(hash = %hash, found = false, "Get item");
            Ok(None)
        }
    }

    /// Get all items.
    ///
    /// Items are unsorted.
    pub async fn get_all(&self) -> Result<BTreeMap<Hash<K>, T>, TableError> {
        let mut items = BTreeMap::new();
        let dir_path = self.directory.clone();
        let mut dir = read_dir(&self.directory)
            .await
            .map_err(|source| TableError::io(TableOperation::ReadDir, Some(dir_path), source))?;
        while let Some(entry) = dir.next_entry().await.map_err(|source| {
            TableError::io(
                TableOperation::ReadEntry,
                Some(self.directory.clone()),
                source,
            )
        })? {
            let path = entry.path();
            let extension = path
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if !path.is_file() || extension != CHUNK_FILE_EXTENSION {
                trace!("Skipping non-chunk file: {}", path.display());
                continue;
            }
            let chunk = read_chunk::<K, C, T>(&path).await?;
            items.extend(chunk);
        }
        trace!(count = items.len(), "Get all items");
        Ok(items)
    }
}

impl<const K: usize, const C: usize, T> Table<K, C, T>
where
    T: Clone + Send + Serialize + DeserializeOwned + 'static,
{
    /// Add or replace an item.
    pub async fn set(&self, hash: Hash<K>, item: T) -> Result<(), TableError> {
        trace!(hash = %hash, "Set item");
        let chunk_path = self.get_chunk_path(get_chunk_hash(hash));
        let lock = acquire_lock(&chunk_path).await?;
        let mut chunk = if chunk_path.exists() {
            read_chunk::<K, C, T>(&chunk_path).await?
        } else {
            BTreeMap::new()
        };
        chunk.insert(hash, item.clone());
        write_chunk::<K, C, T>(chunk_path, chunk).await?;
        release_lock(lock).await?;
        Ok(())
    }

    /// Add many items.
    ///
    /// If `replace` is true then existing items are replaced
    ///
    /// Items are chunked together to minimize IO operations.
    ///
    /// Returns the number of items added
    pub async fn set_many(
        &self,
        items: BTreeMap<Hash<K>, T>,
        replace: bool,
    ) -> Result<usize, TableError> {
        let item_count = items.len();
        let chunks = group_by_chunk(items);
        let chunk_count = chunks.len();
        trace!(
            items = item_count,
            chunks = chunk_count,
            replace,
            "Set many items"
        );
        let futures = chunks.into_iter().map(|(chunk_hash, new_chunk)| {
            let chunk_path = self.get_chunk_path(chunk_hash);
            task::spawn(
                async move { update_chunk::<K, C, T>(chunk_path, new_chunk, replace).await },
            )
        });
        let results = future::join_all(futures).await;
        let mut added = 0;
        let mut errors = Vec::new();
        for result in results {
            match result {
                Ok(Ok(count)) => added += count,
                Ok(Err(e)) => errors.push(e),
                Err(source) => errors.push(TableError::join(source)),
            }
        }
        if errors.is_empty() {
            trace!(added, "Set many items complete");
            Ok(added)
        } else {
            let succeeded = chunk_count - errors.len();
            let failed = errors.len();
            trace!(succeeded, failed, "Set many items complete");
            Err(TableError::batch(succeeded, failed, errors))
        }
    }

    /// Remove an item.
    pub async fn remove(&self, hash: Hash<K>) -> Result<Option<T>, TableError> {
        let chunk_path = self.get_chunk_path(get_chunk_hash(hash));
        let lock = acquire_lock(&chunk_path).await?;
        let mut chunk = if chunk_path.exists() {
            read_chunk::<K, C, T>(&chunk_path).await?
        } else {
            BTreeMap::new()
        };
        let item = chunk.remove(&hash);
        if item.is_some() {
            write_chunk::<K, C, T>(chunk_path, chunk).await?;
        }
        release_lock(lock).await?;
        trace!(hash = %hash, found = item.is_some(), "Remove item");
        Ok(item)
    }
}

/// Get the chunk hash from [`hash`]
fn get_chunk_hash<const K: usize, const C: usize>(hash: Hash<K>) -> Hash<C> {
    hash.truncate::<C>().expect("should be able to truncate")
}

fn group_by_chunk<const K: usize, const C: usize, T>(
    items: BTreeMap<Hash<K>, T>,
) -> BTreeMap<Hash<C>, BTreeMap<Hash<K>, T>> {
    let mut chunks: BTreeMap<Hash<C>, BTreeMap<Hash<K>, T>> = BTreeMap::new();
    for (hash, item) in items {
        let chunk_hash = get_chunk_hash(hash);
        chunks.entry(chunk_hash).or_insert_with(|| BTreeMap::new());
        chunks
            .get_mut(&chunk_hash)
            .expect("should be created in not exist")
            .insert(hash, item);
    }
    chunks
}

/// Read a chunk from a file.
async fn read_chunk<const K: usize, const C: usize, T>(
    path: &PathBuf,
) -> Result<BTreeMap<Hash<K>, T>, TableError>
where
    T: DeserializeOwned,
{
    debug!(path = %path.display(), "Reading chunk");
    let bytes = read(path)
        .await
        .map_err(|source| TableError::io(TableOperation::ReadChunk, Some(path.clone()), source))?;
    serde_yaml::from_slice(&bytes)
        .map_err(|source| TableError::yaml(TableOperation::Deserialize, Some(path.clone()), source))
}

/// Write a chunk to a file
async fn write_chunk<const K: usize, const C: usize, T>(
    path: PathBuf,
    chunk: BTreeMap<Hash<K>, T>,
) -> Result<(), TableError>
where
    T: Serialize,
{
    debug!(path = %path.display(), "Writing chunk");
    let yaml = serde_yaml::to_string(&chunk).map_err(|source| {
        TableError::yaml(TableOperation::Serialize, Some(path.clone()), source)
    })?;
    write(&path, yaml)
        .await
        .map_err(|source| TableError::io(TableOperation::WriteChunk, Some(path), source))?;
    Ok(())
}

/// Update the items in a chunk
///
/// If `replace` is true then existing items are replaced
async fn update_chunk<const K: usize, const C: usize, T>(
    chunk_path: PathBuf,
    new_chunk: BTreeMap<Hash<K>, T>,
    replace: bool,
) -> Result<usize, TableError>
where
    T: DeserializeOwned + Serialize,
{
    let mut added = 0;
    let lock = acquire_lock(&chunk_path).await?;
    let mut chunk = if chunk_path.exists() {
        read_chunk::<K, C, T>(&chunk_path).await?
    } else {
        BTreeMap::new()
    };
    for (hash, item) in new_chunk {
        if replace || !chunk.contains_key(&hash) {
            chunk.insert(hash, item);
            added += 1;
        }
    }
    write_chunk::<K, C, T>(chunk_path, chunk).await?;
    release_lock(lock).await?;
    Ok(added)
}

/// Acquire a lock
///
/// If the lock is already in use then wait
async fn acquire_lock(path: &Path) -> Result<PathBuf, TableError> {
    let start = Instant::now();
    let timeout = Duration::from_secs(LOCK_ACQUIRE_TIMEOUT);
    let mut lock: PathBuf = path.to_path_buf();
    lock.set_extension(LOCK_FILE_EXTENSION);
    loop {
        if OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock)
            .await
            .is_ok()
        {
            trace!(path = %lock.display(), "Lock acquired");
            return Ok(lock);
        }
        if start.elapsed() > timeout {
            return Err(TableError::io(
                TableOperation::AcquireLock,
                Some(lock),
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Exceeded timeout for acquiring lock",
                ),
            ));
        }
        trace!(path = %lock.display(), "Lock busy, waiting");
        sleep(Duration::from_millis(LOCK_ACQUIRE_SLEEP_MILLIS)).await;
    }
}

async fn release_lock(path: PathBuf) -> Result<(), TableError> {
    remove_file(&path).await.map_err(|source| {
        TableError::io(TableOperation::ReleaseLock, Some(path.clone()), source)
    })?;
    trace!(path = %path.display(), "Lock released");
    Ok(())
}

/// Operation being performed when a [`TableError`] occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum TableOperation {
    #[error("read chunk")]
    ReadChunk,
    #[error("write chunk")]
    WriteChunk,
    #[error("read directory")]
    ReadDir,
    #[error("read entry")]
    ReadEntry,
    #[error("acquire lock")]
    AcquireLock,
    #[error("release lock")]
    ReleaseLock,
    #[error("serialize")]
    Serialize,
    #[error("deserialize")]
    Deserialize,
    #[error("update multiple chunks")]
    JoinTask,
    #[error("set items")]
    SetMany,
}

/// Errors returned by [`Table`] operations.
#[derive(Debug)]
pub struct TableError {
    pub operation: TableOperation,
    pub path: Option<PathBuf>,
    source: ErrorSource,
}

#[derive(Debug)]
enum ErrorSource {
    Io(io::Error),
    Yaml(serde_yaml::Error),
    Join(task::JoinError),
    Batch {
        succeeded: usize,
        failed: usize,
        errors: Vec<TableError>,
    },
}

impl TableError {
    fn io(operation: TableOperation, path: Option<PathBuf>, source: io::Error) -> Self {
        Self {
            operation,
            path,
            source: ErrorSource::Io(source),
        }
    }

    fn yaml(operation: TableOperation, path: Option<PathBuf>, source: serde_yaml::Error) -> Self {
        Self {
            operation,
            path,
            source: ErrorSource::Yaml(source),
        }
    }

    fn join(source: task::JoinError) -> Self {
        Self {
            operation: TableOperation::JoinTask,
            path: None,
            source: ErrorSource::Join(source),
        }
    }

    fn batch(succeeded: usize, failed: usize, errors: Vec<TableError>) -> Self {
        Self {
            operation: TableOperation::SetMany,
            path: None,
            source: ErrorSource::Batch {
                succeeded,
                failed,
                errors,
            },
        }
    }
}

impl fmt::Display for TableError {
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

impl Error for TableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.source {
            ErrorSource::Io(e) => Some(e),
            ErrorSource::Yaml(e) => Some(e),
            ErrorSource::Join(e) => Some(e),
            ErrorSource::Batch { .. } => None,
        }
    }
}

impl Diagnostic for TableError {
    fn code<'a>(&'a self) -> Option<Box<dyn fmt::Display + 'a>> {
        Some(Box::new(format!(
            "{}::Table::{:?}",
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
            ErrorSource::Io(_) | ErrorSource::Yaml(_) | ErrorSource::Join(_) => None,
        }
    }
}
