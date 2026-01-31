use crate::Hash;
use colored::Colorize;
use futures::future;
use log::trace;
use miette::Diagnostic;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{fmt, io};
use thiserror::Error as ThisError;
use tokio::fs::{OpenOptions, read_dir, remove_file};
use tokio::task;
use tokio::time::sleep;

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
    pub fn get(&self, hash: Hash<K>) -> Result<Option<T>, TableError> {
        let chunk_path = self.get_chunk_path(get_chunk_hash(hash));
        if chunk_path.exists() {
            let chunk = read_chunk::<K, C, T>(&chunk_path)?;
            Ok(chunk.get(&hash).cloned())
        } else {
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
            .map_err(|source| TableError::new(TableOperation::ReadDir, Some(dir_path), source))?;
        while let Some(entry) = dir.next_entry().await.map_err(|source| {
            TableError::new(
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
            let chunk = read_chunk::<K, C, T>(&path)?;
            items.extend(chunk);
        }
        Ok(items)
    }
}

impl<const K: usize, const C: usize, T> Table<K, C, T>
where
    T: Clone + Send + Serialize + DeserializeOwned + 'static,
{
    /// Add or replace an item.
    pub async fn set(&self, hash: Hash<K>, item: T) -> Result<(), TableError> {
        let chunk_path = self.get_chunk_path(get_chunk_hash(hash));
        let lock = acquire_lock(&chunk_path).await?;
        let mut chunk = if chunk_path.exists() {
            read_chunk::<K, C, T>(&chunk_path)?
        } else {
            BTreeMap::new()
        };
        chunk.insert(hash, item.clone());
        write_chunk::<K, C, T>(chunk_path, chunk)?;
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
        let chunks = group_by_chunk(items);
        let futures = chunks.into_iter().map(|(chunk_hash, new_chunk)| {
            let chunk_path = self.get_chunk_path(chunk_hash);
            task::spawn(
                async move { update_chunk::<K, C, T>(chunk_path, new_chunk, replace).await },
            )
        });
        let mut added = 0;
        for result in future::join_all(futures).await {
            added += result
                .map_err(|source| TableError::new(TableOperation::JoinTask, None, source))??;
        }
        Ok(added)
    }

    /// Remove an item.
    pub async fn remove(&self, hash: Hash<K>) -> Result<Option<T>, TableError> {
        let chunk_path = self.get_chunk_path(get_chunk_hash(hash));
        let lock = acquire_lock(&chunk_path).await?;
        let mut chunk = if chunk_path.exists() {
            read_chunk::<K, C, T>(&chunk_path)?
        } else {
            BTreeMap::new()
        };
        let item = chunk.remove(&hash);
        if item.is_some() {
            write_chunk::<K, C, T>(chunk_path, chunk)?;
        }
        release_lock(lock).await?;
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
fn read_chunk<const K: usize, const C: usize, T>(
    path: &PathBuf,
) -> Result<BTreeMap<Hash<K>, T>, TableError>
where
    T: DeserializeOwned,
{
    if !path.exists() || !path.is_file() {
        return Err(TableError::new(
            TableOperation::ReadChunk,
            Some(path.clone()),
            io::Error::new(io::ErrorKind::NotFound, "Chunk file does not exist"),
        ));
    }
    trace!("{} chunk file: {}", "Reading".bold(), path.display());
    let file = File::open(path)
        .map_err(|source| TableError::new(TableOperation::OpenChunk, Some(path.clone()), source))?;
    let reader = BufReader::new(file);
    serde_yaml::from_reader(reader)
        .map_err(|source| TableError::new(TableOperation::Deserialize, Some(path.clone()), source))
}

/// Write a chunk to a file
fn write_chunk<const K: usize, const C: usize, T>(
    path: PathBuf,
    chunk: BTreeMap<Hash<K>, T>,
) -> Result<(), TableError>
where
    T: Serialize,
{
    trace!("{} chunk file: {}", "Writing".bold(), path.display());
    let file = File::create(&path).map_err(|source| {
        TableError::new(TableOperation::CreateChunk, Some(path.clone()), source)
    })?;
    let mut writer = BufWriter::new(file);
    serde_yaml::to_writer(&mut writer, &chunk)
        .map_err(|source| TableError::new(TableOperation::Serialize, Some(path.clone()), source))?;
    writer
        .flush()
        .map_err(|source| TableError::new(TableOperation::FlushChunk, Some(path), source))?;
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
        read_chunk::<K, C, T>(&chunk_path)?
    } else {
        BTreeMap::new()
    };
    for (hash, item) in new_chunk {
        if replace || !chunk.contains_key(&hash) {
            chunk.insert(hash, item);
            added += 1;
        }
    }
    write_chunk::<K, C, T>(chunk_path, chunk)?;
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
            return Ok(lock);
        }
        if start.elapsed() > timeout {
            return Err(TableError::new(
                TableOperation::AcquireLock,
                Some(lock),
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Exceeded timeout for acquiring lock",
                ),
            ));
        }
        sleep(Duration::from_millis(LOCK_ACQUIRE_SLEEP_MILLIS)).await;
    }
}

async fn release_lock(path: PathBuf) -> Result<(), TableError> {
    remove_file(&path)
        .await
        .map_err(|source| TableError::new(TableOperation::ReleaseLock, Some(path), source))
}

/// Operation being performed when a [`TableError`] occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum TableOperation {
    #[error("open chunk")]
    OpenChunk,
    #[error("create chunk")]
    CreateChunk,
    #[error("read chunk")]
    ReadChunk,
    #[error("flush chunk")]
    FlushChunk,
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
}

/// Errors returned by [`Table`] operations.
#[derive(Debug, Diagnostic)]
pub struct TableError {
    pub operation: TableOperation,
    pub path: Option<PathBuf>,
    pub source: Box<dyn Error + Send + Sync>,
}

impl TableError {
    fn new(
        operation: TableOperation,
        path: Option<PathBuf>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            operation,
            path,
            source: Box::new(source),
        }
    }
}

impl fmt::Display for TableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Failed to {}", self.operation)?;
        if let Some(path) = &self.path {
            write!(f, "\nPath: {}", path.display())?;
        }
        Ok(())
    }
}

impl Error for TableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}
