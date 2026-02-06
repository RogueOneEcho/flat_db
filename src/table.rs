use crate::Hash;
use crate::lock_guard::acquire_lock;
use futures::future;
use rogue_logging::Failure;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use thiserror::Error as ThisError;
use tokio::fs::{read, read_dir, write};
use tokio::task;
use tracing::{debug, trace};

const CHUNK_FILE_EXTENSION: &str = "yml";

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
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
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
    pub async fn get(&self, hash: Hash<K>) -> Result<Option<T>, Failure<TableAction>> {
        let chunk_path = self.get_chunk_path(get_chunk_hash(hash));
        if chunk_path.exists() {
            let chunk = read_chunk::<K, C, T>(&chunk_path)
                .await
                .map_err(Failure::wrap(TableAction::Get))?;
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
    pub async fn get_all(&self) -> Result<BTreeMap<Hash<K>, T>, Failure<TableAction>> {
        let mut items = BTreeMap::new();
        let dir_path = self.directory.clone();
        let mut dir = read_dir(&self.directory)
            .await
            .map_err(Failure::wrap_with_path(TableAction::ReadDir, &dir_path))?;
        while let Some(entry) = dir
            .next_entry()
            .await
            .map_err(Failure::wrap(TableAction::ReadEntry))?
        {
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
            let chunk = read_chunk::<K, C, T>(&path)
                .await
                .map_err(Failure::wrap(TableAction::GetAll))?;
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
    pub async fn set(&self, hash: Hash<K>, item: T) -> Result<(), Failure<TableAction>> {
        trace!(hash = %hash, "Set item");
        let chunk_path = self.get_chunk_path(get_chunk_hash(hash));
        let _lock = acquire_lock(&chunk_path)
            .await
            .map_err(Failure::wrap(TableAction::Set))?;
        let mut chunk = if chunk_path.exists() {
            read_chunk::<K, C, T>(&chunk_path)
                .await
                .map_err(Failure::wrap(TableAction::Set))?
        } else {
            BTreeMap::new()
        };
        chunk.insert(hash, item.clone());
        write_chunk::<K, C, T>(&chunk_path, chunk)
            .await
            .map_err(Failure::wrap(TableAction::Set))?;
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
    ) -> Result<usize, Failure<TableAction>> {
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
                Err(source) => errors.push(Failure::new(TableAction::JoinTask, source)),
            }
        }
        if errors.is_empty() {
            trace!(added, "Set many items complete");
            Ok(added)
        } else {
            let succeeded = chunk_count - errors.len();
            let failed = errors.len();
            trace!(succeeded, failed, "Set many items complete");
            let mut failure = Failure::from_action(TableAction::SetMany)
                .with("succeeded", succeeded.to_string())
                .with("failed", failed.to_string());
            for error in errors {
                failure = failure.with_related(error);
            }
            Err(failure)
        }
    }

    /// Remove an item.
    pub async fn remove(&self, hash: Hash<K>) -> Result<Option<T>, Failure<TableAction>> {
        let chunk_path = self.get_chunk_path(get_chunk_hash(hash));
        let _lock = acquire_lock(&chunk_path)
            .await
            .map_err(Failure::wrap(TableAction::Remove))?;
        let mut chunk = if chunk_path.exists() {
            read_chunk::<K, C, T>(&chunk_path)
                .await
                .map_err(Failure::wrap(TableAction::Remove))?
        } else {
            BTreeMap::new()
        };
        let item = chunk.remove(&hash);
        if item.is_some() {
            write_chunk::<K, C, T>(&chunk_path, chunk)
                .await
                .map_err(Failure::wrap(TableAction::Remove))?;
        }
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
    path: impl AsRef<Path>,
) -> Result<BTreeMap<Hash<K>, T>, Failure<TableAction>>
where
    T: DeserializeOwned,
{
    let path = path.as_ref();
    debug!(path = %path.display(), "Reading chunk");
    let bytes = read(path)
        .await
        .map_err(Failure::wrap_with_path(TableAction::ReadChunk, path))?;
    serde_yaml::from_slice(&bytes).map_err(Failure::wrap_with_path(TableAction::Deserialize, path))
}

/// Write a chunk to a file
async fn write_chunk<const K: usize, const C: usize, T>(
    path: impl AsRef<Path>,
    chunk: BTreeMap<Hash<K>, T>,
) -> Result<(), Failure<TableAction>>
where
    T: Serialize,
{
    let path = path.as_ref();
    debug!(path = %path.display(), "Writing chunk");
    let yaml = serde_yaml::to_string(&chunk)
        .map_err(Failure::wrap_with_path(TableAction::Serialize, path))?;
    write(path, yaml)
        .await
        .map_err(Failure::wrap_with_path(TableAction::WriteChunk, path))?;
    Ok(())
}

/// Update the items in a chunk
///
/// If `replace` is true then existing items are replaced
async fn update_chunk<const K: usize, const C: usize, T>(
    chunk_path: impl AsRef<Path>,
    new_chunk: BTreeMap<Hash<K>, T>,
    replace: bool,
) -> Result<usize, Failure<TableAction>>
where
    T: DeserializeOwned + Serialize,
{
    let chunk_path = chunk_path.as_ref();
    let mut added = 0;
    let _lock = acquire_lock(chunk_path)
        .await
        .map_err(Failure::wrap(TableAction::UpdateChunk))?;
    let mut chunk = if chunk_path.exists() {
        read_chunk::<K, C, T>(chunk_path)
            .await
            .map_err(Failure::wrap(TableAction::UpdateChunk))?
    } else {
        BTreeMap::new()
    };
    for (hash, item) in new_chunk {
        if replace || !chunk.contains_key(&hash) {
            chunk.insert(hash, item);
            added += 1;
        }
    }
    write_chunk::<K, C, T>(chunk_path, chunk)
        .await
        .map_err(Failure::wrap(TableAction::UpdateChunk))?;
    Ok(added)
}

/// Action being performed when a [`Failure<TableAction>`] occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum TableAction {
    #[error("get item")]
    Get,
    #[error("get all items")]
    GetAll,
    #[error("set item")]
    Set,
    #[error("remove item")]
    Remove,
    #[error("update chunk")]
    UpdateChunk,
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
    #[error("serialize")]
    Serialize,
    #[error("deserialize chunk")]
    Deserialize,
    #[error("update multiple chunks")]
    JoinTask,
    #[error("set items")]
    SetMany,
}
