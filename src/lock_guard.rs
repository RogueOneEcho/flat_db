use crate::TableAction;
use rogue_logging::Failure;
use std::fs::remove_file;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::time::sleep;
use tracing::trace;

const LOCK_ACQUIRE_SLEEP_MILLIS: u64 = 50;
const LOCK_ACQUIRE_TIMEOUT: u64 = 2;
const LOCK_FILE_EXTENSION: &str = "lock";

/// RAII guard that removes a lock file when dropped.
pub(crate) struct LockGuard {
    path: PathBuf,
}
impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = remove_file(&self.path);
    }
}

/// Acquire a lock
///
/// If the lock is already in use then wait
pub(crate) async fn acquire_lock(
    path: impl AsRef<Path>,
) -> Result<LockGuard, Failure<TableAction>> {
    let start = Instant::now();
    let timeout = Duration::from_secs(LOCK_ACQUIRE_TIMEOUT);
    let mut lock: PathBuf = path.as_ref().to_path_buf();
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
            return Ok(LockGuard { path: lock });
        }
        if start.elapsed() > timeout {
            return Err(Failure::new(
                TableAction::AcquireLock,
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Exceeded timeout for acquiring lock",
                ),
            )
            .with_path(&lock));
        }
        trace!(path = %lock.display(), "Lock busy, waiting");
        sleep(Duration::from_millis(LOCK_ACQUIRE_SLEEP_MILLIS)).await;
    }
}
