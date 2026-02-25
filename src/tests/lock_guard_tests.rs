use crate::TableAction;
use crate::lock_guard::acquire_lock;
use crate::tests::test_directory::TestDirectory;
use rogue_logging::Failure;
use std::time::Duration;
use tokio::time::sleep;
use tracing_test::traced_test;

#[traced_test]
#[tokio::test]
async fn acquire_lock_creates_lock_file() -> Result<(), Failure<TableAction>> {
    let test_dir = TestDirectory::new();
    let chunk_path = test_dir.path.join("chunk.yml");
    let expected_lock = test_dir.path.join("chunk.lock");
    let guard = acquire_lock(&chunk_path).await?;
    assert!(expected_lock.exists());
    drop(guard);
    Ok(())
}

#[traced_test]
#[tokio::test]
async fn drop_removes_lock_file() -> Result<(), Failure<TableAction>> {
    let test_dir = TestDirectory::new();
    let chunk_path = test_dir.path.join("chunk.yml");
    let expected_lock = test_dir.path.join("chunk.lock");
    let guard = acquire_lock(&chunk_path).await?;
    assert!(expected_lock.exists());
    drop(guard);
    assert!(!expected_lock.exists());
    Ok(())
}

#[traced_test]
#[tokio::test]
async fn second_acquire_succeeds_after_drop() -> Result<(), Failure<TableAction>> {
    let test_dir = TestDirectory::new();
    let chunk_path = test_dir.path.join("chunk.yml");
    let guard = acquire_lock(&chunk_path).await?;
    drop(guard);
    let _guard2 = acquire_lock(&chunk_path).await?;
    Ok(())
}

#[traced_test]
#[tokio::test]
async fn acquire_times_out_when_locked() {
    let test_dir = TestDirectory::new();
    let chunk_path = test_dir.path.join("chunk.yml");
    let _guard = acquire_lock(&chunk_path).await.expect("first lock");
    let result = acquire_lock(&chunk_path).await;
    assert!(result.is_err());
}

#[traced_test]
#[tokio::test]
async fn acquire_succeeds_after_concurrent_release() -> Result<(), Failure<TableAction>> {
    let test_dir = TestDirectory::new();
    let chunk_path = test_dir.path.join("chunk.yml");
    let guard = acquire_lock(&chunk_path).await?;
    let path = chunk_path.clone();
    let handle = tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        drop(guard);
    });
    let _guard2 = acquire_lock(&path).await?;
    handle.await.expect("spawned task should complete");
    Ok(())
}
