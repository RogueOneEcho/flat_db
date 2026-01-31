use crate::tests::example_item::{ExampleItem, example_items};
use crate::tests::helpers::{PKG_NAME, get_temp_dir};
use crate::tests::snapshots::DirectorySnapshot;
use crate::tests::test_directory::TestDirectory;
use crate::{FileTable, Hash};
use rogue_logging::Verbosity::Trace;
use rogue_logging::{Error, LoggerBuilder};
use std::collections::BTreeMap;
use std::fs::{create_dir_all, write};
use std::path::PathBuf;
use tokio::runtime::Runtime;

#[tokio::test]
async fn file_table_set_many_and_get_all() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().with_verbosity(Trace).create();
    let examples = create_example_files();
    let expected_count = examples.len();
    let (test_dir, table) = create_file_table();

    // Act
    table.set_many(examples).await?;
    let items: BTreeMap<Hash<20>, PathBuf> = table.get_all().await?;

    // Assert
    assert_eq!(items.len(), expected_count);
    let snapshot = DirectorySnapshot::from_path(&test_dir.path);
    insta::assert_yaml_snapshot!(snapshot);
    Ok(())
}

#[tokio::test]
async fn file_table_set_and_get_all() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().with_verbosity(Trace).create();
    let examples = create_example_files();
    let expected_count = examples.len();
    let (_test_dir, table) = create_file_table();
    table.set_many(examples).await?;

    // Arrange
    let mut bytes = [0; 20];
    bytes[0] = 0xac;
    bytes[1] = 0x32;
    let hash = Hash::<20>::new(bytes);
    let (hash, new_path) = create_example_file(ExampleItem {
        hash,
        success: true,
        optional: Some("New item".to_owned()),
    })?;

    // Act
    table.set(hash, new_path).await?;

    // Assert
    let items: BTreeMap<Hash<20>, PathBuf> = table.get_all().await?;
    assert_eq!(items.len(), expected_count + 1);
    Ok(())
}

#[tokio::test]
async fn file_table_get_single_file() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().with_verbosity(Trace).create();
    let examples = create_example_files();
    let hash = *examples.keys().next().expect("should have at least one");
    let (_test_dir, table) = create_file_table();
    table.set_many(examples).await?;

    // Act
    let result = table.get(hash)?;

    // Assert
    assert!(result.is_some());
    let path = result.expect("already checked");
    assert!(path.exists());
    Ok(())
}

#[tokio::test]
async fn file_table_get_missing_file() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().with_verbosity(Trace).create();
    let examples = create_example_files();
    let (_test_dir, table) = create_file_table();
    table.set_many(examples).await?;
    let mut bytes = [0; 20];
    bytes[0] = 0xff;
    bytes[1] = 0xee;
    let missing_hash = Hash::<20>::new(bytes);

    // Act
    let result = table.get(missing_hash)?;

    // Assert
    assert!(result.is_none());
    Ok(())
}

#[test]
fn file_table_empty_get_all() {
    // Arrange
    let _ = LoggerBuilder::new().with_verbosity(Trace).create();
    let (test_dir, table) = create_file_table();
    create_dir_all(&test_dir.path).expect("should create dir");

    // Act
    let rt = Runtime::new().expect("should create runtime");
    let result = rt.block_on(table.get_all());

    // Assert
    let items = result.expect("should succeed");
    assert!(items.is_empty());
}

fn create_file_table() -> (TestDirectory, FileTable<20, 1>) {
    let test_dir = TestDirectory::new();
    let table = FileTable::<20, 1> {
        directory: test_dir.path.clone(),
        extension: "txt".to_owned(),
    };
    (test_dir, table)
}

fn create_example_file(example: ExampleItem) -> Result<(Hash<20>, PathBuf), Error> {
    let hash = example.hash.to_hex();
    let path = get_temp_dir(&format!("{PKG_NAME}-source")).join(format!("file-{hash}.txt"));
    create_dir_all(path.parent().expect("should have parent")).map_err(|e| Error {
        action: "create_directory".to_owned(),
        message: e.to_string(),
        domain: Some("file system".to_owned()),
        ..Error::default()
    })?;
    write(path.clone(), hash).map_err(|e| Error {
        action: "write_file".to_owned(),
        message: e.to_string(),
        domain: Some("file system".to_owned()),
        ..Error::default()
    })?;
    Ok((example.hash, path))
}

fn create_example_files() -> BTreeMap<Hash<20>, PathBuf> {
    example_items()
        .into_values()
        .map(|example| create_example_file(example).expect("should not fail"))
        .collect()
}
