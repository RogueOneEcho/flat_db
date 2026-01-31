use crate::tests::example_item::{ExampleItem, example_items};
use crate::tests::snapshots::TableSnapshot;
use crate::tests::test_directory::TestDirectory;
use crate::{Hash, Table};
use rogue_logging::{Error, LoggerBuilder};
use std::collections::BTreeMap;
use std::fs::create_dir_all;
use std::marker::PhantomData;
use tokio::runtime::Runtime;

#[tokio::test]
async fn table_set_many_and_get_all() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().create();
    let (_test_dir, table) = create_table();
    let items = example_items();

    // Act
    table.set_many(items, true).await?;
    let items: BTreeMap<Hash<20>, ExampleItem> = table.get_all().await?;

    // Assert
    let snapshot = TableSnapshot::from_items(&items);
    insta::assert_yaml_snapshot!(snapshot);
    Ok(())
}

#[tokio::test]
async fn table_set_and_remove() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().create();
    let (_test_dir, table) = create_table();
    let items = example_items();
    let expected_count = items.len();
    table.set_many(items, true).await?;

    // Arrange
    let (hash, new_item) = create_single_item();

    // Act
    table.set(hash, new_item.clone()).await?;

    // Assert
    let items: BTreeMap<Hash<20>, ExampleItem> = table.get_all().await?;
    assert_eq!(expected_count + 1, items.len());

    // Act
    table.remove(hash).await?;

    // Assert
    let items: BTreeMap<Hash<20>, ExampleItem> = table.get_all().await?;
    assert_eq!(expected_count, items.len());
    Ok(())
}

#[tokio::test]
async fn table_get_single_item() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().create();
    let (_test_dir, table) = create_table();
    let items = example_items();
    table.set_many(items.clone(), true).await?;
    let (hash, expected) = items.into_iter().next().expect("should have at least one");

    // Act
    let result = table.get(hash)?;

    // Assert
    assert_eq!(result, Some(expected));
    Ok(())
}

#[tokio::test]
async fn table_get_missing_item() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().create();
    let (_test_dir, table) = create_table();
    let items = example_items();
    table.set_many(items, true).await?;
    let (missing_hash, _) = create_single_item();

    // Act
    let result = table.get(missing_hash)?;

    // Assert
    assert_eq!(result, None);
    Ok(())
}

#[tokio::test]
async fn table_set_many_no_replace() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().create();
    let (_test_dir, table) = create_table();
    let items = example_items();
    table.set_many(items.clone(), true).await?;
    let (hash, original) = items.into_iter().next().expect("should have at least one");
    let modified = ExampleItem {
        hash,
        success: !original.success,
        optional: Some("modified".to_owned()),
    };
    let mut update = BTreeMap::new();
    update.insert(hash, modified);

    // Act
    let added = table.set_many(update, false).await?;

    // Assert
    assert_eq!(added, 0);
    let result = table.get(hash)?;
    assert_eq!(result, Some(original));
    Ok(())
}

#[test]
fn table_empty_get_all() {
    // Arrange
    let _ = LoggerBuilder::new().create();
    let (test_dir, table) = create_table();
    create_dir_all(&test_dir.path).expect("should create dir");

    // Act
    let rt = Runtime::new().expect("should create runtime");
    let result = rt.block_on(table.get_all());

    // Assert
    let items = result.expect("should succeed");
    assert!(items.is_empty());
}

#[tokio::test]
async fn table_remove_missing_item() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().create();
    let (_test_dir, table) = create_table();
    let items = example_items();
    table.set_many(items.clone(), true).await?;
    let (missing_hash, _) = create_single_item();

    // Act
    let result = table.remove(missing_hash).await?;

    // Assert
    assert_eq!(result, None);
    let items_after: BTreeMap<Hash<20>, ExampleItem> = table.get_all().await?;
    assert_eq!(items_after.len(), items.len());
    Ok(())
}

fn create_table() -> (TestDirectory, Table<20, 1, ExampleItem>) {
    let test_dir = TestDirectory::new();
    let table = Table::<20, 1, ExampleItem> {
        directory: test_dir.path.clone(),
        phantom: PhantomData,
    };
    (test_dir, table)
}

fn create_single_item() -> (Hash<20>, ExampleItem) {
    let mut bytes = [0; 20];
    bytes[0] = 0xab;
    bytes[1] = 0xcd;
    let hash = Hash::<20>::new(bytes);
    let item = ExampleItem {
        hash,
        success: true,
        optional: Some("single".to_owned()),
    };
    (hash, item)
}
