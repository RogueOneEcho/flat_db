use crate::tests::example_item::{example_items, ExampleItem};
use crate::tests::helpers::{create_temp_dir, PKG_NAME};
use crate::{Hash, Table};
use rogue_logging::Error;
use rogue_logging::Logger;
use std::collections::BTreeMap;
use std::marker::PhantomData;

#[tokio::test]
async fn table_end_to_end() -> Result<(), Error> {
    // Arrange
    Logger::force_init(PKG_NAME.to_owned());
    let table = Table::<20, 1, ExampleItem> {
        directory: create_temp_dir(PKG_NAME),
        phantom: PhantomData,
    };
    let items = example_items();
    let expected_count = items.len();

    // Act
    table.set_many(items, true).await?;

    // Act
    let items: BTreeMap<Hash<20>, ExampleItem> = table.get_all().await?;
    let actual_count = items.len();

    // Assert
    assert_eq!(expected_count, actual_count);

    // Arrange
    let mut bytes = [0; 20];
    bytes[0] = 0xac;
    bytes[1] = 0x32;
    let hash = Hash::<20>::new(bytes);
    let new_item = ExampleItem {
        hash,
        success: true,
        optional: Some("New item".to_owned()),
    };

    // Act
    table.set(hash, new_item.clone()).await?;

    // Assert
    let items: BTreeMap<Hash<20>, ExampleItem> = table.get_all().await?;
    let actual_count = items.len();
    assert_eq!(expected_count + 1, actual_count);

    Ok(())
}
