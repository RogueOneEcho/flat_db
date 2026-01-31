use crate::tests::example_item::{ExampleItem, example_items};
use crate::tests::helpers::{PKG_NAME, create_temp_dir};
use crate::{FileTable, Hash};
use rogue_logging::Verbosity::Trace;
use rogue_logging::{Error, LoggerBuilder};
use std::collections::BTreeMap;
use std::fs::write;
use std::path::PathBuf;

#[tokio::test]
async fn file_table_end_to_end() -> Result<(), Error> {
    // Arrange
    let _ = LoggerBuilder::new().with_verbosity(Trace).create();
    let examples = example_items();
    let examples = examples
        .into_values()
        .map(|example| create_example_file(example).expect("should not fail"))
        .collect();
    let table = FileTable::<20, 1> {
        directory: create_temp_dir(PKG_NAME),
        extension: "txt".to_owned(),
    };
    let items = example_items();
    let expected_count = items.len();

    // Act
    table.set_many(examples).await?;

    // Act
    let items: BTreeMap<Hash<20>, PathBuf> = table.get_all().await?;
    let actual_count = items.len();

    // Assert
    assert_eq!(actual_count, expected_count);

    // Arrange
    let mut bytes = [0; 20];
    bytes[0] = 0xac;
    bytes[1] = 0x32;
    let hash = Hash::<20>::new(bytes);
    let (hash, new_path) = create_example_file(ExampleItem {
        hash,
        success: true,
        optional: Some("New item".to_owned()),
    })
    .expect("should not fail");

    // Act
    table.set(hash, new_path).await?;

    // Assert
    let items: BTreeMap<Hash<20>, PathBuf> = table.get_all().await?;
    let actual_count = items.len();
    assert_eq!(expected_count + 1, actual_count);

    Ok(())
}

fn create_example_file(example: ExampleItem) -> Result<(Hash<20>, PathBuf), Error> {
    let hash = example.hash.to_hex();
    let path = create_temp_dir(&format!("{PKG_NAME}-source")).join(format!("file-{hash}.txt"));
    write(path.clone(), hash).map_err(|e| Error {
        action: "read_directory".to_owned(),
        message: e.to_string(),
        domain: Some("file system".to_owned()),
        ..Error::default()
    })?;
    Ok((example.hash, path))
}
