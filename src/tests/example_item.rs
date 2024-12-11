use crate::Hash;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExampleItem {
    pub hash: Hash<20>,
    pub success: bool,
    pub optional: Option<String>,
}

pub fn example_items() -> BTreeMap<Hash<20>, ExampleItem> {
    let mut items = BTreeMap::new();
    for i in [0x19, 0x89, 0xac] {
        for ii in [i + 11, i + 22, i + 33] {
            let mut bytes = [0; 20];
            bytes[0] = i;
            bytes[1] = ii;
            let hash = Hash::<20>::new(bytes);
            items.insert(
                hash,
                ExampleItem {
                    hash,
                    success: ii % 2 == 0,
                    optional: (ii % 3 == 0).then(|| "Optional".to_owned()),
                },
            );
        }
    }
    items
}
