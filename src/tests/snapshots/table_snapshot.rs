use crate::Hash;
use serde::Serialize;
use std::collections::BTreeMap;

/// Snapshot of table contents for testing.
#[derive(Serialize)]
pub struct TableSnapshot<V: Serialize> {
    pub count: usize,
    pub items: BTreeMap<String, V>,
}

impl<V: Serialize> TableSnapshot<V> {
    /// Create a snapshot from a map of items.
    pub fn from_items<const N: usize>(items: &BTreeMap<Hash<N>, V>) -> Self
    where
        V: Clone,
    {
        let count = items.len();
        let items = items
            .iter()
            .map(|(hash, value)| (hash.to_hex(), value.clone()))
            .collect();
        Self { count, items }
    }
}
