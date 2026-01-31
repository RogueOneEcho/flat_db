//! Flat file database with chunked storage.
//!
//! Chunking achieves a balance between minimizing the number of file operations
//! and the performance cost of serializing large numbers of items to a flat file
//! format that can be manually edited and version controlled.

pub use file_table::*;
pub use hash::*;
pub use table::*;

mod file_table;
mod hash;
mod table;
#[cfg(test)]
mod tests;
