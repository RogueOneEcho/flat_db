use super::helpers::{PKG_NAME, get_temp_dir};
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::thread::current as current_thread;

/// Test directory with isolation based on test name.
pub struct TestDirectory {
    pub path: PathBuf,
}

impl TestDirectory {
    /// Create a new test directory using the current test name.
    pub fn new() -> Self {
        let test_name = current_thread()
            .name()
            .expect("should be able to get test name")
            .replace("::", "_");
        let path = get_temp_dir(PKG_NAME).join(&test_name);
        create_dir_all(&path).expect("should create test dir");
        Self { path }
    }
}
