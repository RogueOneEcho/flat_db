use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

/// Snapshot of a single file.
#[derive(Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileSnapshot {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

/// Snapshot of a directory's file structure.
#[derive(Serialize)]
pub struct DirectorySnapshot {
    pub files: Vec<FileSnapshot>,
}

impl DirectorySnapshot {
    /// Create a snapshot of a directory.
    pub fn from_path(dir: &Path) -> Self {
        let mut files = Vec::new();
        Self::collect_files(dir, dir, &mut files);
        files.sort();
        Self { files }
    }
    fn collect_files(base: &Path, dir: &Path, files: &mut Vec<FileSnapshot>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                Self::collect_files(base, &path, files);
            } else if path.is_file()
                && let Some(snapshot) = Self::file_snapshot(base, &path)
            {
                files.push(snapshot);
            }
        }
    }
    fn file_snapshot(base: &Path, path: &Path) -> Option<FileSnapshot> {
        let content = fs::read(path).ok()?;
        let size = u64::try_from(content.len()).ok()?;
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let sha256 = format!("{:x}", hasher.finalize());
        let relative = path.strip_prefix(base).ok()?;
        Some(FileSnapshot {
            path: relative.to_string_lossy().to_string(),
            size,
            sha256,
        })
    }
}
