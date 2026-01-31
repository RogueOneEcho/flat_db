use std::env::temp_dir;
use std::path::PathBuf;
use std::time::SystemTime;

pub(crate) const PKG_NAME: &str = "flat_db";

pub(crate) fn get_temp_dir(sub_dir_name: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Duration should be valid")
        .as_millis()
        .to_string();
    temp_dir().join(sub_dir_name).join(timestamp)
}
