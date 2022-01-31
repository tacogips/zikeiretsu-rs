pub mod cloud_setting;
pub mod read;
pub mod write;

use crate::tsdb::cloudstorage::*;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::storage::{block, block_list, persisted_error};
use crate::tsdb::timestamp_nano::TimestampNano;
pub use cloud_setting::*;

use std::path::{Path, PathBuf};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, StorageApiError>;
pub struct CacheSetting {
    pub read_cache: bool,
    pub write_cache: bool,
}

impl CacheSetting {
    pub fn none() -> Self {
        Self {
            read_cache: false,
            write_cache: false,
        }
    }

    pub fn both() -> Self {
        Self {
            read_cache: true,
            write_cache: true,
        }
    }

    pub fn only_read() -> Self {
        Self {
            read_cache: true,
            write_cache: false,
        }
    }

    pub fn only_write() -> Self {
        Self {
            read_cache: false,
            write_cache: true,
        }
    }
}

#[derive(Error, Debug)]
pub enum StorageApiError {
    #[error("no block list file: {0}")]
    NoBlockListFile(String),

    #[error("no block  file: {0}")]
    NoBlockFile(String),

    #[error("block load error: {0}")]
    BlockReadError(#[from] block::BlockError),

    #[error("block list load error: {0}")]
    BlockListReadError(#[from] block_list::BlockListError),

    #[error("could not acquire lock {0}")]
    AcquireLockError(String, std::io::Error),

    #[error("failed to create block dir or file. {0}")]
    CreateBlockFileError(std::io::Error),

    #[error("failed to remove block dir or file. {0}")]
    RemoveBlockDirError(std::io::Error),

    #[error("database being on not supported status: {0}")]
    UnsupportedStorageStatus(String),

    #[error("cloud storage error. {0}")]
    CloudStorageError(#[from] CloudStorageError),

    #[error("cloud upload error. {0}")]
    CloudUploadError(String),

    #[error("create lock file error. {0}")]
    CreateLockfileError(String),

    #[error("error on persisted error. {0}")]
    PersistedError(#[from] persisted_error::PersistedErrorIOError),

    #[error("invalid block list file name. {0}")]
    InvalidBlockListFileName(String),

    #[error("Local DB path required. {0}")]
    DbDirPathRequired(String),
}

pub(crate) fn lockfile_path(db_dir: &Path, metrics: &Metrics) -> PathBuf {
    db_dir.join(format!("{metrics}.lock"))
}

pub(crate) fn block_list_file_path(db_dir: &Path, metrics: &Metrics) -> PathBuf {
    db_dir.join(format!("block_list/{metrics}.list"))
}

pub(crate) fn block_list_dir_path(db_dir: &Path) -> PathBuf {
    db_dir.join("block_list")
}

pub(crate) fn persisted_error_file_path(db_dir: &Path, timestamp_nano: &TimestampNano) -> PathBuf {
    db_dir.join(format!("error/{timestamp_nano}.list"))
}

pub(crate) fn block_timestamp_to_block_file_path(
    root_dir: &Path,
    metrics: &Metrics,
    block_timestamp: &block_list::BlockTimestamp,
) -> (PathBuf, PathBuf) {
    let timestamp_head: u64 = block_timestamp.since_sec.0 / (10u64.pow(5u32));

    // path format:
    //  {root_dir}/block/{metrics}/{timestamp_sec_since[:4]}/{timestamp_sec_since}_{timestamp_sec_since}}/block

    let block_path_dir = root_dir.to_path_buf().join(format!(
        "block/{metrics}/{timestamp_head}/{since_sec}_{until_sec}/",
        since_sec = block_timestamp.since_sec,
        until_sec = block_timestamp.until_sec,
    ));

    let block_path = root_dir.to_path_buf().join(format!(
        "block/{metrics}/{timestamp_head}/{since_sec}_{until_sec}/block",
        since_sec = block_timestamp.since_sec,
        until_sec = block_timestamp.until_sec,
    ));
    (block_path_dir, block_path)
}

#[cfg(test)]
mod test {

    use super::block_list::*;
    use super::*;
    use crate::tsdb::timestamp_sec::TimestampSec;
    use std::path::PathBuf;

    #[test]
    fn test_block_timestamp_to_block_file_path() {
        let block_timestamp =
            BlockTimestamp::new(TimestampSec::new(162688734), TimestampSec::new(162688735));
        let (path_dir, path_buf) = block_timestamp_to_block_file_path(
            &PathBuf::from("root_dir"),
            &Metrics::new("some_metrics"),
            &block_timestamp,
        );

        assert_eq!(
            path_dir.display().to_string(),
            "root_dir/block/some_metrics/1626/162688734_162688735/"
        );

        assert_eq!(
            path_buf.display().to_string(),
            "root_dir/block/some_metrics/1626/162688734_162688735/block"
        );
    }
}
