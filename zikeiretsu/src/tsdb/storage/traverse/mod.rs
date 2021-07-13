use super::{block::*, block_list::*};
use crate::tsdb::datapoint::*;
use lockfile::Lockfile;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TraverseBlockError {
    #[error("no block list file: {0}")]
    NoBlockListFile(String),

    #[error("block list load error: {0}")]
    BlockListReadError(#[from] BlockListError),

    #[error("could not acquire lock {0}")]
    AcquireLockError(String, std::io::Error),
}

pub type Result<T> = std::result::Result<T, TraverseBlockError>;

fn lockfile_path<P: AsRef<Path>>(db_dir: P) -> PathBuf {
    db_dir.as_ref().join(".lock")
}

fn block_list_file_path<P: AsRef<Path>>(db_dir: P, metrics: &str) -> PathBuf {
    db_dir.as_ref().join(format!("{}.list", metrics))
}

pub async fn search_data_from_blocks<P: AsRef<Path>>(
    db_dir: P,
    metrics: &str,
    condition: DatapointSearchCondition,
) -> Result<Vec<DataPoint>> {
    let lock_file_path = lockfile_path(&db_dir);
    let _lockfile = Lockfile::create(&lock_file_path).map_err(|e| {
        TraverseBlockError::AcquireLockError(lock_file_path.display().to_string(), e)
    })?;
    let block_list_path = block_list_file_path(&db_dir, metrics);
    if !block_list_path.exists() {
        //TODO(tacogips) call google cloud hear
        return Err(TraverseBlockError::NoBlockListFile(metrics.to_string()));
    }
    let block_list = read_from_blocklist_file(block_list_path)?;

    unimplemented!()
}
