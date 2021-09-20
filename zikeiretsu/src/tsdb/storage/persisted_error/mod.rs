use super::block_list::BlockTimestamp;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::timestamp_nano::*;
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::OpenOptions;
use std::io::{Error as IOError, Write};
use std::path::Path;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, PersistedErrorIOError>;
#[derive(Error, Debug)]
pub enum PersistedErrorIOError {
    #[error("serde json error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("io error: {0}")]
    IOError(#[from] IOError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PersistedErrorType {
    FailedToUploadBlockOrBLockList,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedError {
    time: TimestampNano,
    metrics: Option<Metrics>,
    error_type: PersistedErrorType,
    block_timestamp: Option<BlockTimestamp>,
    detail: Option<String>,
}

impl PersistedError {
    pub(crate) fn new(
        time: TimestampNano,
        metrics: Option<Metrics>,
        error_type: PersistedErrorType,
        block_timestamp: Option<BlockTimestamp>,
        detail: Option<String>,
    ) -> Self {
        Self {
            time,
            metrics,
            error_type,
            block_timestamp,
            detail,
        }
    }
}

pub(crate) fn write_persisted_error<P: AsRef<Path>>(
    path: P,
    persisted_error: PersistedError,
) -> Result<()> {
    let mut dest_file = OpenOptions::new().read(true).create(true).open(path)?;

    let serialized_error = serde_json::to_string(&persisted_error)?;
    dest_file.write_all(serialized_error.as_bytes());

    dest_file.flush()?;
    Ok(())
}
