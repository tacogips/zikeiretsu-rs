pub mod read_only_store;
pub mod writable_store;

use crate::tsdb::{datapoint::*, search::*, storage::api as storage_api, timestamp_nano::*};
use async_trait::async_trait;
use std::cmp::Ordering;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("append error. {0}")]
    AppendError(String),

    #[error("unsorted datapoints. {0}")]
    UnsortedDatapoints(String),

    #[error("data field types mismatched. {0}")]
    DataFieldTypesMismatched(String),

    #[error("search error. {0}")]
    SearchError(String),

    #[error("storage api error. {0}")]
    StorageErorr(#[from] storage_api::StorageApiError),
}

type Result<T> = std::result::Result<T, StoreError>;
