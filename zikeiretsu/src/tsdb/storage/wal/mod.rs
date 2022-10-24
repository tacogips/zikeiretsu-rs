mod single_file_wal;

use crate::tsdb::datapoint::DataPoint;
use async_trait::async_trait;
use bincode::Error as BincodeError;
use std::io::{Error as IOError, Write};
use std::path::Path;
use thiserror::Error;
use tokio::sync::mpsc;

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("io error: {0}")]
    IOError(#[from] IOError),
    #[error("bincode error: {0}")]
    BincodeError(#[from] BincodeError),

    #[error("wal file open error: {0}")]
    WalFileOpenError(String),
}

#[async_trait]
pub trait WalWriter {
    async fn write(&mut self, datapoint: &[DataPoint]) -> Result<()>;
    async fn load(&self) -> Result<Vec<DataPoint>>;
    fn clean(&mut self) -> Result<()>;
    fn exists(data_dir_path: &Path) -> bool;
}

pub struct EmptyWal;

#[async_trait]
impl WalWriter for EmptyWal {
    async fn write(&mut self, _datapoint: &[DataPoint]) -> Result<()> {
        Ok(())
    }
    async fn load(&self) -> Result<Vec<DataPoint>> {
        Ok(vec![])
    }
    fn clean(&mut self) -> Result<()> {
        Ok(())
    }
    fn exists(data_dir_path: &Path) -> bool {
        false
    }
}
