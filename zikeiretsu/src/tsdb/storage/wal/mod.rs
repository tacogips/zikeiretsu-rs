mod single_file_wal;

use crate::tsdb::datapoint::DataPoint;
use crate::tsdb::metrics::Metrics;
use async_trait::async_trait;
use bincode::Error as BincodeError;
pub use single_file_wal::SingleFileWal;
use std::io::Error as IOError;
use std::path::Path;
use thiserror::Error;
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
pub trait WalWriter: Sync + Send {
    async fn write(&mut self, datapoint: &[DataPoint]) -> Result<()>;
    async fn load(&self) -> Result<Vec<DataPoint>>;
    fn clean(&mut self) -> Result<()>;
    fn exists(data_dir_path: &Path, metrics: &Metrics) -> bool;
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
    fn exists(_data_dir_path: &Path, _metrics: &Metrics) -> bool {
        false
    }
}
