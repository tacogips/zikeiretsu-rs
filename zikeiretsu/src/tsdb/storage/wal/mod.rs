mod single_file_wal;

use crate::tsdb::datapoint::DataPoint;
use async_trait::async_trait;
use std::io::{Error as IOError, Write};
use std::path::Path;
use thiserror::Error;
use tokio::sync::mpsc;

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("io error: {0}")]
    IOError(#[from] IOError),
}

#[async_trait]
pub trait WalWriter {
    fn writer_channel(&self) -> Option<mpsc::Sender<DataPoint>>;
    async fn write(&mut self, datapoint: &DataPoint) -> Result<()>;
    fn load(&self) -> Result<Vec<DataPoint>>;
    fn clean(&mut self) -> Result<()>;
    fn exists(dir_path: &Path) -> bool;
}

pub struct EmptyWal;

#[async_trait]
impl WalWriter for EmptyWal {
    fn writer_channel(&self) -> Option<mpsc::Sender<DataPoint>> {
        None
    }
    async fn write(&mut self, datapoint: &DataPoint) -> Result<()> {
        Ok(())
    }
    fn load(&self) -> Result<Vec<DataPoint>> {
        Ok(vec![])
    }
    fn clean(&mut self) -> Result<()> {
        Ok(())
    }
    fn exists(dir_path: &Path) -> bool {
        false
    }
}
