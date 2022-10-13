//    let encoded: Vec<u8> = bincode::serialize(&target).unwrap();
//
//    let decoded: Option<String> = bincode::deserialize(&encoded[..]).unwrap();
//

use super::{Result, WalError, WalWriter};
use crate::tsdb::datapoint::DataPoint;
use async_trait::async_trait;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

const WAL_FILE_NAME: &str = "wal.dat";

pub struct SingleFileWal {
    wal_file: File,

    sink: mpsc::Sender<DataPoint>,
    source: mpsc::Receiver<DataPoint>,
}

#[async_trait]
impl WalWriter for SingleFileWal {
    fn writer_channel(&self) -> Option<mpsc::Sender<DataPoint>> {
        Some(self.sink.clone())
    }

    async fn write(&mut self, datapoint: &DataPoint) -> Result<()> {
        unimplemented!()
    }

    fn load(&self) -> Result<Vec<DataPoint>> {
        unimplemented!()
    }
    fn clean(&mut self) -> Result<()> {
        self.wal_file.set_len(0)?;
        Ok(())
    }

    fn exists(dir_path: &Path) -> bool {
        let mut pb = PathBuf::new();

        pb.push(dir_path);
        pb.push(WAL_FILE_NAME);

        pb.exists()
    }
}

impl SingleFileWal {
    pub fn new(dir_path: &Path, buf_size: usize) -> Result<Self> {
        let mut pb = PathBuf::new();
        pb.push(dir_path);
        pb.push(WAL_FILE_NAME);

        let wal_file = OpenOptions::new().append(true).create(true).open(pb)?;
        let (sink, source) = mpsc::channel(buf_size);
        Ok(Self {
            wal_file,
            sink,
            source,
        })
    }
}
