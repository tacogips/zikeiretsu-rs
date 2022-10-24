//    let encoded: Vec<u8> = bincode::serialize(&target).unwrap();
//
//    let decoded: Option<String> = bincode::deserialize(&encoded[..]).unwrap();
//

use super::{Result, WalError, WalWriter};
use crate::tsdb::datapoint::DataPoint;
use async_trait::async_trait;
use memmap2::MmapOptions;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::task;

const WAL_FILE_NAME: &str = "wal.dat";

pub struct SingleFileWal {
    wal_file: File,
}

fn write_datapint_to_wal<W>(mut w: W, datapoint: &DataPoint) -> Result<()>
where
    W: Write,
{
    let serialized = bincode::serialize(&datapoint)?;
    let data_size = serialized.len() as u64;

    w.write_all(&data_size.to_be_bytes())?;
    //TODO(tacodigs) if consequence data writing fails, should we delete the written data length
    //also?
    w.write_all(&serialized)?;
    Ok(())
}

fn read_datapint_from_wal(datas: &[u8]) -> Result<Vec<DataPoint>> {
    //let mut datapoints =Vec<DataPoint>::new();

    //loop{
    //    r.read(8)

    //let serialized = bincode::deserialize(&datapoint)?;
    //let data_size = serialized.len() as u64;

    //w.write_all(&data_size.to_be_bytes())?;
    ////TODO(tacodigs) if consequence data writing fails, should we delete the written data length
    ////also?
    //w.write_all(&serialized)?;

    //}

    //Ok(())
    unimplemented!()
}

#[async_trait]
impl WalWriter for SingleFileWal {
    async fn write(&mut self, datapoints: &[DataPoint]) -> Result<()> {
        for each_datapoint in datapoints {
            write_datapint_to_wal(&mut self.wal_file, each_datapoint)?
        }
        Ok(())
    }

    async fn load(&self) -> Result<Vec<DataPoint>> {
        let wal_data = unsafe {
            MmapOptions::new()
                .map(&self.wal_file)
                .map_err(|e| WalError::WalFileOpenError(format!("{}", e)))?
        };
        let data = read_datapint_from_wal(&wal_data)?;
        Ok(data)
    }

    fn clean(&mut self) -> Result<()> {
        self.wal_file.set_len(0)?;
        Ok(())
    }

    fn exists(data_dir_path: &Path) -> bool {
        let mut pb = PathBuf::new();

        pb.push(data_dir_path);
        pb.push(WAL_FILE_NAME);

        pb.exists()
    }
}

impl SingleFileWal {
    pub fn new(dir_path: &Path) -> Result<Self> {
        let mut pb = PathBuf::new();
        pb.push(dir_path);
        pb.push(WAL_FILE_NAME);

        let wal_file = OpenOptions::new().append(true).create(true).open(pb)?;

        Ok(Self { wal_file })
    }
}
