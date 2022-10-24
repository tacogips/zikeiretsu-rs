use super::{Result, WalError, WalWriter};
use crate::tsdb::datapoint::DataPoint;
use crate::tsdb::metrics::Metrics;
use async_trait::async_trait;
use memmap2::MmapOptions;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

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
    let mut result = Vec::<DataPoint>::new();
    let mut current_index: usize = 0;
    while current_index < datas.len() {
        //datalength in 64bit
        let mut data_length: [u8; 8] = Default::default();
        data_length.copy_from_slice(&datas[current_index..current_index + 8usize]);
        let data_length: usize = u64::from_be_bytes(data_length) as usize;
        current_index += 8;

        let raw_datapoint = &datas[current_index..current_index + data_length]; //datalength in 64bit
        current_index += data_length;

        let deserialized: DataPoint = bincode::deserialize(&raw_datapoint)?;
        result.push(deserialized);
    }

    Ok(result)
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
        let datapoints = read_datapint_from_wal(&wal_data)?;
        Ok(datapoints)
    }

    fn clean(&mut self) -> Result<()> {
        self.wal_file.set_len(0)?;
        Ok(())
    }

    fn exists(data_dir_path: &Path, metrics: &Metrics) -> bool {
        let mut pb = PathBuf::new();

        pb.push(data_dir_path);
        pb.push("wal");
        pb.push(metrics.as_str());
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
