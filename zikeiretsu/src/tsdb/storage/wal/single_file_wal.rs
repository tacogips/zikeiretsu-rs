use super::{Result, WalError, WalWriter};
use crate::tsdb::datapoint::DataPoint;
use crate::tsdb::metrics::Metrics;
use async_trait::async_trait;
use memmap2::MmapOptions;
use std::fs::create_dir_all;
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
        if self.wal_file.metadata()?.len() == 0 {
            return Ok(vec![]);
        } else {
            let wal_data = unsafe {
                MmapOptions::new()
                    .map(&self.wal_file)
                    .map_err(|e| WalError::WalFileOpenError(format!("{}", e)))?
            };
            let datapoints = read_datapint_from_wal(&wal_data)?;
            Ok(datapoints)
        }
    }

    fn clean(&mut self) -> Result<()> {
        self.wal_file.set_len(0)?;
        Ok(())
    }

    fn open_or_create(data_dir_path: &Path, metrics: &Metrics) -> Result<Self> {
        let mut wal_file_path = PathBuf::new();

        wal_file_path.push(data_dir_path);
        wal_file_path.push("wal");
        wal_file_path.push(metrics.as_str());
        wal_file_path.push(WAL_FILE_NAME);

        let wal_dir = wal_file_path
            .parent()
            .ok_or(WalError::WalFileOpenError(format!(
                "could not create wal directory {wal_file_path:?}"
            )))?;
        if !wal_dir.exists() {
            create_dir_all(wal_dir)?;
        }

        let wal_file = if wal_file_path.exists() {
            OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(wal_file_path)?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(wal_file_path)?
        };
        Ok(Self { wal_file })
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::tsdb::{DataPoint, FieldValue, Metrics, TimestampNano};
    use std::fs::OpenOptions;
    use std::io::Read;
    use std::path::PathBuf;
    use tempdir::TempDir;

    macro_rules! float_data_points {
        ($({$timestamp:expr,$values:expr}),*) => {
            vec![
            $(DataPoint::new(ts!($timestamp), $values.into_iter().map(|each| FieldValue::Float64(each as f64)).collect())),*
            ]
        };
    }

    macro_rules! ts {
        ($v:expr) => {
            TimestampNano::new($v)
        };
    }

    #[tokio::test]
    async fn test_wal_write_then_read() {
        let temp_data_dir = TempDir::new("wal_test").expect("Could not create temp dir");
        let db_dir = temp_data_dir.path();
        let test_metrics = Metrics::new("s").unwrap();
        let mut wal = SingleFileWal::open_or_create(db_dir, &test_metrics).unwrap();

        let mut datapoints = float_data_points!(
            {1629745451_715062000, vec![200f64, 12f64]},
            {1629745451_715063000, vec![300f64, 36f64]},
            {1629745451_715064000, vec![400f64, 36f64]},
            {1629745451_715065000, vec![500f64, 36f64]},
            {1629745451_715066000, vec![600f64, 36f64]}
        );

        wal.write(&datapoints).await.unwrap();

        let mut datapoints2 = float_data_points!(
            {1629745451_715067000, vec![700f64, 36f64]},
            {1629745451_715068000, vec![800f64, 37f64]}
        );
        wal.write(&datapoints2).await.unwrap();

        let mut wal_file_path = PathBuf::new();
        wal_file_path.push(db_dir);
        wal_file_path.push("wal");
        wal_file_path.push(test_metrics.as_str());
        wal_file_path.push(WAL_FILE_NAME);

        {
            let mut wal_file = OpenOptions::new()
                .read(true)
                .open(wal_file_path.as_path())
                .unwrap();
            let mut bytes = Vec::<u8>::new();
            let read_size = wal_file.read_to_end(&mut bytes).unwrap();
            assert_ne!(read_size, 0);
        }

        let loaded = wal.load().await.unwrap();

        datapoints.append(&mut datapoints2);
        assert_eq!(loaded, datapoints);
        wal.clean().unwrap();

        {
            let mut wal_file = OpenOptions::new()
                .read(true)
                .open(wal_file_path.as_path())
                .unwrap();
            let mut bytes = Vec::<u8>::new();
            let read_size = wal_file.read_to_end(&mut bytes).unwrap();
            assert_eq!(read_size, 0);
        }
    }

    #[tokio::test]
    async fn test_load_from_empty_wal() {
        let temp_data_dir = TempDir::new("wal_test").expect("Could not create temp dir");
        let db_dir = temp_data_dir.path();
        let test_metrics = Metrics::new("s").unwrap();
        let wal = SingleFileWal::open_or_create(db_dir, &test_metrics).unwrap();

        let v = wal.load().await.unwrap();

        assert!(v.is_empty());
    }
}
