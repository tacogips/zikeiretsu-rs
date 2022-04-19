///
/// ┌───────────────────────────┐
/// │(1)number of data (n bytes)│
/// └───────────────────────────┘
/// ┌───────────────────────────┐
/// │(2)data fields num (1 byte)│
/// └───────────────────────────┘
/// ┌───────────────────────────┬─────────────────────────────────────────────────────────────┐
/// │(3)type of field_1(1 byte) │ ... (type of field block repeated over the number of fields)│
/// └───────────────────────────┴─────────────────────────────────────────────────────────────┘
/// ┌───────────────────────────┐
/// │(4)head timestamp (8 byte) │
/// └───────────────────────────┘
/// ┌────────────────────────────────────┐
/// │(5)timestamp deltas(sec)(8 byte * n)│
/// └────────────────────────────────────┘
/// ┌───────────────────────────────────────────────────────┐
/// │(6) common trailing zero num of timestamp nano (8 bits)│
/// └───────────────────────────────────────────────────────┘
/// ┌───────────────────────────────────────┐
/// │(7) timestamp sub nano sec(n bytes)    │
/// └───────────────────────────────────────┘
/// ┌──────────────────────────────┬──────────────────────────────────────┐
/// │(8)datas of field 1(n bytes)  │ ... (reapeat over number of fields)  │
/// └──────────────────────────────┴──────────────────────────────────────┘
///
mod compress;
mod field_type_convert;
pub mod read;
pub mod write;

use crate::tsdb::*;
use crate::FieldError;
use compress::CompressError;
use memmap2::MmapOptions;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, BlockError>;

#[derive(Error, Debug)]
pub enum BlockError {
    #[error("block file already exists {0}")]
    FileAlreadyExists(PathBuf),

    #[error("empty datapoint")]
    EmptyDatapoints,

    #[error("block file error {0}")]
    FileError(#[from] std::io::Error),

    #[error("block file error {0}")]
    FileWithPathError(std::io::Error, String),

    #[error("simple 128 variant error  {0}")]
    Simpe128VariantError(#[from] base_128_variants::Error),

    #[error("bits ope error {0}")]
    BitsOpeError(#[from] bits_ope::Error),

    #[error("simple 8b rle error {0}")]
    Simple8bRleError(#[from] simple8b_rle::Error),

    #[error("xor encoding {0}")]
    XorEncoding(#[from] xor_encoding::Error),

    #[error("field error on block. {0}")]
    FieldError(#[from] FieldError),

    #[error("invalid block file : {0} at {1}")]
    InvalidBlockfileError(String, usize),

    #[error("unsupported field type : {0}")]
    UnsupportedFieldType(FieldType),

    #[error("unknwon error : {0}")]
    UnKnownError(String),

    #[error("compress error : {0}")]
    CompressError(#[from] CompressError),

    #[error("invalid field selector : {0}")]
    InvalidFieldSelector(String),
}

impl BlockError {
    pub(crate) fn file_error<P: AsRef<Path>>(e: std::io::Error, p: P) -> BlockError {
        BlockError::FileWithPathError(e, p.as_ref().display().to_string())
    }
}

#[derive(Debug)]
pub(crate) struct TimestampDeltas {
    pub head_timestamp: TimestampNano,
    pub timestamps_deltas_second: Vec<u64>,
    pub common_trailing_zero_bits: u8,
    pub timestamps_nanoseconds: Vec<u64>,
}

impl TimestampDeltas {
    pub fn as_timestamps(self) -> Vec<TimestampNano> {
        debug_assert_eq!(
            self.timestamps_deltas_second.len(),
            self.timestamps_nanoseconds.len()
        );

        let mut timestamps = Vec::<TimestampNano>::new();
        timestamps.push(self.head_timestamp.clone());
        let mut prev_timestamp = self.head_timestamp;

        for data_idx in 0..self.timestamps_deltas_second.len() {
            let current_timestamp = ((*prev_timestamp / SEC_IN_NANOSEC as u64)
                * SEC_IN_NANOSEC as u64)
                + (self.timestamps_deltas_second.get(data_idx).unwrap() * SEC_IN_NANOSEC as u64)
                + (self.timestamps_nanoseconds.get(data_idx).unwrap()
                    << self.common_trailing_zero_bits);
            let current_timestamp = TimestampNano(current_timestamp);
            timestamps.push(current_timestamp.clone());
            prev_timestamp = current_timestamp;
        }
        timestamps
    }
}

impl From<&[DataPoint]> for TimestampDeltas {
    fn from(datapoints: &[DataPoint]) -> TimestampDeltas {
        debug_assert!(!datapoints.is_empty());

        let head_data_point = unsafe { datapoints.get_unchecked(0) };

        let mut timestamps_deltas_second = Vec::<u64>::new();
        let mut timestamps_nanoseconds = Vec::<u64>::new();

        for i in 1..datapoints.len() {
            let prev = unsafe { datapoints.get_unchecked(i - 1) };
            let curr = unsafe { datapoints.get_unchecked(i) };
            let delta_sec =
                &curr.timestamp_nano.as_timestamp_sec() - &prev.timestamp_nano.as_timestamp_sec();

            let nanosec: u64 = *curr.timestamp_nano % SEC_IN_NANOSEC as u64;

            timestamps_deltas_second.push(delta_sec);
            timestamps_nanoseconds.push(nanosec);
        }

        let mut common_trailing_zero_bits = u32::MAX;
        for each in timestamps_nanoseconds.iter() {
            let curr_trailing_zero = each.trailing_zeros();
            if curr_trailing_zero < common_trailing_zero_bits {
                common_trailing_zero_bits = curr_trailing_zero
            }
        }

        let timestamps_sub_nanoseconds = timestamps_nanoseconds
            .into_iter()
            .map(|each| each >> common_trailing_zero_bits)
            .collect();

        let head_timestamp = head_data_point.timestamp_nano.clone();
        TimestampDeltas {
            head_timestamp,
            timestamps_deltas_second,
            common_trailing_zero_bits: common_trailing_zero_bits as u8,
            timestamps_nanoseconds: timestamps_sub_nanoseconds,
        }
    }
}

pub fn read_from_block_file<P: AsRef<Path>>(
    path: P,
    field_selectors: Option<&[usize]>,
) -> Result<TimeSeriesDataFrame> {
    let block_file =
        File::open(path.as_ref()).map_err(|e| BlockError::file_error(e, path.as_ref()))?;
    let block_data = unsafe {
        MmapOptions::new()
            .map(&block_file)
            .map_err(|e| BlockError::file_error(e, path))?
    };
    read::read_from_block_with_specific_fields(&block_data, field_selectors)
}

pub fn write_to_block_file<P: AsRef<Path>>(path: P, datapoints: &[DataPoint]) -> Result<()> {
    let mut block_file = if path.as_ref().exists() {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .map_err(|e| BlockError::file_error(e, path.as_ref()))?
    } else {
        File::create(path.as_ref()).map_err(|e| BlockError::file_error(e, path.as_ref()))?
    };

    write::write_to_block(&mut block_file, datapoints)?;
    block_file
        .flush()
        .map_err(|e| BlockError::file_error(e, path))?;
    Ok(())
}

#[cfg(test)]
mod test {

    use super::*;
    use tempfile;

    macro_rules! empty_data_points {
        ($($timestamp:expr),*) => {
            vec![
            $(DataPoint::new(ts!($timestamp),vec![])),*
            ]
        };
    }

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

    #[test]
    fn test_timestamp_deltas_1() {
        let datapoints = empty_data_points!(
            1629745451_715062000,
            1629745452_715062000,
            1629745452_715062100
        );

        let TimestampDeltas {
            head_timestamp,
            timestamps_deltas_second,
            common_trailing_zero_bits,
            timestamps_nanoseconds,
        } = TimestampDeltas::from(datapoints.as_slice());

        assert_eq!(head_timestamp, datapoints.get(0).unwrap().timestamp_nano);

        assert_eq!(timestamps_deltas_second, vec![1, 0]);
        assert_eq!(timestamps_nanoseconds, vec![178765500, 178765525]);

        assert_eq!(common_trailing_zero_bits, 2u8);
    }

    #[test]
    fn test_timestamp_deltas_2() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715064000, vec![200f64,36f64]}
        );

        let timestamp_deltas = TimestampDeltas::from(datapoints.as_slice());

        let tss = timestamp_deltas.as_timestamps();
        assert_eq!(
            tss,
            vec![
                TimestampNano::new(1629745451_715062000),
                TimestampNano::new(1629745451_715064000)
            ]
        );
    }

    #[test]
    fn test_timestamp_deltas_3() {
        let datapoints = float_data_points!(
            {1629745451_715066000, vec![300f64,36f64]},
            {1639745451_715061000, vec![1300f64,36f64]}
        );

        let timestamp_deltas = TimestampDeltas::from(datapoints.as_slice());

        let tss = timestamp_deltas.as_timestamps();
        assert_eq!(
            tss,
            vec![
                TimestampNano::new(1629745451_715066000),
                TimestampNano::new(1639745451_715061000)
            ]
        );
    }

    #[test]
    fn test_block_1() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64]}

        );

        let mut data = Vec::<u8>::new();
        let result = write::write_to_block(&mut data, &datapoints);
        assert!(result.is_ok());

        let read_data = read::read_from_block_with_specific_fields(&data, None);

        assert!(read_data.is_ok());
        let read_data = read_data.unwrap();
        assert_eq!(read_data.len(), 1);
        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }

    #[test]
    fn test_block_2() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]}

        );

        let mut data = Vec::<u8>::new();
        let result = write::write_to_block(&mut data, &datapoints);
        assert!(result.is_ok());

        let read_data = read::read_from_block_with_specific_fields(&data, None);

        assert!(read_data.is_ok());
        let read_data = read_data.unwrap();
        assert_eq!(read_data.len(), 1);
        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }

    #[test]
    fn test_block_3() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715064000, vec![200f64,36f64]}
        );

        let mut data = Vec::<u8>::new();
        let result = write::write_to_block(&mut data, &datapoints);
        assert!(result.is_ok());

        let read_data = read::read_from_block_with_specific_fields(&data, None);

        assert!(read_data.is_ok());
        let read_data = read_data.unwrap();
        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }

    #[test]
    fn test_block_4() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629755451_715064000, vec![200f64,36f64]}
        );

        let mut data = Vec::<u8>::new();
        let result = write::write_to_block(&mut data, &datapoints);
        assert!(result.is_ok());

        let read_data = read::read_from_block_with_specific_fields(&data, None);

        assert!(read_data.is_ok());
        let read_data = read_data.unwrap();
        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }

    #[test]
    fn test_block_5() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715062000, vec![200f64,36f64]},
            {1629745451_715062000, vec![200f64,36f64]}
        );

        let mut data = Vec::<u8>::new();
        let result = write::write_to_block(&mut data, &datapoints);
        assert!(result.is_ok());

        let read_data = read::read_from_block_with_specific_fields(&data, None);

        assert!(read_data.is_ok());
        let read_data = read_data.unwrap();
        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }

    #[test]
    fn test_block_6() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715062000, vec![200f64,36f64]},
            {2629745451_715062000, vec![200f64,36f64]}
        );

        let mut data = Vec::<u8>::new();
        let result = write::write_to_block(&mut data, &datapoints);
        assert!(result.is_ok());

        let read_data = read::read_from_block_with_specific_fields(&data, None);

        assert!(read_data.is_ok());
        let read_data = read_data.unwrap();
        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }

    #[test]
    fn test_block_7() {
        let datapoints = float_data_points!(
            {1629745451_715066000, vec![300f64,36f64]},
            {1639745451_715061000, vec![1300f64,36f64]}
        );

        let mut data = Vec::<u8>::new();
        let result = write::write_to_block(&mut data, &datapoints);
        assert!(result.is_ok());

        let read_data = read::read_from_block_with_specific_fields(&data, None);

        assert!(read_data.is_ok());
        let read_data = read_data.unwrap();
        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }

    #[test]
    fn test_multiple_types_block_1() {
        let datapoints = vec![DataPoint::new(
            ts!(1629745451_715066000),
            vec![FieldValue::Bool(true), FieldValue::Float64(300f64)],
        )];

        let mut data = Vec::<u8>::new();
        let result = write::write_to_block(&mut data, &datapoints);
        assert!(result.is_ok());

        let read_data = read::read_from_block_with_specific_fields(&data, None);

        assert!(read_data.is_ok());
        let read_data = read_data.unwrap();
        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }

    #[test]
    fn test_multiple_types_block_2w() {
        let datapoints = vec![
            DataPoint::new(
                ts!(1629745451_715066000),
                vec![FieldValue::Bool(true), FieldValue::Float64(300f64)],
            ),
            DataPoint::new(
                ts!(1629745452_715066000),
                vec![FieldValue::Bool(false), FieldValue::Float64(301f64)],
            ),
            DataPoint::new(
                ts!(1629745452_715066000),
                vec![FieldValue::Bool(true), FieldValue::Float64(301f64)],
            ),
            DataPoint::new(
                ts!(1629745453_715066000),
                vec![FieldValue::Bool(false), FieldValue::Float64(302f64)],
            ),
            DataPoint::new(
                ts!(1629745454_715066000),
                vec![FieldValue::Bool(false), FieldValue::Float64(303f64)],
            ),
            DataPoint::new(
                ts!(1629745455_715066000),
                vec![FieldValue::Bool(true), FieldValue::Float64(304f64)],
            ),
            DataPoint::new(
                ts!(1629745456_715066000),
                vec![FieldValue::Bool(true), FieldValue::Float64(305f64)],
            ),
            DataPoint::new(
                ts!(1629745457_715066000),
                vec![FieldValue::Bool(false), FieldValue::Float64(306f64)],
            ),
            DataPoint::new(
                ts!(1629745458_715066000),
                vec![FieldValue::Bool(false), FieldValue::Float64(307f64)],
            ),
        ];

        let mut data = Vec::<u8>::new();
        let result = write::write_to_block(&mut data, &datapoints);
        assert!(result.is_ok());

        let read_data = read::read_from_block_with_specific_fields(&data, None);

        assert!(read_data.is_ok());
        let read_data = read_data.unwrap();
        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }

    #[test]
    fn test_block_file_1() {
        let target_file = tempfile::NamedTempFile::new().unwrap();
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64, 12f64]},
            {1629745451_715062000, vec![200f64, 36f64]},
            {1629746451_715062000, vec![200f64, 36f64]}
        );

        let result = write_to_block_file(target_file.as_ref(), &datapoints);
        assert!(result.is_ok());

        let result = read_from_block_file(target_file.as_ref(), None);
        assert!(result.is_ok());
        let read_data = result.unwrap();

        assert_eq!(read_data.into_datapoints().unwrap(), datapoints);
    }
}
