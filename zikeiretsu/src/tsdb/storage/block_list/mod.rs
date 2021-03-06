/// block list file format
///
///
///  (1) updated timestamp(8 byte)
///  (2) number of data (n bytes)
///  (3) timestamp second head (since)(v byte)
///  (4) timestamp second deltas(since)(v byte)
///  (5) timestamp second head (untile)(v byte)
///  (6) timestamp second (until)(v byte)
///  (7) timestamp nums in each blocks (v byte)
///
mod block_timestamp;

use crate::tsdb::{metrics::Metrics, timestamp_nano::*, timestamp_sec::*};
use crate::FieldError;
use base_128_variants;
use bits_ope::*;
pub use block_timestamp::*;
use memmap2::MmapOptions;
use searcher::*;
use serde::{Deserialize, Serialize};
use simple8b_rle;
use std::cmp::Ordering;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::Write;
use std::iter::Iterator;
use std::path::Path;
use thiserror::Error;
use xor_encoding;

pub const BLOCK_LIST_FILE_NAME_PATTERN: &str = r"([\w-]+?).list";

type Result<T> = std::result::Result<T, BlockListError>;

#[derive(Error, Debug)]
pub enum BlockListError {
    #[error(" block timstamp is empty")]
    EmptyBlockTimestampNano,

    #[error("invalid block timestamp: block timstamp is not sorted. {0} ")]
    BlockTimestampIsNotSorted(Metrics),

    #[error("invalid block list path error. {0}")]
    InvalidBlockListPathError(String),

    #[error("block list file error. {0}")]
    FileError(#[from] std::io::Error),

    #[error("invalid block list file : {0} at {1}")]
    InvalidBlocklistFileError(String, usize),

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
}

#[derive(Debug)]
pub(crate) struct TimestampSecDeltas {
    pub head_timestamp_sec: TimestampSec,
    pub timestamps_deltas_second: Vec<u64>,
}

impl TimestampSecDeltas {
    pub fn as_timestamp_secs(&self) -> Vec<TimestampSec> {
        let mut timestamps = vec![self.head_timestamp_sec];
        let mut prev_timestamp = self.head_timestamp_sec;

        for each_delta in self.timestamps_deltas_second.iter() {
            let each_timestmap = prev_timestamp + *each_delta;
            timestamps.push(each_timestmap);
            prev_timestamp = each_timestmap
        }
        timestamps
    }
}

impl From<Vec<TimestampSec>> for TimestampSecDeltas {
    fn from(timestamp_secs: Vec<TimestampSec>) -> TimestampSecDeltas {
        debug_assert!(!timestamp_secs.is_empty());
        let head_timestamp_sec = *unsafe { timestamp_secs.get_unchecked(0) };
        let mut prev = &head_timestamp_sec;

        let mut timestamps_deltas_second = Vec::<u64>::new();

        for each_timestamp in timestamp_secs.as_slice()[1..].iter() {
            let delta = each_timestamp - prev;
            timestamps_deltas_second.push(delta);
            prev = each_timestamp;
        }

        TimestampSecDeltas {
            head_timestamp_sec,
            timestamps_deltas_second,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct BlockMetaInfo {
    pub block_timestamp: BlockTimestamp,
    pub timestamp_num: usize,
}
impl BlockMetaInfo {
    pub(crate) fn new(block_timestamp: BlockTimestamp, timestamp_num: usize) -> Self {
        Self {
            block_timestamp,
            timestamp_num,
        }
    }

    pub fn from_splited_timestamps(
        since_secs: Vec<TimestampSec>,
        until_secs: Vec<TimestampSec>,
        timestmap_nums: Vec<u64>,
    ) -> Vec<BlockMetaInfo> {
        debug_assert_eq!(since_secs.len(), until_secs.len());
        let timestamp_pairs: Vec<(TimestampSec, TimestampSec)> =
            since_secs.into_iter().zip(until_secs.into_iter()).collect();
        let block_timestsamps = BlockTimestamp::from_timestamp_pairs(timestamp_pairs);

        block_timestsamps
            .into_iter()
            .zip(timestmap_nums.into_iter())
            .map(|(block_timestamp, timestamp_num)| {
                BlockMetaInfo::new(block_timestamp, timestamp_num as usize)
            })
            .collect()
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct BlockList {
    pub metrics: Metrics,
    pub updated_timestamp_sec: TimestampNano,
    pub block_meta_infos: Vec<BlockMetaInfo>,
}

impl BlockList {
    pub(crate) fn new(
        metrics: Metrics,
        updated_timestamp_sec: TimestampNano,
        block_meta_infos: Vec<BlockMetaInfo>,
    ) -> Self {
        Self {
            metrics,
            updated_timestamp_sec,
            block_meta_infos,
        }
    }

    pub fn block_num(&self) -> usize {
        self.block_meta_infos.len()
    }

    pub fn range(&self) -> Option<(&TimestampSec, &TimestampSec)> {
        let mut min: Option<&TimestampSec> = None;
        let mut max: Option<&TimestampSec> = None;

        for each in self.block_meta_infos.iter() {
            match min {
                Some(current_min) => {
                    if each.block_timestamp.since_sec < *current_min {
                        min = Some(&each.block_timestamp.since_sec)
                    }
                }
                None => min = Some(&each.block_timestamp.since_sec),
            }

            match max {
                Some(current_max) => {
                    if each.block_timestamp.until_sec > *current_max {
                        max = Some(&each.block_timestamp.until_sec)
                    }
                }
                None => max = Some(&each.block_timestamp.until_sec),
            }
        }
        match (min, max) {
            (None, None) => None,
            (Some(min), Some(max)) => Some((min, max)),
            _ => panic!("range of block list has bug"),
        }
    }
    pub fn update_updated_at(&mut self, dt: TimestampNano) {
        self.updated_timestamp_sec = dt;
    }

    pub fn add_blockmeta(&mut self, meta_info: BlockMetaInfo) -> Result<()> {
        // in almost case,  the new block_timestamp will be inserted at the tail
        let mut insert_at = 0;
        for (idx, each) in self.block_meta_infos.iter().rev().enumerate() {
            if each.block_timestamp.until_sec <= meta_info.block_timestamp.until_sec {
                insert_at = self.block_meta_infos.len() - idx;
                break;
            }
        }
        if insert_at == self.block_meta_infos.len() {
            self.block_meta_infos.push(meta_info);
        } else {
            self.block_meta_infos.insert(insert_at, meta_info);
        }

        Ok(())
    }

    fn check_block_timestamp_is_sorted(&self) -> Result<()> {
        if self.block_meta_infos.is_empty() {
            Ok(())
        } else {
            let mut prev = unsafe { self.block_meta_infos.get_unchecked(0) };
            for each_block_meta in self.block_meta_infos.as_slice()[1..].iter() {
                if each_block_meta
                    .block_timestamp
                    .until_sec
                    .cmp(&prev.block_timestamp.until_sec)
                    == Ordering::Less
                {
                    return Err(BlockListError::BlockTimestampIsNotSorted(
                        self.metrics.clone(),
                    ));
                }
                prev = each_block_meta;
            }

            Ok(())
        }
    }

    fn split_block_list_timestamps(&self) -> (Vec<TimestampSec>, Vec<TimestampSec>, Vec<usize>) {
        let mut sinces = Vec::<TimestampSec>::new();
        let mut untils = Vec::<TimestampSec>::new();
        let mut timesamp_nums = Vec::<usize>::new();

        for each in self.block_meta_infos.iter() {
            sinces.push(each.block_timestamp.since_sec);
            untils.push(each.block_timestamp.until_sec);
            timesamp_nums.push(each.timestamp_num);
        }

        (sinces, untils, timesamp_nums)
    }

    pub fn search(
        &self,
        since_inclusive: Option<&TimestampSec>,
        until_exclusive: Option<&TimestampSec>,
    ) -> Result<Option<&[BlockMetaInfo]>> {
        debug_assert!(self.check_block_timestamp_is_sorted().is_ok());

        let block_meta_infos = self.block_meta_infos.as_slice();

        log::debug!(
            "block_list. all block timestamps num: {:?}",
            block_meta_infos.len()
        );

        match (since_inclusive, until_exclusive) {
            (Some(since), Some(until)) => {
                let lower_idx = binary_search_by(
                    block_meta_infos,
                    |block_meta| block_meta.block_timestamp.until_sec.cmp(since),
                    BinaryRangeSearchType::AtLeastInclusive,
                );

                match lower_idx {
                    None => Ok(None),
                    Some(lower_idx) => {
                        let upper_idx = binary_search_by(
                            block_meta_infos,
                            |block_meta| block_meta.block_timestamp.since_sec.cmp(until),
                            BinaryRangeSearchType::AtMostInclusive,
                        );

                        match upper_idx {
                            Some(upper_idx) => {
                                Ok(Some(&block_meta_infos[lower_idx..upper_idx + 1]))
                            }
                            None => Ok(None),
                        }
                    }
                }
            }

            (Some(since), None) => {
                let lower_idx = binary_search_by(
                    block_meta_infos,
                    |block_meta| block_meta.block_timestamp.until_sec.cmp(since),
                    BinaryRangeSearchType::AtLeastInclusive,
                );

                match lower_idx {
                    Some(lower_idx) => Ok(Some(&block_meta_infos[lower_idx..])),
                    None => Ok(None),
                }
            }

            (None, Some(until)) => {
                let upper_idx = binary_search_by(
                    block_meta_infos,
                    |block_meta| block_meta.block_timestamp.since_sec.cmp(until),
                    BinaryRangeSearchType::AtMostInclusive,
                );

                match upper_idx {
                    Some(upper_idx) => Ok(Some(&block_meta_infos[..upper_idx + 1])),
                    None => Ok(None),
                }
            }
            (None, None) => {
                if block_meta_infos.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(block_meta_infos))
                }
            }
        }
    }
}

pub(crate) fn write_to_blocklist<W>(mut block_list_file: W, block_list: BlockList) -> Result<()>
where
    W: Write,
{
    let block_timestamp_size = block_list.block_num();
    if block_timestamp_size == 0 {
        return Err(BlockListError::EmptyBlockTimestampNano);
    }
    #[cfg(feature = "validate")]
    block_list.check_block_timestamp_is_sorted()?;

    //  (1) updated timestamp(8 byte)
    {
        let mut bits_writer = BitsWriter::default();
        bits_writer.append(u64_bits_reader!(*block_list.updated_timestamp_sec, 64)?, 64)?;
        bits_writer.flush(&mut block_list_file)?;
    }

    //  (2) number of block timestamps (n bytes)
    base_128_variants::compress_u64(block_timestamp_size as u64, &mut block_list_file)?;

    let (sinces, untils, timestamp_nums) = block_list.split_block_list_timestamps();

    //  (3) timestamp second head (since)(v byte)
    //  (4) timestamp second deltas(since)(v byte)
    write_timestamp_sec_and_deltas(sinces, &mut block_list_file)?;

    //  (5) timestamp second head (untile)(v byte)
    //  (6) timestamp second (until)(v byte)
    write_timestamp_sec_and_deltas(untils, &mut block_list_file)?;
    // (7) timestamps nums in each block
    simple8b_rle::compress(
        &timestamp_nums
            .iter()
            .map(|e| *e as u64)
            .collect::<Vec<u64>>(),
        &mut block_list_file,
    )?;

    Ok(())
}

fn write_timestamp_sec_and_deltas<W>(
    timestamp_secs: Vec<TimestampSec>,
    block_file: &mut W,
) -> Result<()>
where
    W: Write,
{
    let delta = TimestampSecDeltas::from(timestamp_secs);

    base_128_variants::compress_u64(*delta.head_timestamp_sec, block_file)?;
    simple8b_rle::compress(&delta.timestamps_deltas_second, block_file)?;
    Ok(())
}

pub(crate) fn read_from_blocklist_file<P: AsRef<Path>>(
    metrics: &Metrics,
    path: P,
) -> Result<BlockList> {
    let block_list_file = File::open(path)?;
    let block_list_data = unsafe { MmapOptions::new().map(&block_list_file)? };
    read_from_blocklist(metrics, &block_list_data)
}

pub(crate) fn write_to_block_listfile<P: AsRef<Path>>(
    path: P,
    block_list: BlockList,
) -> Result<()> {
    let mut block_list_file = if path.as_ref().exists() {
        OpenOptions::new().read(true).write(true).open(path)?
    } else {
        let parent_dir = path.as_ref().parent().ok_or_else(|| {
            BlockListError::InvalidBlockListPathError(path.as_ref().display().to_string())
        })?;

        create_dir_all(parent_dir)?;

        File::create(path)?
    };

    write_to_blocklist(&mut block_list_file, block_list)?;
    block_list_file.flush()?;
    Ok(())
}

pub(crate) fn read_from_blocklist(metrics: &Metrics, block_data: &[u8]) -> Result<BlockList> {
    //  (1) updated timestamp(8 byte)
    let mut block_idx = 0;
    let (updated_timestamp_sec, consumed_idx): (TimestampNano, usize) = {
        let mut reader = RefBitsReader::new(&block_data[block_idx..]);
        match reader.chomp_as_u64(64)? {
            Some(head_timestamp) => (
                TimestampNano::new(head_timestamp),
                reader.current_byte_index() + 1,
            ),
            None => {
                return Err(BlockListError::InvalidBlocklistFileError(
                    "no `updated timestamp` data".to_string(),
                    block_idx,
                ))
            }
        }
    };
    block_idx += consumed_idx;

    //  (2) number of block timestamps (n bytes)
    let (number_of_block_timstamps, consumed_idx) =
        base_128_variants::decompress_u64(&block_data[block_idx..])?;

    let number_of_block_timstamps_deltas = number_of_block_timstamps - 1;
    block_idx += consumed_idx;

    //  (3) timestamp second head (since)(v byte)
    //  (4) timestamp second deltas(since)(v byte)
    let (since_timedeltas, block_idx) = read_timestamp_sec_and_deltas(
        block_data,
        number_of_block_timstamps_deltas as usize,
        block_idx,
    )?;

    //  (5) timestamp second head (untile)(v byte)
    //  (6) timestamp second (until)(v byte)
    let (until_timedeltas, block_idx) = read_timestamp_sec_and_deltas(
        block_data,
        number_of_block_timstamps_deltas as usize,
        block_idx,
    )?;

    // (7) timestamps nums in each block
    let mut timestamp_nums = Vec::<u64>::new();
    let _block_idx = simple8b_rle::decompress(
        &block_data[block_idx..],
        &mut timestamp_nums,
        Some(number_of_block_timstamps as usize),
    )?;

    //TODO(tacogips) rename from_splited_timestamps
    let block_meta_infos = BlockMetaInfo::from_splited_timestamps(
        since_timedeltas.as_timestamp_secs(),
        until_timedeltas.as_timestamp_secs(),
        timestamp_nums,
    );

    let block_list = BlockList {
        metrics: metrics.clone(),
        updated_timestamp_sec,
        block_meta_infos,
    };

    Ok(block_list)
}

fn read_timestamp_sec_and_deltas(
    block_data: &[u8],
    number_of_block_timstamps_deltas: usize,
    mut block_idx: usize,
) -> Result<(TimestampSecDeltas, usize)> {
    let (head_timestamp_sec, consumed_idx) =
        base_128_variants::decompress_u64(&block_data[block_idx..])?;
    let head_timestamp_sec = TimestampSec::new(head_timestamp_sec);
    block_idx += consumed_idx;
    let mut timestamps_deltas_second = Vec::<u64>::new();
    let consumed_idx = simple8b_rle::decompress(
        &block_data[block_idx..],
        &mut timestamps_deltas_second,
        Some(number_of_block_timstamps_deltas),
    )?;
    block_idx += consumed_idx;

    Ok((
        TimestampSecDeltas {
            head_timestamp_sec,
            timestamps_deltas_second,
        },
        block_idx,
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tsdb::metrics::Metrics;

    #[test]
    fn test_wr_block_list_1() {
        let mut dest = Vec::<u8>::new();

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = {
            let meta1 = BlockMetaInfo::new(
                BlockTimestamp::new(TimestampSec::new(1629745452), TimestampSec::new(1629745453)),
                2,
            );

            let meta2 = BlockMetaInfo::new(
                BlockTimestamp::new(TimestampSec::new(1629745454), TimestampSec::new(1629745455)),
                10,
            );

            let updated_timestamp = TimestampNano::new(1629745452_715062000);

            BlockList::new(metrics.clone(), updated_timestamp, vec![meta1, meta2])
        };

        let result = write_to_blocklist(&mut dest, block_list.clone());
        assert!(result.is_ok());

        let result = read_from_blocklist(&metrics, &mut dest);
        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(result, block_list);
    }

    macro_rules! blts {
        ($since:expr,$until:expr) => {
            BlockTimestamp::new(TimestampSec::new($since), TimestampSec::new($until))
        };
    }

    macro_rules! blmeta {
        ($since:expr,$until:expr) => {
            BlockMetaInfo::new(
                BlockTimestamp::new(TimestampSec::new($since), TimestampSec::new($until)),
                10,
            )
        };
    }

    macro_rules! block_timestamps {
        ($({$since:expr,$until:expr}),*) => {
            vec![
                $(blts!($since,$until) ),*
            ]
        };
    }

    macro_rules! block_metas {
        ($({$since:expr,$until:expr}),*) => {
            vec![
                $(blmeta!($since,$until) ),*
            ]
        };
    }

    macro_rules! ts {
        ($v:expr) => {
            TimestampSec::new($v)
        };
    }

    #[test]
    fn test_block_timestamps_insert_1() {
        let mut block_timestamps = block_timestamps!({10,20},{10,20}, {21,30});
        BlockTimestamp::insert(&mut block_timestamps, blts!(10, 15));
        assert_eq!(
            block_timestamps,
            block_timestamps!({10,20},{10,20}, {10,15},{21,30})
        );
    }

    #[test]
    fn test_block_timestamps_insert_2() {
        let mut block_timestamps = block_timestamps!({10,20},{10,20}, {21,30});
        BlockTimestamp::insert(&mut block_timestamps, blts!(22, 50));
        assert_eq!(
            block_timestamps,
            block_timestamps!({10,20},{10,20}, {21,30},{22,50})
        );
    }

    #[test]
    fn test_block_timestamps_search_1() {
        let block_meta_infos =
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&ts!(11)), Some(&ts!(15)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_metas!({10,20},{10,20}, {10,20}, {11,30}, {11,30}, {12,30}, {15,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_2() {
        let block_meta_infos =
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&ts!(10)), Some(&ts!(15)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_3() {
        let block_meta_infos =
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&ts!(10)), Some(&ts!(22)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_4() {
        let block_meta_infos =
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&ts!(10)), Some(&ts!(22)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_5() {
        let block_meta_infos = block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30},{21,31});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&ts!(10)), Some(&ts!(21)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30},{21,31})
        );
    }

    #[test]
    fn test_block_timestamps_search_6() {
        let block_meta_infos =
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(None, Some(&ts!(13)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_7() {
        let block_meta_infos =
            block_metas!({10,11},{10,12}, {10,13},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&ts!(13)), None);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_metas!({10,13},{11,30}, {11,30}, {12,30}, {15,30},{21,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_8() {
        let block_meta_infos =
            block_metas!({10,20},{10,20}, {10,20},{11,21}, {11,22}, {12,30}, {15,30},{21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&ts!(22)), None);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_metas!( {11,22}, {12,30}, {15,30},{21,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_9() {
        let block_meta_infos =
            block_metas!({9,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30}, {21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(None, Some(&ts!(9)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.unwrap(), block_metas!({9,20}));
    }

    #[test]
    fn test_block_timestamps_search_10() {
        let block_meta_infos =
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30}, {21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&ts!(4)), Some(&ts!(9)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_block_timestamps_search_11() {
        let block_meta_infos =
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30}, {21,30});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(None, None);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_metas!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30}, {21,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_12() {
        let block_meta_infos = block_metas!({1632735700,1632735903});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(
            Some(&TimestampSec::new(1632735720)),
            Some(&TimestampSec::new(1632735903)),
        );
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), block_metas!({1632735700,1632735903}));
    }

    #[test]
    fn test_block_timestamps_search_13() {
        let block_meta_infos = block_metas!({10,12},{21,23},{30,36});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&TimestampSec::new(22)), None);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), block_metas!({21,23},{30,36}));
    }

    #[test]
    fn test_block_timestamps_search_14() {
        let block_meta_infos = block_metas!({10,12},{21,23},{30,36});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(None, Some(&TimestampSec::new(22)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), block_metas!({10,12},{21,23}));
    }

    #[test]
    fn test_block_timestamps_search_15() {
        let block_meta_infos = block_metas!({10,12},{21,23},{30,36});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(None, Some(&TimestampSec::new(9)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_block_timestamps_search_16() {
        let block_meta_infos = block_metas!({10,12},{21,23},{30,36});

        let metrics = Metrics::new("dummy").unwrap();
        let block_list = BlockList {
            metrics,
            updated_timestamp_sec: TimestampNano::new(0),
            block_meta_infos,
        };

        let result = block_list.search(Some(&TimestampSec::new(40)), None);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_block_list_add_timestamps_1() {
        let updated_timestamp = TimestampNano::new(1629745452_715062000);

        let metrics = Metrics::new("dummy").unwrap();
        let mut blocklist = BlockList::new(metrics, updated_timestamp, vec![]);
        block_timestamps!({10,20});
        {
            let block_meta = blmeta!(10, 20);
            let result = blocklist.add_blockmeta(block_meta);
            assert!(result.is_ok());

            let expected = block_metas!({10,20});
            assert_eq!(blocklist.block_meta_infos, expected);
        }

        {
            let meta = blmeta!(21, 22);
            let result = blocklist.add_blockmeta(meta);
            assert!(result.is_ok());

            let expected = block_metas!({10, 20},{21, 22});
            assert_eq!(blocklist.block_meta_infos, expected);
        }

        {
            let meta = blmeta!(9, 10);
            let result = blocklist.add_blockmeta(meta);
            assert!(result.is_ok());

            let expected = block_metas!({9, 10}, {10, 20},{21, 22});
            assert_eq!(blocklist.block_meta_infos, expected);
        }

        {
            let meta = blmeta!(10, 10);
            let result = blocklist.add_blockmeta(meta);
            assert!(result.is_ok());

            let expected = block_metas!({9, 10}, {10, 10},{10, 20},{21, 22});
            assert_eq!(blocklist.block_meta_infos, expected);
        }

        {
            let meta = blmeta!(23, 23);
            let result = blocklist.add_blockmeta(meta);
            assert!(result.is_ok());

            let expected = block_metas!({9, 10}, {10, 10},{10, 20},  {21, 22},{23,23});
            assert_eq!(blocklist.block_meta_infos, expected);
        }
    }

    #[test]
    fn test_block_list_add_timestamps_2() {
        let updated_timestamp = TimestampNano::new(1629745452_715062000);

        let init_metas = block_metas!(
            { 1638257405, 1638257436 },
            { 1638257435, 1638257467 },
            { 1638268342, 1638268372 },
            { 1638268372, 1638268404 },
            { 1638275138, 1638275169 }
        );

        let metrics = Metrics::new("dummy").unwrap();
        let mut blocklist = BlockList::new(metrics, updated_timestamp, init_metas);
        {
            let block_meta = blmeta!(1638275168, 1638275200);
            let result = blocklist.add_blockmeta(block_meta);
            assert!(result.is_ok());

            let expected = block_metas!(
                { 1638257405, 1638257436 },
                { 1638257435, 1638257467 },
                { 1638268342, 1638268372 },
                { 1638268372, 1638268404 },
                { 1638275138, 1638275169 },
                { 1638275168, 1638275200 }
            );
            assert_eq!(blocklist.block_meta_infos, expected);
        }
    }

    #[test]
    fn test_block_list_add_timestamps_3() {
        let updated_timestamp = TimestampNano::new(1629745452_715062000);

        let init_metas = block_metas!(
        {1638257405,1638257436 },
        {1638257435,1638257467 },
        {1638268342,1638268372 },
        {1638268372,1638268404 },
        {1638275138,1638275169 },
        {1638275615,1638275617 },
        {1638276635,1638276665 },
        {1638276665,1638276697 });

        let metrics = Metrics::new("dummy").unwrap();
        let mut blocklist = BlockList::new(metrics, updated_timestamp, init_metas);
        {
            let block_timestamp = blmeta!(1638276696, 1638276728);
            let result = blocklist.add_blockmeta(block_timestamp);
            assert!(result.is_ok());

            let expected = block_metas!(
            { 1638257405,1638257436 },
            { 1638257435,1638257467 },
            { 1638268342,1638268372 },
            { 1638268372,1638268404 },
            { 1638275138,1638275169 },
            { 1638275615,1638275617 },
            { 1638276635,1638276665 },
            { 1638276665,1638276697 },
            { 1638276696,1638276728 });
            assert_eq!(blocklist.block_meta_infos, expected);
        }
    }
}
