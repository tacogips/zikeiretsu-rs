/// block list file format
///
///
///  (1) updated timestamp(8 byte)
///  (2) number of data (n bytes)
///  (3) timestamp second head (since)(v byte)
///  (4) timestamp second deltas(since)(v byte)
///  (5) timestamp second head (untile)(v byte)
///  (6) timestamp second (until)(v byte)
///
use crate::tsdb::search::*;
use crate::tsdb::{timestamp_nano::*, timestamp_sec::*};
use crate::FieldError;
use base_128_variants;
use bits_ope::*;
use memmap2::MmapOptions;
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

    #[error("invalid block timestamp: block timstamp is not sorted ")]
    BlockTimestampIsNotSorted,

    #[error("invalid block list path error")]
    InvalidBlockListPathError(String),

    #[error("block list file error {0}")]
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
    pub fn as_timestamp_secs(self) -> Vec<TimestampSec> {
        let mut timestamps = Vec::<TimestampSec>::new();
        timestamps.push(self.head_timestamp_sec);
        let mut prev_timestamp = self.head_timestamp_sec;

        for each_delta in self.timestamps_deltas_second {
            let each_timestmap = prev_timestamp + each_delta;
            timestamps.push(each_timestmap);
            prev_timestamp = each_timestmap
        }
        timestamps
    }
}

impl From<Vec<TimestampSec>> for TimestampSecDeltas {
    fn from(timestamp_secs: Vec<TimestampSec>) -> TimestampSecDeltas {
        debug_assert!(!timestamp_secs.is_empty());
        let head_timestamp_sec = unsafe { timestamp_secs.get_unchecked(0) }.clone();
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

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct BlockList {
    pub updated_timestamp_sec: TimestampNano,
    pub block_timestamps: Vec<BlockTimestamp>,
}

impl BlockList {
    pub(crate) fn new(
        updated_timestamp_sec: TimestampNano,
        block_timestamps: Vec<BlockTimestamp>,
    ) -> Self {
        Self {
            updated_timestamp_sec,
            block_timestamps,
        }
    }

    pub fn update_updated_at(&mut self, dt: TimestampNano) {
        self.updated_timestamp_sec = dt;
    }

    pub fn add_timestamp(&mut self, block_timestamp: BlockTimestamp) -> Result<()> {
        // in almost  all case,  the block_timestamp will be stored at the tail
        let mut insert_at = self.block_timestamps.len();
        for (idx, each) in self.block_timestamps.iter().rev().enumerate() {
            if each.until_sec <= block_timestamp.since_sec {
                insert_at = self.block_timestamps.len() - idx;
                break;
            }
        }
        if insert_at == self.block_timestamps.len() {
            self.block_timestamps.push(block_timestamp);
        } else {
            self.block_timestamps.insert(insert_at, block_timestamp);
        }

        Ok(())
    }

    fn check_block_timestamp_is_sorted(&self) -> Result<()> {
        if self.block_timestamps.is_empty() {
            Ok(())
        } else {
            let mut prev = unsafe { self.block_timestamps.get_unchecked(0) };
            for each_block_timestamp in self.block_timestamps.as_slice()[1..].iter() {
                if each_block_timestamp.since_sec.cmp(&prev.since_sec) == Ordering::Less {
                    return Err(BlockListError::BlockTimestampIsNotSorted);
                }
                prev = each_block_timestamp;
            }

            Ok(())
        }
    }

    fn block_timestamp_num(&self) -> usize {
        self.block_timestamps.len()
    }

    fn split_block_list_timestamps(&self) -> (Vec<TimestampSec>, Vec<TimestampSec>) {
        let mut sinces = Vec::<TimestampSec>::new();
        let mut untils = Vec::<TimestampSec>::new();

        for each in self.block_timestamps.iter() {
            sinces.push(each.since_sec);
            untils.push(each.until_sec);
        }

        (sinces, untils)
    }

    pub fn search(
        &self,
        since: Option<&TimestampSec>,
        until: Option<&TimestampSec>,
    ) -> Result<Option<&[BlockTimestamp]>> {
        //TODO(tacogis)  maybe redundunt
        self.check_block_timestamp_is_sorted()?;

        let block_timestamps = self.block_timestamps.as_slice();

        match (since, until) {
            (Some(since), Some(until)) => {
                let lower_idx = binary_search_by(
                    block_timestamps,
                    |block_timestamp| block_timestamp.since_sec.cmp(&since),
                    BinaryRangeSearchType::AtLeast,
                );

                match lower_idx {
                    None => Ok(None),
                    Some(lower_idx) => {
                        let upper_idx = binary_search_by(
                            block_timestamps,
                            |block_timestamp| block_timestamp.since_sec.cmp(&until),
                            BinaryRangeSearchType::AtMost,
                        );

                        match upper_idx {
                            Some(upper_idx) => {
                                Ok(Some(&block_timestamps[lower_idx..upper_idx + 1]))
                            }
                            None => Ok(None),
                        }
                    }
                }
            }

            (Some(since), None) => {
                let lower_idx = binary_search_by(
                    block_timestamps,
                    |block_timestamp| block_timestamp.since_sec.cmp(&since),
                    BinaryRangeSearchType::AtLeast,
                );

                match lower_idx {
                    Some(lower_idx) => Ok(Some(&block_timestamps[lower_idx..])),
                    None => Ok(None),
                }
            }

            (None, Some(until)) => {
                let upper_idx = binary_search_by(
                    block_timestamps,
                    |block_timestamp| block_timestamp.since_sec.cmp(&until),
                    BinaryRangeSearchType::AtMost,
                );

                match upper_idx {
                    Some(upper_idx) => Ok(Some(&block_timestamps[..upper_idx + 1])),
                    None => Ok(None),
                }
            }
            (None, None) => {
                if block_timestamps.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(block_timestamps))
                }
            }
        }
    }
}

#[derive(Copy, Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct BlockTimestamp {
    pub since_sec: TimestampSec,
    pub until_sec: TimestampSec,
}

impl BlockTimestamp {
    #[allow(dead_code)]
    pub fn new(since_sec: TimestampSec, until_sec: TimestampSec) -> Self {
        Self {
            since_sec,
            until_sec,
        }
    }

    pub fn is_before(&self, other: &Self) -> bool {
        self.until_sec <= other.since_sec
    }

    #[allow(dead_code)]
    pub fn is_after(&self, other: &Self) -> bool {
        other.until_sec <= self.since_sec
    }

    #[allow(dead_code)]
    fn is_valid(&self) -> bool {
        self.since_sec <= self.until_sec
    }
    fn from_splited_timestamps(
        since_secs: Vec<TimestampSec>,
        until_secs: Vec<TimestampSec>,
    ) -> Vec<BlockTimestamp> {
        debug_assert_eq!(since_secs.len(), until_secs.len());
        let timestamp_pairs: Vec<(TimestampSec, TimestampSec)> =
            since_secs.into_iter().zip(until_secs.into_iter()).collect();
        Self::from_timestamp_pairs(timestamp_pairs)
    }

    fn from_timestamp_pairs(timestamps: Vec<(TimestampSec, TimestampSec)>) -> Vec<BlockTimestamp> {
        timestamps
            .into_iter()
            .map(|(since_sec, until_sec)| BlockTimestamp {
                since_sec,
                until_sec,
            })
            .collect()
    }

    #[allow(dead_code)]
    fn insert(block_timestamps: &mut Vec<BlockTimestamp>, new_block: BlockTimestamp) -> Result<()> {
        let insert_idx = match binary_search_by(
            block_timestamps.as_slice(),
            |block_timestamp| block_timestamp.since_sec.cmp(&new_block.since_sec),
            BinaryRangeSearchType::AtMost,
        ) {
            Some(idx) => idx + 1,
            None => block_timestamps.len(),
        };
        if insert_idx >= block_timestamps.len() {
            block_timestamps.push(new_block);
        } else {
            block_timestamps.insert(insert_idx, new_block);
        }

        Ok(())
    }
}

pub(crate) fn write_to_blocklist<W>(mut block_list_file: W, block_list: BlockList) -> Result<()>
where
    W: Write,
{
    let block_timestamp_size = block_list.block_timestamp_num();
    if block_timestamp_size == 0 {
        return Err(BlockListError::EmptyBlockTimestampNano);
    }
    #[cfg(feature = "validate")]
    block_list.check_block_timestamp_is_sorted()?;

    //  (1) updated timestamp(8 byte)
    {
        let mut bits_writer = BitsWriter::new();
        bits_writer.append(u64_bits_reader!(*block_list.updated_timestamp_sec, 64)?, 64)?;
        bits_writer.flush(&mut block_list_file)?;
    }

    //  (2) number of block timestamps (n bytes)
    base_128_variants::compress_u64(block_timestamp_size as u64, &mut block_list_file)?;

    let (sinces, untils) = block_list.split_block_list_timestamps();

    //  (3) timestamp second head (since)(v byte)
    //  (4) timestamp second deltas(since)(v byte)
    write_timestamp_sec_and_deltas(sinces, &mut block_list_file)?;

    //  (5) timestamp second head (untile)(v byte)
    //  (6) timestamp second (until)(v byte)
    write_timestamp_sec_and_deltas(untils, &mut block_list_file)?;

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

pub(crate) fn read_from_blocklist_file<P: AsRef<Path>>(path: P) -> Result<BlockList> {
    let block_list_file = File::open(path)?;
    let block_list_data = unsafe { MmapOptions::new().map(&block_list_file)? };
    read_from_blocklist(&block_list_data)
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

pub(crate) fn read_from_blocklist(block_data: &[u8]) -> Result<BlockList> {
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
    let (until_timedeltas, _block_idx) = read_timestamp_sec_and_deltas(
        block_data,
        number_of_block_timstamps_deltas as usize,
        block_idx,
    )?;

    let block_timestamps = BlockTimestamp::from_splited_timestamps(
        since_timedeltas.as_timestamp_secs(),
        until_timedeltas.as_timestamp_secs(),
    );

    let block_list = BlockList {
        updated_timestamp_sec,
        block_timestamps,
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

    #[test]
    fn test_wr_block_list_1() {
        let mut dest = Vec::<u8>::new();

        let block_list = {
            let ts1 =
                BlockTimestamp::new(TimestampSec::new(1629745452), TimestampSec::new(1629745453));

            let ts2 =
                BlockTimestamp::new(TimestampSec::new(1629745454), TimestampSec::new(1629745455));

            let updated_timestamp = TimestampNano::new(1629745452_715062000);
            BlockList::new(updated_timestamp, vec![ts1, ts2])
        };

        let result = write_to_blocklist(&mut dest, block_list.clone());
        assert!(result.is_ok());

        let result = read_from_blocklist(&mut dest);
        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(result, block_list);
    }

    macro_rules! block_timestamps {
        ($({$since:expr,$until:expr}),*) => {
            vec![
                $(blts!($since,$until) ),*
            ]
        };
    }

    macro_rules! blts {
        ($since:expr,$until:expr) => {
            BlockTimestamp::new(TimestampSec::new($since), TimestampSec::new($until))
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
        let result = BlockTimestamp::insert(&mut block_timestamps, blts!(10, 15));
        assert!(result.is_ok());
        assert_eq!(
            block_timestamps,
            block_timestamps!({10,20},{10,20}, {10,15},{21,30})
        );
    }

    #[test]
    fn test_block_timestamps_insert_2() {
        let mut block_timestamps = block_timestamps!({10,20},{10,20}, {21,30});
        let result = BlockTimestamp::insert(&mut block_timestamps, blts!(22, 50));
        assert!(result.is_ok());
        assert_eq!(
            block_timestamps,
            block_timestamps!({10,20},{10,20}, {21,30},{22,50})
        );
    }

    #[test]
    fn test_block_timestamps_search_1() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(Some(&ts!(11)), Some(&ts!(15)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_timestamps!({11,30}, {11,30}, {12,30}, {15,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_2() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(Some(&ts!(10)), Some(&ts!(15)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_3() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(Some(&ts!(10)), Some(&ts!(22)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_4() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(Some(&ts!(10)), Some(&ts!(22)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_5() {
        let block_timestamps = block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30},{21,31});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(Some(&ts!(10)), Some(&ts!(21)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30},{21,31})
        );
    }

    #[test]
    fn test_block_timestamps_search_6() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(None, Some(&ts!(13)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30})
        );
    }

    #[test]
    fn test_block_timestamps_search_7() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(Some(&ts!(13)), None);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), block_timestamps!({15,30},{21,30}));
    }

    #[test]
    fn test_block_timestamps_search_8() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30},{21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(Some(&ts!(22)), None);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_block_timestamps_search_9() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30}, {21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(None, Some(&ts!(9)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_block_timestamps_search_10() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30}, {21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(Some(&ts!(4)), Some(&ts!(9)));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_block_timestamps_search_11() {
        let block_timestamps =
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30}, {21,30});

        let block_list = BlockList {
            updated_timestamp_sec: TimestampNano::new(0),
            block_timestamps,
        };

        let result = block_list.search(None, None);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            block_timestamps!({10,20},{10,20}, {10,20},{11,30}, {11,30}, {12,30}, {15,30}, {21,30})
        );
    }
}
