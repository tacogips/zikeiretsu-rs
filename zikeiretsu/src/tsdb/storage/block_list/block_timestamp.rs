use crate::tsdb::timestamp_sec::*;
use searcher::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Iterator;

#[derive(Copy, Debug, PartialEq, Clone, Serialize, Deserialize, Eq, Hash)]
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

    pub fn is_adjacent_before_of(&self, other: &Self) -> bool {
        self.until_sec == other.since_sec
    }

    //TODO(tacogipa)  implment  PartialOrd
    pub fn is_before(&self, other: &Self) -> bool {
        self.until_sec <= other.since_sec
    }

    //TODO(tacogipa)  implment  PartialOrd
    #[allow(dead_code)]
    pub fn is_after(&self, other: &Self) -> bool {
        other.until_sec <= self.since_sec
    }

    #[allow(dead_code)]
    fn is_valid(&self) -> bool {
        self.since_sec <= self.until_sec
    }

    pub fn merge(&mut self, other: &Self) {
        if other.since_sec < self.since_sec {
            self.since_sec = other.since_sec
        }

        if self.until_sec < other.until_sec {
            self.until_sec = other.until_sec
        }
    }
    pub fn from_timestamp_pairs(
        timestamps: Vec<(TimestampSec, TimestampSec)>,
    ) -> Vec<BlockTimestamp> {
        timestamps
            .into_iter()
            .map(|(since_sec, until_sec)| BlockTimestamp {
                since_sec,
                until_sec,
            })
            .collect()
    }

    #[allow(dead_code)]
    pub fn insert(block_timestamps: &mut Vec<BlockTimestamp>, new_block: BlockTimestamp) {
        let insert_idx = match binary_search_by(
            block_timestamps.as_slice(),
            |block_timestamp| block_timestamp.since_sec.cmp(&new_block.since_sec),
            BinaryRangeSearchType::AtMostInclusive,
        ) {
            Some(idx) => idx + 1,
            None => block_timestamps.len(),
        };
        if insert_idx >= block_timestamps.len() {
            block_timestamps.push(new_block);
        } else {
            block_timestamps.insert(insert_idx, new_block);
        }
    }
}

impl fmt::Display for BlockTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.since_sec, self.until_sec)
    }
}
