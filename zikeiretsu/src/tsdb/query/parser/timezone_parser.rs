use super::*;

use chrono::{FixedOffset, TimeZone};
use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

pub fn parse_timezone_offset<'q>(pair: Pair<'q, Rule>) -> Result<FixedOffset> {
    if pair.as_rule() != Rule::TIMEZONE_OFFSET_VAL {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::TIMEZONE_OFFSET_VAL),
            format!("{:?}", pair.as_rule()),
        ));
    }
    let offset_sec = timeoffset_sec_from_str(pair.as_str())?;
    Ok(FixedOffset::east(offset_sec))
}

pub(crate) fn timeoffset_sec_from_str(mut offfset_str: &str) -> Result<i32> {
    let secs = 0;
    if -86_400 < secs && secs < 86_400 {
        return Err(TimeOffsetOutOfBound(secs));
    }
    unimplemented!()
}
