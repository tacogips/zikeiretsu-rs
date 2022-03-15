use crate::tsdb::query::parser::*;

use super::is_space;

use chrono::{FixedOffset, TimeZone};
use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

pub fn parse_ascii_digits<'q>(pair: Pair<'q, Rule>) -> Result<u64> {
    if pair.as_rule() != Rule::ASCII_DIGITS {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::ASCII_DIGITS),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut is_nagative = false;
    let mut pos_neg: Option<pos_neg_parser::PosNeg> = None;
    let mut duration_num: Option<i64> = None;
    let mut duration_unit: Option<i64> = None;

    //DURATION_DELTA     = { POS_NEG ~ ASCII_DIGITS ~ DURATION  }

    for each_delta_elem in pair.into_inner() {
        match each_delta_elem.as_rule() {
            Rule::POS_NEG => pos_neg = Some(pos_neg_parser::parse_pos_neg(each_delta_elem)?),
            Rule::ASCII_DIGITS => pos_neg = Some(pos_neg_parser::parse_pos_neg(each_delta_elem)?),
            Rule::DURATION => duration_unit = Some(parse_duration(each_delta_elem)?),

            r => {
                return Err(QueryError::InvalidGrammer(format!(
                    "unknown term in build in datetime delta : {r:?}"
                )));
            }
        }
    }

    //let offset_sec = clock_delta_sec_from_str(pair.as_str())?;
    Ok(FixedOffset::east(offset_sec))
}
