use crate::tsdb::query::parser::*;

use super::is_space;
use chrono::{FixedOffset, TimeZone};
use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use std::convert::TryFrom;
use thiserror::Error;

pub fn parse_ascii_digits<'q>(pair: Pair<'q, Rule>) -> Result<u64> {
    if pair.as_rule() != Rule::ASCII_DIGITS {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::ASCII_DIGITS),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let val = pair.as_str().parse::<u64>()?;
    Ok(val)
}
