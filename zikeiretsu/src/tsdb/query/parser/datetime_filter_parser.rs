use once_cell::sync::OnceCell;
use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use std::collections::HashSet;
use thiserror::Error;

use super::*;
use chrono::{format as chrono_format, DateTime, FixedOffset, TimeZone};

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeFilter<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME_FILTER {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::DATETIME_FILTER),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut filter_val1: Option<DatetimeFilterValue> = None;
    let mut filter_val2: Option<DatetimeFilterValue> = None;

    let mut relation_op: Option<Pair<'q, Rule>> = None;

    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::REL_OP => {
                let mut rel_ope = each.into_inner();
                match rel_ope.next() {
                    Some(rel_ope) => relation_op = Some(rel_ope),
                    None => {
                        return Err(QueryError::InvalidGrammer(format!(
                            "empty relation operator in datetime filter"
                        )))
                    }
                }
            }
            Rule::DATETIME => {}
            Rule::DATETIME_RANGE => {}
            Rule::KW_TIMESTAMP => {}
            r @ _ => {
                return Err(QueryError::InvalidGrammer(format!(
                    "unknown term in datetime filter : {r:?}"
                )))
            }
        }
    }

    unimplemented!()
    //let mut columns = Vec::<Column<'q>>::new();
    //for each_pair_in_columns in pair.into_inner() {
    //    if each_pair_in_columns.as_rule() == Rule::COLUMN_NAME {
    //        let column_str = each_pair_in_columns.as_str();
    //        if column_str == "*" {
    //            if allow_asterisk {
    //                columns.push(Column::Asterick)
    //            } else {
    //                return Err(QueryError::InvalidColumnName(column_str.to_string()));
    //            }
    //        } else {
    //            columns.push(Column::ColumnName(ColumnName(
    //                each_pair_in_columns.as_str(),
    //            )))
    //        }
    //    }
    //}
    //Ok(columns)
}

pub fn parse_datetime_range_close<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeFilterValue> {
    unimplemented!()
}

pub fn parse_datetime<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeFilterValue> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::DATETIME),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut datetime_str: Option<&'q str> = None;
    let mut datetime_fn: Option<BuildinDatetimeFunction> = None;
    let mut offset: Option<FixedOffset> = None;

    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::DATETIME_FN => {
                for date_time_fn in each.into_inner() {
                    match date_time_fn.as_rule() {
                        Rule::FN_TODAY => datetime_fn = Some(BuildinDatetimeFunction::Today),
                        Rule::FN_YESTERDAY => {
                            datetime_fn = Some(BuildinDatetimeFunction::Yesterday)
                        }
                        Rule::FN_TOMORROW => datetime_fn = Some(BuildinDatetimeFunction::Tomorrow),
                        r @ _ => {
                            return Err(QueryError::InvalidGrammer(format!(
                                "unknown term in build in datetime  : {r:?}"
                            )));
                        }
                    }
                }
            }
            Rule::DATETIME_DELTA => {}

            r @ _ => {
                return Err(QueryError::InvalidGrammer(format!(
                    "unknown term in datetime : {r:?}"
                )))
            }
        }
    }

    unimplemented!()
}

static DATETIME_FORMATS: OnceCell<Vec<chrono_format::StrftimeItems<'static>>> = OnceCell::new();

pub(crate) fn datetime_formats() -> &'static [chrono_format::StrftimeItems<'static>] {
    DATETIME_FORMATS.get_or_init(|| vec![]).as_slice()
}

/// availabe formats
/// 'yyyy-MM-DD hh:mm:ss.ZZZZZZ'
/// 'yyyy-MM-DD hh:mm:ss'
/// 'yyyy-MM-DD hh:mm'
/// 'yyyy-MM-DD hh'
/// 'yyyy-MM-DD'
///
fn parse_datetime_str(datetime_str: &str) -> Result<DateTime<FixedOffset>> {
    //DateTime::<FixedOffset>::parse_from_str()
    //    let mut parsed = chrono_format::Parsed::new();

    for each_format in datetime_formats() {
        let mut parsed = chrono_format::Parsed::new();
        if let Ok(_) = chrono_format::parse(&mut parsed, datetime_str, each_format.clone()) {
            return Ok(parsed.to_datetime()?);
        }
    }

    unimplemented!()
}

fn parse_datetime_delta<'q>(pair: Pair<'q, Rule>) -> Result<FixedOffset> {
    unimplemented!()
}

fn parse_duraion_delta(datetime_delta_str: &str) -> Result<FixedOffset> {
    unimplemented!()
}

fn parse_clock_delta(datetime_delta_str: &str) -> Result<FixedOffset> {
    unimplemented!()
}
