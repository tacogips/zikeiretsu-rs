use crate::tsdb::query::parser::*;

use super::{is_space, pos_neg_parser};
use chrono::{FixedOffset, TimeZone};
use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use std::ops::Deref;
use thiserror::Error;

pub struct DeltaInMicroSeconds(i64);
impl Deref for DeltaInMicroSeconds {
    type Target = i64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn parse_duration_delta<'q>(pair: Pair<'q, Rule>) -> Result<DeltaInMicroSeconds> {
    if pair.as_rule() != Rule::DURATION_DELTA {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::DURATION_DELTA),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut is_nagative = false;

    let mut pos_neg: Option<pos_neg_parser::PosNeg> = None;
    let mut duration_num: Option<u64> = None;
    let mut duration_unit: Option<DurationUnit> = None;

    for each_delta_elem in pair.into_inner() {
        match each_delta_elem.as_rule() {
            Rule::POS_NEG => pos_neg = Some(pos_neg_parser::parse_pos_neg(each_delta_elem)?),
            Rule::ASCII_DIGITS => pos_neg = Some(pos_neg_parser::parse_pos_neg(each_delta_elem)?),
            Rule::DURATION_UNIT => duration_unit = Some(parse_duration(each_delta_elem)?),

            r => {
                return Err(QueryError::InvalidGrammer(format!(
                    "unknown term in build in datetime delta : {r:?}"
                )));
            }
        }
    }

    match (pos_neg, duration_num, duration_unit) {
        (Some(pos_neg), Some(duration_num), Some(duration_unit)) => {
            let sign = if pos_neg.is_nagative() { -1 } else { 1 };

            let micro_sec = duration_unit.convert_in_micro_sec(duration_num as i64 * sign);
            Ok(DeltaInMicroSeconds(micro_sec))
        }
        (pos_neg, duration_num, duration_unit) => Err(QueryError::InvalidGrammer(format!(
            "invalid duration: {pos_neg:?}, {duration_num:?}, {duration_unit:?}"
        ))),
    }
}

#[derive(Debug)]
pub enum DurationUnit {
    MicroSecond,
    Millisecond,
    Second,
    Minutes,
    Hour,
    Day,
    Month,
    Year,
}

impl DurationUnit {
    fn convert_in_micro_sec(&self, v: i64) -> i64 {
        0
    }
}

pub fn parse_duration<'q>(pair: Pair<'q, Rule>) -> Result<DurationUnit> {
    if pair.as_rule() != Rule::DURATION_UNIT {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::DURATION_UNIT),
            format!("{:?}", pair.as_rule()),
        ));
    }
    let duration_unit = pair.into_inner().next();
    match duration_unit {
        None => {
            return Err(QueryError::InvalidGrammer(format!(
                "invalid empty duration unit "
            )))
        }
        Some(duration_unit) => match duration_unit.as_rule() {
            Rule::KW_MICROSECOND => Ok(DurationUnit::MicroSecond),
            Rule::KW_MILLISECOND => Ok(DurationUnit::Millisecond),
            Rule::KW_SECOND => Ok(DurationUnit::Second),
            Rule::KW_MINUTES => Ok(DurationUnit::Minutes),
            Rule::KW_HOUR => Ok(DurationUnit::Hour),
            Rule::KW_DAY => Ok(DurationUnit::Day),
            Rule::KW_MONTH => Ok(DurationUnit::Month),
            Rule::KW_YEAR => Ok(DurationUnit::Year),
            r => {
                return Err(QueryError::InvalidGrammer(format!(
                    "invalid duration unit: {r:?}"
                )));
            }
        },
    }
}

//// [+|-]01:00 => 0 as i32
//fn duration_delta_micro_sec_from_str(clock_delta_str: &str) -> Result<i64> {
//    let mut parsing_offset: &[u8] = &clock_delta_str.as_bytes();
//    let is_nagative = match parsing_offset.first() {
//        Some(b'+') => false,
//        Some(b'-') => true,
//        _ => return Err(QueryError::InvalidTimeOffset(clock_delta_str.to_string())),
//    };
//
//    let mut white_space_count = 0;
//    for i in 1..parsing_offset.len() {
//        if !is_space(parsing_offset[i]) {
//            break;
//        }
//        white_space_count += 1;
//    }
//
//    let sec = time_sec_from_clock_str(&clock_delta_str[white_space_count + 1..])?;
//
//    if is_nagative {
//        Ok(sec * -1)
//    } else {
//        Ok(sec)
//    }
//}
//
//// 00:00 => 0 as i32
//pub(crate) fn time_sec_from_clock_str(clock_str: &str) -> Result<i32> {
//    let mut parsing_offset: &[u8] = &clock_str.as_bytes();
//    //parse hours
//    let hour_num = if parsing_offset.is_empty() {
//        return Err(QueryError::InvalidTimeOffset(clock_str.to_string()));
//    } else if parsing_offset.len() > 2 {
//        match (parsing_offset[0], parsing_offset[1]) {
//            (hour_10 @ b'0'..=b'9', hour_1 @ b'0'..=b'9') => {
//                parsing_offset = &parsing_offset[2..];
//                i32::from(hour_10 - b'0') * 10 + i32::from(hour_1 - b'0')
//            }
//
//            (hour @ b'0'..=b'9', b':') => {
//                parsing_offset = &parsing_offset[1..];
//                i32::from(hour - b'0')
//            }
//            _ => return Err(QueryError::InvalidTimeOffset(clock_str.to_string())),
//        }
//    } else {
//        match parsing_offset[0] {
//            hour @ b'0'..=b'9' => {
//                parsing_offset = &parsing_offset[1..];
//                i32::from(hour - b'0')
//            }
//            _ => return Err(QueryError::InvalidTimeOffset(clock_str.to_string())),
//        }
//    };
//
//    //parse minutes
//    let minute_num = if let Some(&b':') = &parsing_offset.first() {
//        parsing_offset = &parsing_offset[1..];
//        if parsing_offset.len() < 2 {
//            return Err(QueryError::InvalidTimeOffset(clock_str.to_string()));
//        }
//        match (parsing_offset[0], parsing_offset[1]) {
//            (minute_10 @ b'0'..=b'5', minute_1 @ b'0'..=b'9') => {
//                parsing_offset = &parsing_offset[2..];
//                i32::from(minute_10 - b'0') * 10 + i32::from(minute_1 - b'0')
//            }
//            _ => return Err(QueryError::InvalidTimeOffset(clock_str.to_string())),
//        }
//    } else {
//        0
//    };
//
//    //parse minutes
//    let sec_num = if let Some(&b':') = &parsing_offset.first() {
//        parsing_offset = &parsing_offset[1..];
//
//        if parsing_offset.len() < 2 {
//            return Err(QueryError::InvalidTimeOffset(clock_str.to_string()));
//        }
//        match (parsing_offset[0], parsing_offset[1]) {
//            (secs_10 @ b'0'..=b'5', secs_1 @ b'0'..=b'9') => {
//                parsing_offset = &parsing_offset[2..];
//                i32::from(secs_10 - b'0') * 10 + i32::from(secs_1 - b'0')
//            }
//            _ => return Err(QueryError::InvalidTimeOffset(clock_str.to_string())),
//        }
//    } else {
//        0
//    };
//    if !parsing_offset.is_empty() {
//        return Err(QueryError::InvalidTimeOffset(clock_str.to_string()));
//    }
//
//    Ok(hour_num * 3600 + minute_num * 60 + sec_num)
//}
//
//#[cfg(test)]
//mod test {
//
//    use super::*;
//    #[test]
//    fn parse_clock_delta_sec_from_str_1() {
//        let result = clock_delta_sec_from_str("+1");
//        assert!(result.is_ok());
//        assert_eq!(result.unwrap(), 1 * 3600);
//
//        let result = clock_delta_sec_from_str("-1");
//        assert!(result.is_ok());
//        assert_eq!(result.unwrap(), -1 * 3600);
//
//        let result = clock_delta_sec_from_str("1");
//        assert!(result.is_err());
//    }
//
//    #[test]
//    fn parse_clock_delta_sec_from_str_2() {
//        let result = clock_delta_sec_from_str("+2:00");
//        assert!(result.is_ok());
//        assert_eq!(result.unwrap(), 2 * 3600);
//
//        let result = clock_delta_sec_from_str("+  12:00");
//        assert!(result.is_ok());
//        assert_eq!(result.unwrap(), 12 * 3600);
//
//        let result = clock_delta_sec_from_str("+2:23");
//        assert!(result.is_ok());
//        assert_eq!(result.unwrap(), 2 * 3600 + 23 * 60);
//
//        let result = clock_delta_sec_from_str("+02:00");
//        assert!(result.is_ok());
//        assert_eq!(result.unwrap(), 2 * 3600);
//
//        let result = clock_delta_sec_from_str("+02:23");
//        assert!(result.is_ok());
//        assert_eq!(result.unwrap(), 2 * 3600 + 23 * 60);
//
//        let result = clock_delta_sec_from_str("-        2:00");
//        assert!(result.is_ok());
//        assert_eq!(result.unwrap(), -2 * 3600);
//
//        let result = clock_delta_sec_from_str("+2:00z");
//        assert!(result.is_err());
//    }
//}
