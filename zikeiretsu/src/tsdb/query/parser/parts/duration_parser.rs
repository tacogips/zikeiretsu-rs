use crate::tsdb::query::parser::*;

use super::{ascii_digits_parser, pos_neg_parser};
use pest::iterators::Pair;
use std::ops::Deref;

pub struct DeltaInMicroSeconds(i64);
impl Deref for DeltaInMicroSeconds {
    type Target = i64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn parse_duration_delta(pair: Pair<'_, Rule>) -> Result<DeltaInMicroSeconds> {
    if pair.as_rule() != Rule::DURATION_DELTA {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::DURATION_DELTA),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut pos_neg: Option<pos_neg_parser::PosNeg> = None;
    let mut duration_num: Option<u64> = None;
    let mut duration_unit: Option<DurationUnit> = None;

    for each_delta_elem in pair.into_inner() {
        match each_delta_elem.as_rule() {
            Rule::POS_NEG => pos_neg = Some(pos_neg_parser::parse_pos_neg(each_delta_elem)?),
            Rule::ASCII_DIGITS => {
                duration_num = Some(ascii_digits_parser::parse_ascii_digits(each_delta_elem)?)
            }
            Rule::DURATION_UNIT => duration_unit = Some(parse_duration(each_delta_elem)?),

            r => {
                return Err(ParserError::InvalidGrammer(format!(
                    "unknown term in build in datetime delta : {r:?}"
                )));
            }
        }
    }

    match (pos_neg, duration_num, duration_unit) {
        (pos_neg, Some(duration_num), Some(duration_unit)) => {
            let sign = match pos_neg {
                None => 1,
                Some(pos_neg) => {
                    if pos_neg.is_nagative() {
                        -1
                    } else {
                        1
                    }
                }
            };

            let micro_sec = duration_unit.convert_in_micro_sec(duration_num as i64 * sign);
            Ok(DeltaInMicroSeconds(micro_sec))
        }
        (pos_neg, duration_num, duration_unit) => Err(ParserError::InvalidGrammer(format!(
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
}

impl DurationUnit {
    fn convert_in_micro_sec(&self, v: i64) -> i64 {
        match self {
            Self::MicroSecond => v,
            Self::Millisecond => v * 1000,
            Self::Second => v * 1_000_000,
            Self::Minutes => v * 1_000_000 * 60,
            Self::Hour => v * 1_000_000 * 60 * 60,
            Self::Day => v * 1_000_000 * 60 * 60 * 24,
        }
    }
}

pub fn parse_duration(pair: Pair<'_, Rule>) -> Result<DurationUnit> {
    if pair.as_rule() != Rule::DURATION_UNIT {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::DURATION_UNIT),
            format!("{:?}", pair.as_rule()),
        ));
    }
    let duration_unit = pair.into_inner().next();
    match duration_unit {
        None => Err(ParserError::InvalidGrammer(
            "invalid empty duration unit ".to_string(),
        )),
        Some(duration_unit) => match duration_unit.as_rule() {
            Rule::KW_MICROSECOND => Ok(DurationUnit::MicroSecond),
            Rule::KW_MILLISECOND => Ok(DurationUnit::Millisecond),
            Rule::KW_SECOND => Ok(DurationUnit::Second),
            Rule::KW_MINUTES => Ok(DurationUnit::Minutes),
            Rule::KW_HOUR => Ok(DurationUnit::Hour),
            Rule::KW_DAY => Ok(DurationUnit::Day),
            r => {
                return Err(ParserError::InvalidGrammer(format!(
                    "invalid duration unit: {r:?}"
                )));
            }
        },
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn parse_duration_delta_1() {
        let dt_delta = r"+ 2 hours";

        let pairs = QueryGrammer::parse(Rule::DURATION_DELTA, dt_delta);

        assert!(pairs.is_ok());
    }

    #[test]
    fn parse_duration_delta_2() {
        let dt_delta = r"2 hours";
        let pairs = QueryGrammer::parse(Rule::DURATION_DELTA, dt_delta);
        assert!(pairs.is_ok());
    }

    #[test]
    fn parse_duration_delta_3() {
        let dt_delta = r"-2 hours";
        let pairs = QueryGrammer::parse(Rule::DURATION_DELTA, dt_delta);
        assert!(pairs.is_ok());
    }

    #[test]
    fn convert_duration_to_micro_secs() {
        let v = 2;

        let d1 = DurationUnit::MicroSecond;
        assert_eq!(2, d1.convert_in_micro_sec(v));
        let d2 = DurationUnit::Millisecond;
        assert_eq!(
            d1.convert_in_micro_sec(v) * 1000,
            d2.convert_in_micro_sec(v)
        );
        let d3 = DurationUnit::Second;
        assert_eq!(
            d2.convert_in_micro_sec(v) * 1000,
            d3.convert_in_micro_sec(v)
        );
        let d4 = DurationUnit::Minutes;
        assert_eq!(d3.convert_in_micro_sec(v) * 60, d4.convert_in_micro_sec(v));

        let d5 = DurationUnit::Hour;
        assert_eq!(d4.convert_in_micro_sec(v) * 60, d5.convert_in_micro_sec(v));
        let d6 = DurationUnit::Day;
        assert_eq!(d5.convert_in_micro_sec(v) * 24, d6.convert_in_micro_sec(v));
    }

    //pub enum DurationUnit {
    //    MicroSecond,
    //    Millisecond,
    //    Second,
    //    Minutes,
    //    Hour,
    //    Day,
    //    Month,
    //    Year,
    //}
    //
    //impl DurationUnit {
    //    fn convert_in_micro_sec(&self, v: i64) -> i64 {
    //        match self {
    //            Self::MicroSecond => v,
    //            Self::Millisecond => v * 1000,
    //            Self::Second => v * 1_000_000,
    //            Self::Minutes => v * 1_000_000 * 60,
    //            Self::Hour => v * 1_000_000 * 60 * 60,
    //            Self::Day => v * 1_000_000 * 60 * 60 * 24,
    //            Self::Month => v * 1_000_000 * 60 * 60 * 24 * 30,
    //            Self::Year => v * 1_000_000 * 60 * 60 * 24 * 365,
    //        }
    //    }
    //}
    //
    //
}
