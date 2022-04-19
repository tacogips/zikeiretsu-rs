use super::{clock_parser, duration_parser};
use pest::iterators::Pair;

use crate::tsdb::datetime::{parse_datetime_str, today, tomorrow, yesterday, TimestampNano};
use crate::tsdb::query::parser::*;
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, PartialEq)]
pub enum DatetimeFilter<'q> {
    In(ColumnName<'q>, DatetimeFilterValue, DatetimeFilterValue),
    Gte(ColumnName<'q>, DatetimeFilterValue),
    Gt(ColumnName<'q>, DatetimeFilterValue),
    Lte(ColumnName<'q>, DatetimeFilterValue),
    Lt(ColumnName<'q>, DatetimeFilterValue),
    Equal(ColumnName<'q>, DatetimeFilterValue),
}

impl<'q> DatetimeFilter<'q> {
    pub fn from(
        column_name: ColumnName<'q>,
        ope: &'q str,
        datetime_1: DatetimeFilterValue,
        datetime_2: Option<DatetimeFilterValue>,
    ) -> Result<DatetimeFilter<'q>> {
        match ope.to_uppercase().as_str() {
            "IN" => match datetime_2 {
                None => {
                    if let DatetimeFilterValue::Function(build_in_func, tz) = datetime_1 {
                        Ok(DatetimeFilter::Equal(
                            column_name,
                            DatetimeFilterValue::Function(build_in_func, tz),
                        ))
                    } else {
                        Err(ParserError::InvalidGrammer(format!(
                            "'in' needs datetime range or buildin function "
                        )))
                    }
                }
                Some(datetime_2) => Ok(DatetimeFilter::In(column_name, datetime_1, datetime_2)),
            },
            ">=" => Ok(DatetimeFilter::Gte(column_name, datetime_1)),
            ">" => Ok(DatetimeFilter::Gt(column_name, datetime_1)),
            "<=" => Ok(DatetimeFilter::Lte(column_name, datetime_1)),
            "<" => Ok(DatetimeFilter::Lt(column_name, datetime_1)),
            "=" => Ok(DatetimeFilter::Equal(column_name, datetime_1)),
            invalid_operator => Err(ParserError::InvalidDatetimeFilterOperator(
                invalid_operator.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DatetimeDelta {
    FixedOffset(FixedOffset),
    MicroSec(i64),
    Composit(Box<DatetimeDelta>, Box<DatetimeDelta>),
}
impl DatetimeDelta {
    pub fn to_composit_if_some(self, other: Option<Self>) -> Self {
        match other {
            Some(other) => Self::Composit(Box::new(other), Box::new(self)),
            None => self,
        }
    }

    pub fn as_micro_second(&self) -> i64 {
        match self {
            DatetimeDelta::FixedOffset(fixed_offset) => {
                fixed_offset.local_minus_utc() as i64 * 1_000_000i64
            }
            DatetimeDelta::MicroSec(delta) => *delta,
            DatetimeDelta::Composit(delta_1, delta_2) => {
                delta_1.as_micro_second() + delta_2.as_micro_second()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DatetimeFilterValue {
    DateString(DateTime<Utc>, Option<DatetimeDelta>),
    Function(BuildinDatetimeFunction, Option<DatetimeDelta>),
}

impl DatetimeFilterValue {
    pub fn to_timestamp_nano(&self, offset: &FixedOffset) -> TimestampNano {
        match self {
            Self::DateString(datetime, delta) => {
                let mut naive_datetime = datetime.naive_utc();
                let delta_micro_seconds = Duration::microseconds(
                    delta
                        .as_ref()
                        .map(|delta| delta.as_micro_second())
                        .unwrap_or(0),
                );
                naive_datetime = naive_datetime + delta_micro_seconds;

                let datetime = offset.from_local_datetime(&naive_datetime).unwrap();
                TimestampNano::new(datetime.timestamp_nanos() as u64)
            }

            Self::Function(build_func, delta) => {
                let micro_sec_delta = delta
                    .as_ref()
                    .map(|delta| delta.as_micro_second())
                    .unwrap_or(0);

                let timestamp_nano: TimestampNano = match build_func {
                    BuildinDatetimeFunction::Today => today(*offset).into(),
                    BuildinDatetimeFunction::Yesterday => yesterday(*offset).into(),
                    BuildinDatetimeFunction::Tomorrow => tomorrow(*offset).into(),
                };

                timestamp_nano + Duration::microseconds(micro_sec_delta)
            }
        }
    }
}

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeFilter<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME_FILTER {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::DATETIME_FILTER),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut column_name: Option<ColumnName<'q>> = None;
    let mut filter_val1: Option<DatetimeFilterValue> = None;
    let mut filter_val2: Option<DatetimeFilterValue> = None;
    let mut relation_op: Option<&'q str> = None;

    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::REL_OP => {
                let mut rel_ope = each.into_inner();
                match rel_ope.next() {
                    Some(rel_ope) => relation_op = Some(rel_ope.as_str().trim()),
                    None => {
                        return Err(ParserError::InvalidGrammer(format!(
                            "empty relation operator in datetime filter"
                        )))
                    }
                }
            }
            Rule::DATETIME => {
                filter_val1 = Some(parse_datetime(each)?);
            }
            Rule::DATETIME_RANGE => {
                for date_time_range in each.into_inner() {
                    match date_time_range.as_rule() {
                        Rule::DATETIME => {
                            filter_val1 = Some(parse_datetime(date_time_range)?);
                        }
                        Rule::DATETIME_RANGE_CLOSE => {
                            filter_val2 = Some(parse_datetime_range_close(
                                date_time_range,
                                filter_val1.as_ref(),
                            )?);
                        }
                        _ => { /* do nothing */ }
                    }
                }
            }
            Rule::KW_TIMESTAMP => column_name = Some(ColumnName(each.as_str())),
            r @ _ => {
                return Err(ParserError::InvalidGrammer(format!(
                    "unknown term in datetime filter : {r:?}"
                )))
            }
        }
    }

    match (column_name, relation_op, filter_val1) {
        (Some(column_name), Some(relation_op), Some(filter_val1)) => {
            DatetimeFilter::from(column_name, relation_op, filter_val1, filter_val2)
        }
        (column_name, relation_op, filter_val1) => {
            Err(ParserError::InvalidGrammer(format!(
            "unknown term in datetime filter.  column:{column_name:?}, ope:{relation_op:?}, val1: {filter_val1:?}"
        )))
        }
    }
}

pub fn parse_datetime_range_close<'q>(
    pair: Pair<'q, Rule>,
    base_datetime: Option<&DatetimeFilterValue>,
) -> Result<DatetimeFilterValue> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME_RANGE_CLOSE {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::DATETIME_RANGE_CLOSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut datetime: Option<DatetimeFilterValue> = None;

    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::DATETIME => {
                datetime = Some(parse_datetime(each)?);
            }
            Rule::DATETIME_DELTA => {
                let datetime_delta = parse_datetime_delta(each)?;
                if let Some(base_datetime) = base_datetime {
                    let calced_datetime = match base_datetime {
                        DatetimeFilterValue::DateString(dt, base_delta) => {
                            DatetimeFilterValue::DateString(
                                dt.clone(),
                                Some(datetime_delta.to_composit_if_some(base_delta.clone())),
                            )
                        }
                        DatetimeFilterValue::Function(func, base_delta) => {
                            DatetimeFilterValue::Function(
                                func.clone(),
                                Some(datetime_delta.to_composit_if_some(base_delta.clone())),
                            )
                        }
                    };

                    datetime = Some(calced_datetime)
                } else {
                    return Err(ParserError::InvalidGrammer(format!(
                        " datetime filter val1  is needed. "
                    )));
                }
            }
            r @ _ => {
                return Err(ParserError::InvalidGrammer(format!(
                    "unknown term in datetime filter : {r:?}"
                )))
            }
        }
    }

    match datetime {
        None => Err(ParserError::InvalidGrammer(format!(
            "invalid datetime filter close. "
        ))),
        Some(datetime) => Ok(datetime),
    }
}

pub fn parse_datetime<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeFilterValue> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::DATETIME),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut datetime: Option<DateTime<Utc>> = None;
    let mut datetime_fn: Option<BuildinDatetimeFunction> = None;
    let mut datetime_delta: Option<DatetimeDelta> = None;

    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::DATETIME_STR => {
                let parse_datetime = parse_datetime_str(each.as_str())?;
                datetime = Some(parse_datetime);
            }

            Rule::DATETIME_FN => {
                for date_time_fn in each.into_inner() {
                    match date_time_fn.as_rule() {
                        Rule::FN_TODAY => datetime_fn = Some(BuildinDatetimeFunction::Today),
                        Rule::FN_YESTERDAY => {
                            datetime_fn = Some(BuildinDatetimeFunction::Yesterday)
                        }
                        Rule::FN_TOMORROW => datetime_fn = Some(BuildinDatetimeFunction::Tomorrow),
                        r => {
                            return Err(ParserError::InvalidGrammer(format!(
                                "unknown term in build in datetime  : {r:?}"
                            )));
                        }
                    }
                }
            }

            Rule::DATETIME_DELTA => {
                datetime_delta = Some(parse_datetime_delta(each)?);
            }

            r @ _ => {
                return Err(ParserError::InvalidGrammer(format!(
                    "unknown term in datetime : {r:?}"
                )))
            }
        }
    }

    match (datetime, datetime_fn) {
        (Some(datetime), None) => Ok(DatetimeFilterValue::DateString(datetime, datetime_delta)),
        (None, Some(datetime_fn)) => Ok(DatetimeFilterValue::Function(datetime_fn, datetime_delta)),
        (datetime_str, datetime_fn) => Err(ParserError::InvalidGrammer(format!(
            "invalid datetime : {datetime_str:?},  {datetime_fn:?}"
        ))),
    }
}

pub fn parse_datetime_delta<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeDelta> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME_DELTA {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::DATETIME_DELTA),
            format!("{:?}", pair.as_rule()),
        ));
    }

    match pair.into_inner().next() {
        None => Err(ParserError::InvalidGrammer(format!(
            "invalid datetime delta"
        ))),

        Some(date_time_delta) => match date_time_delta.as_rule() {
            Rule::DURATION_DELTA => {
                // e.g. "+ 1 hour"
                Ok(DatetimeDelta::MicroSec(
                    *duration_parser::parse_duration_delta(date_time_delta)?,
                ))
            }
            Rule::CLOCK_DELTA => {
                // e.g. "- 2:00"
                Ok(DatetimeDelta::FixedOffset(clock_parser::parse_clock_delta(
                    date_time_delta,
                )?))
            }

            r => Err(ParserError::InvalidGrammer(format!(
                "unknown term in build in datetime delta : {r:?}"
            ))),
        },
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn parse_datetime_delta_1() {
        let dt_delta = r"+ 2 hours";

        let pairs = QueryGrammer::parse(Rule::DATETIME_DELTA, dt_delta);

        assert!(pairs.is_ok());
    }

    #[test]
    fn parse_datetime_delta_2() {
        let dt_delta = r"2 hours";
        let pairs = QueryGrammer::parse(Rule::DATETIME_DELTA, dt_delta);
        assert!(pairs.is_ok());
    }

    #[test]
    fn parse_datetime_delta_3() {
        let dt_delta = r"-2 hours";
        let pairs = QueryGrammer::parse(Rule::DATETIME_DELTA, dt_delta);
        assert!(pairs.is_ok());
    }

    #[test]
    fn parse_datetime_range_1() {
        let dt_delta = r"('2012-12-30', +  2 hours)";
        let pairs = QueryGrammer::parse(Rule::DATETIME_RANGE, dt_delta);
        assert!(pairs.is_ok());
    }

    #[test]
    fn parse_datetime_filter_1() {
        let dt_delta = r"ts in ('2012-12-30', +  2 hours)";
        let pairs = QueryGrammer::parse(Rule::FILTER, dt_delta);
        assert!(pairs.is_ok());

        let dt_delta = r"where ts in ('2012-12-30', +  2 hours)";
        let pairs = QueryGrammer::parse(Rule::WHERE_CLAUSE, dt_delta);
        assert!(pairs.is_ok());
    }

    #[test]
    fn parse_datetime_range_close_1() {
        let dt_delta = r"+  2 hours";
        let pairs = QueryGrammer::parse(Rule::DATETIME_RANGE_CLOSE, dt_delta);
        assert!(pairs.is_ok());
    }
}
