use super::{clock_parser, duration_parser};
use once_cell::sync::OnceCell;
use pest::iterators::Pair;

use crate::tsdb::query::parser::*;
use chrono::{format as chrono_format, DateTime, NaiveDateTime, NaiveTime, Utc};

#[derive(Debug)]
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
                None => Err(QueryError::InvalidGrammer(format!(
                    "'in' needs datetime range  "
                ))),
                Some(datetime_2) => Ok(DatetimeFilter::In(column_name, datetime_1, datetime_2)),
            },
            ">=" => Ok(DatetimeFilter::Gte(column_name, datetime_1)),
            ">" => Ok(DatetimeFilter::Gt(column_name, datetime_1)),
            "<=" => Ok(DatetimeFilter::Lte(column_name, datetime_1)),
            "<" => Ok(DatetimeFilter::Lt(column_name, datetime_1)),
            "=" => Ok(DatetimeFilter::Equal(column_name, datetime_1)),
            invalid_operator => Err(QueryError::InvalidDatetimeFilterOperator(
                invalid_operator.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
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
}

#[derive(Debug, Clone)]
pub enum DatetimeFilterValue {
    DateString(DateTime<Utc>, Option<DatetimeDelta>),
    Function(BuildinDatetimeFunction, Option<DatetimeDelta>),
}

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeFilter<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME_FILTER {
        return Err(QueryError::UnexpectedPair(
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
                        return Err(QueryError::InvalidGrammer(format!(
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
                return Err(QueryError::InvalidGrammer(format!(
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
            Err(QueryError::InvalidGrammer(format!(
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
        return Err(QueryError::UnexpectedPair(
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
                    return Err(QueryError::InvalidGrammer(format!(
                        " datetime filter val1  is needed. "
                    )));
                }
            }
            r @ _ => {
                return Err(QueryError::InvalidGrammer(format!(
                    "unknown term in datetime filter : {r:?}"
                )))
            }
        }
    }

    match datetime {
        None => Err(QueryError::InvalidGrammer(format!(
            "invalid datetime filter close. "
        ))),
        Some(datetime) => Ok(datetime),
    }
}

pub fn parse_datetime<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeFilterValue> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME {
        return Err(QueryError::UnexpectedPair(
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
                            return Err(QueryError::InvalidGrammer(format!(
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
                return Err(QueryError::InvalidGrammer(format!(
                    "unknown term in datetime : {r:?}"
                )))
            }
        }
    }

    match (datetime, datetime_fn) {
        (Some(datetime), None) => Ok(DatetimeFilterValue::DateString(datetime, datetime_delta)),
        (None, Some(datetime_fn)) => Ok(DatetimeFilterValue::Function(datetime_fn, datetime_delta)),
        (datetime_str, datetime_fn) => Err(QueryError::InvalidGrammer(format!(
            "invalid datetime : {datetime_str:?},  {datetime_fn:?}"
        ))),
    }
}

pub fn parse_datetime_delta<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeDelta> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME_DELTA {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::DATETIME_DELTA),
            format!("{:?}", pair.as_rule()),
        ));
    }

    match pair.into_inner().next() {
        None => Err(QueryError::InvalidGrammer(format!(
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

            r => Err(QueryError::InvalidGrammer(format!(
                "unknown term in build in datetime delta : {r:?}"
            ))),
        },
    }
}

static DATETIME_FORMATS: OnceCell<Vec<(chrono_format::StrftimeItems<'static>, bool)>> =
    OnceCell::new();

type NaiveDateOrNot = bool;
pub fn datetime_formats() -> &'static [(chrono_format::StrftimeItems<'static>, NaiveDateOrNot)] {
    fn dt_fmt(s: &str) -> chrono_format::StrftimeItems {
        chrono_format::StrftimeItems::new(s)
    }

    DATETIME_FORMATS
        .get_or_init(|| {
            vec![
                (dt_fmt("%Y-%m-%d %H:%M:%S"), false),
                (dt_fmt("%Y-%m-%d %H:%M:%S.%f"), false),
                (dt_fmt("%Y-%m-%d %H:%M"), false),
                (dt_fmt("%Y-%m-%d"), true),
            ]
        })
        .as_slice()
}

/// availabe formats
/// 'yyyy-MM-DD hh:mm:ss.ZZZZZZ'
/// 'yyyy-MM-DD hh:mm:ss'
/// 'yyyy-MM-DD hh:mm'
/// 'yyyy-MM-DD'
fn parse_datetime_str(datetime_str: &str) -> Result<DateTime<Utc>> {
    if datetime_str.len() < 2 {
        return Err(QueryError::InvalidDatetimeFormat(datetime_str.to_string()));
    }
    if !datetime_str.starts_with("'") || !datetime_str.ends_with("'") {
        return Err(QueryError::InvalidDatetimeFormat(datetime_str.to_string()));
    }

    //strip single quotes
    let datetime_str: &str = &datetime_str[1..][..datetime_str.len() - 2];
    for (each_format, is_naive_date) in datetime_formats() {
        let mut parsed = chrono_format::Parsed::new();

        if let Ok(_) = chrono_format::parse(&mut parsed, datetime_str, each_format.clone()) {
            if *is_naive_date {
                let naive = parsed.to_naive_date()?;
                let naive = NaiveDateTime::new(naive, NaiveTime::from_hms(0, 0, 0));
                return Ok(DateTime::from_utc(naive, Utc));
            } else {
                let parsed = parsed.to_datetime_with_timezone(&Utc)?;

                return Ok(parsed);
            }
        }
    }

    Err(QueryError::InvalidDatetimeFormat(datetime_str.to_string()))
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn parse_datetetime_test() {
        let parse_result = parse_datetime_str("'2019-12-13 23:33:12'");
        assert!(parse_result.is_ok());

        let parse_result = parse_datetime_str("'2019-12-13 23:33:12.023'");
        assert!(parse_result.is_ok());

        let parse_result = parse_datetime_str("'2019-12-13 23:33'");
        assert!(parse_result.is_ok());

        let parse_result = parse_datetime_str("'2019-12-13'");
        assert!(parse_result.is_ok());

        let parse_result = parse_datetime_str("'2019-12-13");
        assert!(parse_result.is_err());
    }
}
