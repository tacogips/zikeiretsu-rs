use crate::tsdb::datapoint::DatapointSearchCondition;
use crate::tsdb::datetime::DatetimeAccuracy;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::query::parser::*;

use chrono::{DateTime, Duration, FixedOffset, ParseError as ChoronoParseError, TimeZone, Utc};
use std::collections::HashMap;

use crate::EngineError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LexerError {
    #[error("invalid datertime range. start:{0}, end: {1}")]
    InvalidDatetimeRange(String, String),

    #[error("no from clause")]
    NoFrom,

    #[error("no selecft clause")]
    NoSelect,

    #[error("no column name definition in with clause. {0}")]
    NoColumnDef(String),

    #[error("invalid column definition:{0}")]
    InvalidColumnDefinition(String),

    #[error("invalid metrics:{0}")]
    InvalidMetrics(String),
}

pub type Result<T> = std::result::Result<T, LexerError>;

pub enum Query {
    ListMetrics,
    Metrics(QueryContext),
}

pub struct QueryContext {
    pub metrics: Metrics,
    pub field_selectors: Option<Vec<usize>>,
    pub search_condition: DatapointSearchCondition,
    pub output_format: OutputFormat,
    pub timezone: FixedOffset,
}

pub enum BuildinMetrics {
    ListMetrics,
}

impl BuildinMetrics {
    fn from(metrics: &str) -> Option<Self> {
        match metrics {
            ".metrics" => Some(Self::ListMetrics),
            _ => None,
        }
    }
}

fn interpret_search_condition<'q>(
    timezone: &FixedOffset,
    where_clause: Option<&WhereClause<'q>>,
) -> Result<DatapointSearchCondition> {
    match where_clause {
        None => Ok(DatapointSearchCondition::all()),
        Some(where_clause) => datetime_filter_to_condition(timezone, &where_clause.datetime_filter),
    }
}

fn datetime_filter_to_condition<'q>(
    timezone: &FixedOffset,
    datetime_filter: &DatetimeFilter<'q>,
) -> Result<DatapointSearchCondition> {
    match &datetime_filter {
        DatetimeFilter::In(_, from, to) => Ok(DatapointSearchCondition::new(
            Some(from.to_timestamp_nano(&timezone)),
            Some(to.to_timestamp_nano(&timezone)),
        )),
        DatetimeFilter::Gte(_, from) => Ok(DatapointSearchCondition::new(
            Some(from.to_timestamp_nano(&timezone)),
            None,
        )),
        DatetimeFilter::Gt(_, from) => Ok(DatapointSearchCondition::new(
            Some(from.to_timestamp_nano(&timezone) + Duration::nanoseconds(1)),
            None,
        )),
        DatetimeFilter::Lte(_, to) => Ok(DatapointSearchCondition::new(
            None,
            Some(to.to_timestamp_nano(&timezone) + Duration::nanoseconds(1)),
        )),
        DatetimeFilter::Lt(_, to) => Ok(DatapointSearchCondition::new(
            None,
            Some(to.to_timestamp_nano(&timezone)),
        )),
        DatetimeFilter::Equal(_, datetime_value) => {
            let from_dt_nano = datetime_value.to_timestamp_nano(&timezone);
            let from_dt = from_dt_nano.as_datetime_with_tz(timezone);
            let until_date_offset = match DatetimeAccuracy::from_datetime(from_dt) {
                DatetimeAccuracy::NanoSecond => Duration::nanoseconds(1),
                DatetimeAccuracy::MicroSecond => Duration::microseconds(1),
                DatetimeAccuracy::MilliSecond => Duration::milliseconds(1),
                DatetimeAccuracy::Second => Duration::seconds(1),
                DatetimeAccuracy::Minute => Duration::minutes(1),
                DatetimeAccuracy::Hour => Duration::hours(1),
                DatetimeAccuracy::Day => Duration::days(1),
            };

            Ok(DatapointSearchCondition::new(
                Some(from_dt_nano),
                Some((from_dt + until_date_offset).into()),
            ))
        }
    }
}

fn interpret_field_selector<'q>(
    column_index_map: Option<&HashMap<&'q str, usize>>,
    select: Option<&SelectClause<'q>>,
) -> Result<Option<Vec<usize>>> {
    // select columns
    match select {
        None => return Err(LexerError::NoSelect),
        Some(select) => {
            if select
                .select_columns
                .iter()
                .find(|each| *each == &Column::Asterick)
                .is_some()
            {
                Ok(None)
            } else {
                let mut field_selectors = Vec::<usize>::new();
                match column_index_map {
                    None => {
                        return Err(LexerError::NoColumnDef(format!(
                            "columns :{}",
                            select
                                .select_columns
                                .iter()
                                .map(|e| e.to_string())
                                .collect::<Vec<String>>()
                                .join(",")
                        )))
                    }
                    Some(column_index_map) => {
                        for column in select.select_columns.iter() {
                            if let Column::ColumnName(column_name) = column {
                                match column_index_map.get(column_name.as_str()) {
                                    Some(column_idx) => field_selectors.push(*column_idx),
                                    None => {
                                        return Err(LexerError::NoColumnDef(format!(
                                            "{}",
                                            column_name.as_str()
                                        )))
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Some(field_selectors))
            }
        }
    }
}

struct With<'q> {
    timezone: FixedOffset,
    output_format: OutputFormat,
    column_index_map: Option<HashMap<&'q str, usize>>,
}

impl<'q> Default for With<'q> {
    fn default() -> Self {
        let mut timezone: FixedOffset = FixedOffset::west(0);
        let mut output_format: OutputFormat = OutputFormat::Table;
        let mut column_index_map: Option<HashMap<&'q str, usize>> = None;

        Self {
            timezone,
            output_format,
            column_index_map,
        }
    }
}

fn interpret_with<'q>(with_clause: Option<WithClause<'q>>) -> Result<With<'q>> {
    let mut with = With::default();

    // with
    if let Some(with_clause) = with_clause {
        // def columns
        if let Some(def_columns) = with_clause.def_columns {
            let mut column_index = HashMap::new();
            for (idx, column) in def_columns.iter().enumerate() {
                match column {
                    Column::Asterick => {
                        return Err(LexerError::InvalidColumnDefinition("".to_string()))
                    }
                    Column::ColumnName(column_name) => {
                        column_index.insert(column_name.as_str(), idx);
                    }
                }
            }
            with.column_index_map = Some(column_index)
        }
        // time zone
        if let Some(tz) = with_clause.def_timezone {
            with.timezone = tz
        }

        // output format
        if let Some(output) = with_clause.def_output {
            with.output_format = output
        }
    }
    Ok(with)
}

pub fn interpret<'q>(parsed_query: ParsedQuery<'q>) -> Result<Query> {
    let metrics = match parsed_query.from {
        None => return Err(LexerError::NoFrom),
        Some(metrics) => match BuildinMetrics::from(&metrics.from) {
            Some(build_in_query) => match build_in_query {
                BuildinMetrics::ListMetrics => return Ok(Query::ListMetrics),
            },
            None => Metrics::new(metrics.from.to_string())
                .map_err(|err_msg| LexerError::InvalidMetrics(err_msg))?,
        },
    };

    let with = interpret_with(parsed_query.with)?;

    // select columns
    let field_selectors =
        interpret_field_selector(with.column_index_map.as_ref(), parsed_query.select.as_ref())?;
    let search_condition =
        interpret_search_condition(&with.timezone, parsed_query.r#where.as_ref())?;

    let query_context = QueryContext {
        metrics,
        field_selectors,
        search_condition,
        output_format: with.output_format,
        timezone: with.timezone,
    };
    Ok(Query::Metrics(query_context))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tsdb::datetime::*;

    fn jst() -> FixedOffset {
        FixedOffset::east(9 * 3600)
    }

    #[test]
    fn lexer_datetime_eq_1() {
        let dt = parse_datetime_str("'2021-09-27'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst(), &filter).unwrap();

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointSearchCondition::new(
                Some(expected_from),
                Some((expected_from + Duration::days(1)).into()),
            ),
            filter_cond
        );
    }

    #[test]
    fn lexer_datetime_eq_2() {
        let dt = parse_datetime_str("'2021-09-27 23:00'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst(), &filter).unwrap();

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointSearchCondition::new(
                Some(expected_from),
                Some((expected_from + Duration::hours(1)).into()),
            ),
            filter_cond
        );
    }

    #[test]
    fn lexer_datetime_eq_3() {
        let dt = parse_datetime_str("'2021-09-27 23:10'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst(), &filter).unwrap();

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointSearchCondition::new(
                Some(expected_from),
                Some((expected_from + Duration::minutes(1)).into()),
            ),
            filter_cond
        );
    }

    #[test]
    fn lexer_datetime_eq_4() {
        let dt = parse_datetime_str("'2021-09-27 23:00:01'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst(), &filter).unwrap();

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointSearchCondition::new(
                Some(expected_from),
                Some((expected_from + Duration::seconds(1)).into()),
            ),
            filter_cond
        );
    }
}
