use crate::tsdb::datapoint::DatapointSearchCondition;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::query::parser::*;

use chrono::{DateTime, FixedOffset, ParseError as ChoronoParseError, TimeZone, Utc};
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
    fn search(metrics: &str) -> Option<Self> {
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
    unimplemented!()
    //match where_clause {
    //    None => Ok(DatapointSearchCondition::all()),
    //    Some(where_clause) => match where_clause.datetime_filter {
    //        DatetimeFilter::In(_, filter_value, DatetimeFilterValue) => {}
    //        DatetimeFilter::Gte(_, filter_value) => {}
    //        DatetimeFilter::Gt(_, filter_value) => {}
    //        DatetimeFilter::Lte(_, filter_value) => {}
    //        DatetimeFilter::Lt(_, filter_value) => {}
    //        DatetimeFilter::Equal(_, filter_value) => {}
    //    },
    //}
}

fn interpret_field_selector<'q>(
    column_index_map: Option<HashMap<&'q str, usize>>,
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

pub fn interpret<'q>(parsed_query: ParsedQuery<'q>) -> Result<Query> {
    let metrics = match parsed_query.from {
        None => return Err(LexerError::NoFrom),
        Some(from) => match BuildinMetrics::search(&from.from) {
            Some(build_in_query) => match build_in_query {
                BuildinMetrics::ListMetrics => return Ok(Query::ListMetrics),
            },
            None => Metrics::new(from.from.to_string())
                .map_err(|err_msg| LexerError::InvalidMetrics(err_msg))?,
        },
    };

    let mut timezone: FixedOffset = FixedOffset::west(0);
    let mut output_format: OutputFormat = OutputFormat::Table;
    let mut column_index_map: Option<HashMap<&'q str, usize>> = None;

    // with
    if let Some(with) = parsed_query.with {
        // def columns
        if let Some(def_columns) = with.def_columns {
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
            column_index_map = Some(column_index)
        }
        // time zone
        if let Some(tz) = with.def_timezone {
            timezone = tz
        }

        // output format
        if let Some(output) = with.def_output {
            output_format = output
        }
    }

    // select columns
    let field_selectors = interpret_field_selector(column_index_map, parsed_query.select.as_ref())?;
    let search_condition = interpret_search_condition(&timezone, parsed_query.r#where.as_ref())?;

    let query_context = QueryContext {
        metrics,
        field_selectors,
        search_condition,
        output_format,
        timezone,
    };
    Ok(Query::Metrics(query_context))
}
