mod from;
mod select;
mod r#where;
mod with;

use crate::tsdb::datapoint::DatapointSearchCondition;
use crate::tsdb::metrics::Metrics;
pub use crate::tsdb::query::parser::clause::{OutputFormat, WhereClause, WithClause};
use crate::tsdb::query::parser::*;
use crate::tsdb::{CacheSetting, CloudStorageSetting};
use chrono::FixedOffset;
use either::Either;
use std::fs;
use std::io::Error as IoError;
use std::path::PathBuf;
use std::result::Result as StdResult;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LexerError {
    #[error("invalid datertime range. start:{0}, end: {1}")]
    InvalidDatetimeRange(String, String),

    #[error("metrics filter not supported :{0}")]
    MetricsFilterIsNotSupported(String),

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

#[derive(Error, Debug)]
pub enum OutputError {
    #[error("{0}")]
    IoError(#[from] IoError),

    #[error("invalid output file path : {0}")]
    InvalidPath(String),

    #[error("invalid output destination : {0}")]
    InvalidOutputDestination(String),
}

#[derive(Debug)]
pub enum InterpretedQuery {
    ListMetrics(OutputCondition, QuerySetting),
    DescribeMetrics(DescribeMetrics, QuerySetting),
    DescribeBlockList(DescribeBlockList, QuerySetting),
    SearchMetrics(InterpretedQueryCondition, QuerySetting),
}

#[derive(Debug)]
pub struct QuerySetting {
    pub cache_setting: CacheSetting,
    pub cloud_setting: CloudStorageSetting,
}

#[derive(Debug)]
pub struct OutputCondition {
    pub output_format: OutputFormat,
    pub output_file_path: Option<PathBuf>,
}

#[derive(Debug)]
pub struct DescribeMetrics {
    pub output_condition: OutputCondition,
    pub metrics_filter: Option<Metrics>,
}

#[derive(Debug)]
pub struct DescribeBlockList {
    pub output_condition: OutputCondition,
    pub metrics_filter: Option<Metrics>,
}

pub enum OutputWriter {
    Stdout,
    File(fs::File),
}
impl OutputWriter {
    fn validate_available_for_format(&self, format: &OutputFormat) -> StdResult<(), OutputError> {
        match format {
            OutputFormat::Json => Ok(()),
            OutputFormat::DataFrame => Ok(()),
            OutputFormat::Parquet => match &self {
                OutputWriter::File(_) => Ok(()),
                OutputWriter::Stdout => Err(OutputError::InvalidOutputDestination(
                    "parquet format can output to only a file".to_string(),
                )),
            },
        }
    }
}

impl OutputCondition {
    pub fn output_wirter(&self) -> StdResult<OutputWriter, OutputError> {
        let output_destination = match &self.output_file_path {
            None => Ok(OutputWriter::Stdout),

            Some(output_file_path) => match output_file_path.parent() {
                None => Err(OutputError::InvalidPath(format!("{:?} ", output_file_path))),
                Some(output_dir) => {
                    if output_dir.exists() {
                        fs::create_dir_all(output_dir)?;
                    }
                    let f = fs::File::create(output_file_path)?;
                    Ok(OutputWriter::File(f))
                }
            },
        }?;

        output_destination.validate_available_for_format(&self.output_format)?;
        Ok(output_destination)
    }
}

#[derive(Debug)]
pub struct InterpretedQueryCondition {
    pub metrics: Metrics,
    pub field_selectors: Option<Vec<usize>>,
    pub field_names: Option<Vec<String>>,
    pub datetime_search_condition: DatapointSearchCondition,
    pub output_condition: Option<OutputCondition>,
    pub timezone: FixedOffset,
}

macro_rules! prepend_ts_column_to_head {
    ($column_names:expr) => {{
        let mut field_names_following_to_ts = vec!["ts".to_string()];
        field_names_following_to_ts.append(&mut $column_names);
        field_names_following_to_ts
    }};
}

pub(crate) fn interpret<'q>(parsed_query: ParsedQuery<'q>) -> Result<InterpretedQuery> {
    let metrics = match from::parse_from(parsed_query.from.as_ref())? {
        Either::Right(buildin_metrics) => {
            return interpret_buildin_metrics(parsed_query, buildin_metrics)
        }
        Either::Left(parsed_metrics) => parsed_metrics,
    };

    let with = with::interpret_with(parsed_query.with)?;

    let query_setting = QuerySetting {
        cache_setting: with.cache_setting,
        cloud_setting: with.cloud_setting,
    };

    // select columns
    let (field_selectors, filtered_field_names) = match select::interpret_field_selector(
        with.column_index_map.as_ref(),
        parsed_query.select.as_ref(),
    )? {
        None => (None, None),
        Some((field_selectors, field_names)) => (Some(field_selectors), Some(field_names)),
    };

    let field_names = match filtered_field_names {
        Some(mut field_names) => Some(prepend_ts_column_to_head!(field_names)),
        None => match with.column_name_aliases {
            Some(mut field_names) => Some(prepend_ts_column_to_head!(field_names)),
            None => None,
        },
    };

    let datetime_search_condition = r#where::interpret_datatime_search_condition(
        &with.timezone,
        parsed_query.r#where.as_ref(),
    )?;

    invalid_if_metrics_filter_exists(parsed_query.r#where.as_ref())?;

    let output_condition = Some(OutputCondition {
        output_format: with.output_format,
        output_file_path: with.output_file_path,
    });

    let query_context = InterpretedQueryCondition {
        metrics,
        field_selectors,
        field_names,
        datetime_search_condition,
        output_condition,
        timezone: with.timezone,
    };
    Ok(InterpretedQuery::SearchMetrics(
        query_context,
        query_setting,
    ))
}

pub(crate) fn interpret_buildin_metrics<'q>(
    parsed_query: ParsedQuery<'q>,
    buildin_metrics: from::BuildinMetrics,
) -> Result<InterpretedQuery> {
    let with = with::interpret_with(parsed_query.with)?;

    let query_setting = QuerySetting {
        cache_setting: with.cache_setting,
        cloud_setting: with.cloud_setting,
    };

    match buildin_metrics {
        from::BuildinMetrics::ListMetrics => {
            invalid_if_metrics_filter_exists(parsed_query.r#where.as_ref())?;
            Ok(InterpretedQuery::ListMetrics(
                OutputCondition {
                    output_format: with.output_format,
                    output_file_path: with.output_file_path,
                },
                query_setting,
            ))
        }

        from::BuildinMetrics::DescribeMetrics => {
            let output_condition = OutputCondition {
                output_format: with.output_format,
                output_file_path: with.output_file_path,
            };

            let metrics_filter = match parsed_query.r#where {
                Some(where_clause) => where_clause.metrics_filter,
                None => None,
            };

            Ok(InterpretedQuery::DescribeMetrics(
                DescribeMetrics {
                    output_condition,
                    metrics_filter,
                },
                query_setting,
            ))
        }

        from::BuildinMetrics::DescribeBlockList => {
            let output_condition = OutputCondition {
                output_format: with.output_format,
                output_file_path: with.output_file_path,
            };

            let metrics_filter = match parsed_query.r#where {
                Some(where_clause) => where_clause.metrics_filter,
                None => None,
            };

            Ok(InterpretedQuery::DescribeBlockList(
                DescribeBlockList {
                    output_condition,
                    metrics_filter,
                },
                query_setting,
            ))
        }
    }
}

fn invalid_if_metrics_filter_exists(where_clause: Option<&WhereClause<'_>>) -> Result<()> {
    if let Some(where_clause) = where_clause {
        if where_clause.metrics_filter.is_some() {
            return Err(LexerError::MetricsFilterIsNotSupported(
                "allowed only on '.describe', '.block_list'".to_string(),
            ));
        }
    }
    Ok(())
}
