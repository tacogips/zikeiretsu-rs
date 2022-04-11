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
}

pub struct QuerySetting {
    pub cache_setting: CacheSetting,
    pub cloud_setting: CloudStorageSetting,
}

pub enum InterpretedQuery {
    ListMetrics(OutputCondition, QuerySetting),
    DescribeMetrics(DescribeMetrics, QuerySetting),
    SearchMetrics(InterpretedQueryCondition, QuerySetting),
}

pub struct OutputCondition {
    pub output_format: OutputFormat,
    pub output_file_path: Option<PathBuf>,
}

pub struct DescribeMetrics {
    pub output_condition: OutputCondition,
    pub metrics_filter: Option<Metrics>,
}

pub enum OutputWriter {
    Stdout,
    File(fs::File),
}

impl OutputCondition {
    pub fn output_wirter(&self) -> StdResult<OutputWriter, OutputError> {
        match &self.output_file_path {
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
        }
    }
}

pub struct InterpretedQueryCondition {
    pub metrics: Metrics,
    pub field_selectors: Option<Vec<usize>>,
    pub field_names: Option<Vec<String>>,
    pub datetime_search_condition: DatapointSearchCondition,
    pub output_condition: Option<OutputCondition>,
    pub timezone: FixedOffset,
}

pub fn interpret<'q>(parsed_query: ParsedQuery<'q>) -> Result<InterpretedQuery> {
    let with = with::interpret_with(parsed_query.with)?;

    let query_setting = QuerySetting {
        cache_setting: with.cache_setting,
        cloud_setting: with.cloud_setting,
    };

    let metrics = match from::parse_from(parsed_query.from.as_ref())? {
        Either::Right(buildin_metrics) => match buildin_metrics {
            from::BuildinMetrics::ListMetrics => {
                invalid_if_metrics_filter_exists(parsed_query.r#where.as_ref())?;
                return Ok(InterpretedQuery::ListMetrics(
                    OutputCondition {
                        output_format: with.output_format,
                        output_file_path: with.output_file_path,
                    },
                    query_setting,
                ));
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

                return Ok(InterpretedQuery::DescribeMetrics(
                    DescribeMetrics {
                        output_condition,
                        metrics_filter,
                    },
                    query_setting,
                ));
            }
        },
        Either::Left(parsed_metrics) => parsed_metrics,
    };

    // select columns
    let (field_selectors, field_names) = match select::interpret_field_selector(
        with.column_index_map.as_ref(),
        parsed_query.select.as_ref(),
    )? {
        None => (None, None),
        Some((field_selectors, field_names)) => (Some(field_selectors), Some(field_names)),
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

fn invalid_if_metrics_filter_exists(where_clause: Option<&WhereClause<'_>>) -> Result<()> {
    if let Some(where_clause) = where_clause {
        if where_clause.metrics_filter.is_some() {
            return Err(LexerError::MetricsFilterIsNotSupported(
                "allowed only on '.describe'".to_string(),
            ));
        }
    }
    Ok(())
}
