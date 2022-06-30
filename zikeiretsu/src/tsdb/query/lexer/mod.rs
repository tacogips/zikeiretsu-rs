mod from;
mod select;
mod r#where;
mod with;

use crate::tsdb::datapoint::DatapointsSearchCondition;
use crate::tsdb::metrics::Metrics;
pub use crate::tsdb::query::parser::clause::{OutputFormat, WhereClause, WithClause};
use crate::tsdb::query::parser::*;
use crate::tsdb::TimeZoneAndOffset;
use crate::tsdb::{CacheSetting, CloudStorageSetting};
use either::Either;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Error as IoError;
use std::path::PathBuf;
use std::result::Result as StdResult;
use thiserror::Error;

#[derive(Error, Debug, Serialize, Deserialize)]
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

    #[error("you need at least one where condition ")]
    EmptyFilterCondition,

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

    #[error("invalid output format : {0}")]
    InvalidOutputFormat(String),

    #[error("cannot output to file: {0}")]
    CannotOutputToFile(String),
}

#[derive(Debug)]
pub struct DatabaseName(String);
impl DatabaseName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug)]
pub enum InterpretedQuery {
    ListMetrics(Option<DatabaseName>, OutputCondition, QuerySetting),
    DescribeMetrics(Option<DatabaseName>, DescribeMetrics, QuerySetting),
    DescribeBlockList(Option<DatabaseName>, DescribeBlockList, QuerySetting),
    SearchMetrics(
        Option<DatabaseName>,
        InterpretedQueryCondition,
        QuerySetting,
    ),
}

#[derive(Debug)]
pub struct QuerySetting {
    pub cache_setting: CacheSetting,
    pub cloud_setting: CloudStorageSetting,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct OutputCondition {
    pub output_format: OutputFormat,
    pub output_to_memory: bool,
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
    Memory,
}
impl OutputWriter {
    fn validate_available_for_format(&self, format: &OutputFormat) -> StdResult<(), OutputError> {
        match self {
            OutputWriter::Memory => match format {
                OutputFormat::Table => Ok(()),
                _ => Err(OutputError::InvalidOutputFormat(
                    "output format must be 'Table' when output to memory".to_string(),
                )),
            },

            OutputWriter::File(_) => Ok(()),
            OutputWriter::Stdout => match format {
                OutputFormat::Json => Ok(()),
                OutputFormat::Table => Ok(()),
                OutputFormat::Parquet | OutputFormat::ParquetSnappy => match self {
                    OutputWriter::File(_) => Ok(()),
                    OutputWriter::Stdout | OutputWriter::Memory => {
                        Err(OutputError::InvalidOutputDestination(
                            "parquet format can output to only a file".to_string(),
                        ))
                    }
                },
            },
        }
    }
}

impl OutputCondition {
    pub fn output_wirter(&self) -> StdResult<OutputWriter, OutputError> {
        if self.output_to_memory {
            if self.output_file_path.is_some() {
                Err(OutputError::CannotOutputToFile("memory".to_string()))
            } else {
                Ok(OutputWriter::Memory)
            }
        } else {
            let output_destination = match &self.output_file_path {
                None => Ok(OutputWriter::Stdout),

                Some(output_file_path) => match output_file_path.parent() {
                    None => Err(OutputError::InvalidPath(format!("{:?} ", output_file_path))),
                    Some(output_dir) => {
                        if !output_dir.exists() {
                            fs::create_dir_all(output_dir)?;
                        }

                        let f = fs::OpenOptions::new()
                            .write(true)
                            .truncate(true)
                            .open(output_file_path)?;
                        Ok(OutputWriter::File(f))
                    }
                },
            }?;

            output_destination.validate_available_for_format(&self.output_format)?;
            Ok(output_destination)
        }
    }
}

#[derive(Debug)]
pub struct InterpretedQueryCondition {
    pub metrics: Metrics,
    pub field_selectors: Option<Vec<usize>>,
    pub field_names: Option<Vec<String>>,
    pub datetime_search_condition: DatapointsSearchCondition,
    pub output_condition: OutputCondition,
    pub format_datetime: bool,
    pub timezone: &'static TimeZoneAndOffset,
}

macro_rules! prepend_ts_column_to_head {
    ($column_names:expr) => {{
        let mut field_names_following_to_ts = vec!["ts".to_string()];
        field_names_following_to_ts.append(&mut $column_names);
        field_names_following_to_ts
    }};
}

pub(crate) fn interpret(parsed_query: ParsedQuery<'_>) -> Result<InterpretedQuery> {
    log::debug!("interpriting parsed query :{parsed_query:?}");
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
        None => with
            .column_name_aliases
            .map(|mut field_names| prepend_ts_column_to_head!(field_names)),
    };

    let datetime_search_condition = match parsed_query.r#where.as_ref() {
        None => return Err(LexerError::EmptyFilterCondition),
        Some(filter) => {
            r#where::interpret_datatime_search_condition(&with.timezone.offset, filter)?
        }
    };

    invalid_if_metrics_filter_exists(parsed_query.r#where.as_ref())?;

    let output_condition = OutputCondition {
        output_format: with.output_format,
        output_to_memory: with.output_to_memory,
        output_file_path: with.output_file_path,
    };

    let query_context = InterpretedQueryCondition {
        metrics,
        field_selectors,
        field_names,
        datetime_search_condition,
        output_condition,
        format_datetime: with.format_datetime,
        timezone: with.timezone,
    };
    let database_name = with
        .database
        .map(|database_name| DatabaseName(database_name.to_string()));

    Ok(InterpretedQuery::SearchMetrics(
        database_name,
        query_context,
        query_setting,
    ))
}

pub(crate) fn interpret_buildin_metrics(
    parsed_query: ParsedQuery<'_>,
    buildin_metrics: from::BuildinMetrics,
) -> Result<InterpretedQuery> {
    let with = with::interpret_with(parsed_query.with)?;

    let query_setting = QuerySetting {
        cache_setting: with.cache_setting,
        cloud_setting: with.cloud_setting,
    };
    let database_name = with
        .database
        .map(|database_name| DatabaseName(database_name.to_string()));

    match buildin_metrics {
        from::BuildinMetrics::ListMetrics => {
            invalid_if_metrics_filter_exists(parsed_query.r#where.as_ref())?;
            Ok(InterpretedQuery::ListMetrics(
                database_name,
                OutputCondition {
                    output_format: with.output_format,
                    output_to_memory: with.output_to_memory,
                    output_file_path: with.output_file_path,
                },
                query_setting,
            ))
        }

        from::BuildinMetrics::DescribeMetrics => {
            let output_condition = OutputCondition {
                output_format: with.output_format,
                output_to_memory: with.output_to_memory,
                output_file_path: with.output_file_path,
            };

            let metrics_filter = match parsed_query.r#where {
                Some(where_clause) => where_clause.metrics_filter,
                None => None,
            };

            Ok(InterpretedQuery::DescribeMetrics(
                database_name,
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
                output_to_memory: with.output_to_memory,
                output_file_path: with.output_file_path,
            };

            let metrics_filter = match parsed_query.r#where {
                Some(where_clause) => where_clause.metrics_filter,
                None => None,
            };

            Ok(InterpretedQuery::DescribeBlockList(
                database_name,
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

#[cfg(test)]
mod test {

    use super::*;

    use tempfile::tempfile;
    #[test]
    fn test_output_writer_validate() {
        {
            //stdout
            let writer = OutputWriter::Stdout;
            assert!(writer
                .validate_available_for_format(&OutputFormat::Json)
                .is_ok());

            assert!(writer
                .validate_available_for_format(&OutputFormat::Table)
                .is_ok());

            assert!(writer
                .validate_available_for_format(&OutputFormat::Parquet)
                .is_err());

            assert!(writer
                .validate_available_for_format(&OutputFormat::ParquetSnappy)
                .is_err());
        }

        {
            //file
            let f = tempfile().unwrap();
            let writer = OutputWriter::File(f);
            assert!(writer
                .validate_available_for_format(&OutputFormat::Json)
                .is_ok());

            assert!(writer
                .validate_available_for_format(&OutputFormat::Table)
                .is_ok());

            assert!(writer
                .validate_available_for_format(&OutputFormat::Parquet)
                .is_ok());

            assert!(writer
                .validate_available_for_format(&OutputFormat::ParquetSnappy)
                .is_ok());
        }

        {
            let writer = OutputWriter::Memory;
            assert!(writer
                .validate_available_for_format(&OutputFormat::Json)
                .is_err());

            assert!(writer
                .validate_available_for_format(&OutputFormat::Table)
                .is_ok());

            assert!(writer
                .validate_available_for_format(&OutputFormat::Parquet)
                .is_err());

            assert!(writer
                .validate_available_for_format(&OutputFormat::ParquetSnappy)
                .is_err());
        }
    }
}
