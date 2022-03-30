mod from;
mod select;
mod r#where;
mod with;

use crate::tsdb::datapoint::DatapointSearchCondition;
use crate::tsdb::datetime::DatetimeAccuracy;
use crate::tsdb::metrics::Metrics;
pub use crate::tsdb::query::parser::clause::OutputFormat;
pub use crate::tsdb::query::parser::clause::WithClause;
use crate::tsdb::query::parser::*;
use chrono::{DateTime, Duration, FixedOffset, ParseError as ChoronoParseError, TimeZone, Utc};
use either::Either;
use std::path::PathBuf;

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

pub enum InterpretedQuery {
    ListMetrics(OutputCondition),
    Metrics(InterpretedQueryCondition),
}

pub struct OutputCondition {
    pub output_format: OutputFormat,
    pub output_file_path: Option<PathBuf>,
}

pub struct InterpretedQueryCondition {
    pub metrics: Metrics,
    pub field_selectors: Option<Vec<usize>>,
    pub search_condition: DatapointSearchCondition,
    pub output_format: OutputFormat,
    pub output_file_path: Option<PathBuf>,
    pub timezone: FixedOffset,
}

pub fn interpret<'q>(parsed_query: ParsedQuery<'q>) -> Result<InterpretedQuery> {
    let with = with::interpret_with(parsed_query.with)?;
    let metrics = match from::parse_from(parsed_query.from.as_ref())? {
        Either::Right(buildin_metrics) => match buildin_metrics {
            from::BuildinMetrics::ListMetrics => {
                return Ok(InterpretedQuery::ListMetrics(OutputCondition {
                    output_format: with.output_format,
                    output_file_path: with.output_file_path,
                }))
            }
        },
        Either::Left(parsed_metrics) => parsed_metrics,
    };

    // select columns
    let field_selectors = select::interpret_field_selector(
        with.column_index_map.as_ref(),
        parsed_query.select.as_ref(),
    )?;
    let search_condition =
        r#where::interpret_search_condition(&with.timezone, parsed_query.r#where.as_ref())?;

    let query_context = InterpretedQueryCondition {
        metrics,
        field_selectors,
        search_condition,
        output_format: with.output_format,
        output_file_path: with.output_file_path,
        timezone: with.timezone,
    };
    Ok(InterpretedQuery::Metrics(query_context))
}
