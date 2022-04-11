pub mod describe_metrics;
pub mod metrics;
pub mod metrics_list;
pub mod output;

use crate::tsdb::query::{
    parser::{parse_query, ParserError},
    DBContext,
};
use crate::tsdb::DBConfig;

use crate::tsdb::data_types::DataSeriesRefsError;
use crate::tsdb::engine::EngineError;
use crate::tsdb::lexer::{
    interpret, InterpretedQuery, InterpretedQueryCondition, LexerError, OutputError,
};
pub use metrics::*;
pub use metrics_list::*;
pub use output::*;
use std::io::Error as IoError;
use thiserror::Error;

use crate::tsdb::dataframe::DataframeError;
#[derive(Error, Debug)]
pub enum EvalError {
    #[error("repl read line error {0}")]
    IoError(#[from] IoError),

    #[error("dataframe error {0}")]
    DataframeError(#[from] DataframeError),

    #[error("parser error {0}")]
    ParserError(#[from] ParserError),

    #[error("lexer error {0}")]
    LexerError(#[from] LexerError),

    #[error("output error {0}")]
    OutputError(#[from] OutputError),

    #[error("engine error {0}")]
    EngineError(#[from] EngineError),

    #[error("dataseries ref error {0}")]
    DataSeriesRefsError(#[from] DataSeriesRefsError),

    #[error("serde json error {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("metrics not found: {0}")]
    MetricsNotFoundError(String),

    #[error("no db dir")]
    DBDirNotSet,
}

pub type Result<T> = std::result::Result<T, EvalError>;

pub async fn execute(ctx: &DBContext, query: &str) -> Result<()> {
    let parsed_query = parse_query(query)?;
    let interpreted_query = interpret(parsed_query)?;
    match interpreted_query {
        InterpretedQuery::ListMetrics(output_condition, query_setting) => {
            let db_config = DBConfig {
                cache_setting: query_setting.cache_setting,
                cloud_storage: ctx.cloud_storage.clone(),
                cloud_setting: query_setting.cloud_setting,
            };

            metrics_list::execute_metrics_list(ctx, &db_config, Some(output_condition)).await?;
        }

        InterpretedQuery::DescribeMetrics(describe_condition, query_setting) => {
            let db_config = DBConfig {
                cache_setting: query_setting.cache_setting,
                cloud_storage: ctx.cloud_storage.clone(),
                cloud_setting: query_setting.cloud_setting,
            };

            describe_metrics::execute_describe_metrics(
                ctx,
                &db_config,
                describe_condition.metrics_filter,
                Some(describe_condition.output_condition),
            )
            .await?;
        }
        InterpretedQuery::SearchMetrics(query_condition, query_setting) => {

            //TODO(impl)
        }
    }

    Ok(())
}
