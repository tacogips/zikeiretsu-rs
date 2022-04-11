pub mod describe_metrics;
pub mod metrics_list;
pub mod output;
pub mod search_metrics;

use crate::tsdb::data_types::DataSeriesRefsError;
use crate::tsdb::engine::EngineError;
use crate::tsdb::lexer::{interpret, InterpretedQuery, LexerError, OutputError};
use crate::tsdb::query::parser::{parse_query, ParserError};
use crate::tsdb::query::QuerySetting;
use crate::tsdb::{DBConfig, DBContext};
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

pub async fn execute_query(ctx: &DBContext, query: &str) -> Result<()> {
    let parsed_query = parse_query(query)?;
    let interpreted_query = interpret(parsed_query)?;
    match interpreted_query {
        InterpretedQuery::ListMetrics(output_condition, query_setting) => {
            let db_config = to_db_config(&ctx, query_setting);
            metrics_list::execute_metrics_list(ctx, &db_config, Some(output_condition)).await?;
        }

        InterpretedQuery::DescribeMetrics(describe_condition, query_setting) => {
            let db_config = to_db_config(&ctx, query_setting);
            describe_metrics::execute_describe_metrics(
                ctx,
                &db_config,
                describe_condition.metrics_filter,
                Some(describe_condition.output_condition),
                false,
            )
            .await?;
        }

        InterpretedQuery::DescribeBlockList(describe_condition, query_setting) => {
            let db_config = to_db_config(&ctx, query_setting);
            describe_metrics::execute_describe_metrics(
                ctx,
                &db_config,
                describe_condition.metrics_filter,
                Some(describe_condition.output_condition),
                true,
            )
            .await?;
        }

        InterpretedQuery::SearchMetrics(query_condition, query_setting) => {
            let db_config = to_db_config(&ctx, query_setting);
            search_metrics::execute_search_metrics(ctx, &db_config, query_condition).await?;
        }
    }

    Ok(())
}

fn to_db_config(ctx: &DBContext, query_setting: QuerySetting) -> DBConfig {
    DBConfig {
        cache_setting: query_setting.cache_setting,
        cloud_storage: ctx.cloud_storage.clone(),
        cloud_setting: query_setting.cloud_setting,
    }
}
