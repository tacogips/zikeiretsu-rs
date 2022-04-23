pub mod describe_metrics;
pub mod metrics_list;
pub mod output;
pub mod search_metrics;

use crate::tsdb::data_types::DataSeriesRefsError;
use crate::tsdb::engine::EngineError;
use crate::tsdb::lexer::{interpret, DatabaseName, InterpretedQuery, LexerError, OutputError};
use crate::tsdb::query::parser::{parse_query, ParserError};
use crate::tsdb::query::QuerySetting;
use crate::tsdb::{DBConfig, DBContext, TimeSeriesDataFrame};
pub use output::*;
use polars::prelude::PolarsError;
use serde::{Deserialize, Serialize};
use std::io::Error as IoError;
use std::path::PathBuf;
use thiserror::Error;

pub mod execute_results {
    pub use super::{ExecuteResult, ExecuteResultData};
    pub use crate::describe_metrics::MetricsDescribe;
    pub use crate::tsdb::Metrics;
    pub use crate::DataFrame;
    pub use crate::OutputCondition;
}

use crate::tsdb::dataframe::DataframeError;
use execute_results::*;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename = "ocr")]
pub struct OutputConditionResult {
    #[serde(rename = "output_condition")]
    pub output_condition: OutputCondition,

    #[serde(rename = "column_names")]
    pub column_names: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct ExecuteResult {
    data: Option<ExecuteResultData>,
    error_message: Option<String>,
}

impl ExecuteResult {
    fn is_error(&self) -> bool {
        self.error_message.is_some()
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "t", content = "c")]
pub enum ExecuteResultData {
    MetricsList(Vec<Metrics>, OutputConditionResult),
    DescribeMetrics(DataFrame, OutputConditionResult),
    SearchMetrics(Option<TimeSeriesDataFrame>, OutputConditionResult),
}

pub async fn execute_query(ctx: &DBContext, query: &str) -> ExecuteResult {
    match inner_execute_query(ctx, query).await {
        Ok(data) => ExecuteResult {
            data: Some(data),
            error_message: None,
        },
        Err(e) => ExecuteResult {
            data: None,
            error_message: Some(format!("{e:?}")),
        },
    }
}
async fn inner_execute_query(ctx: &DBContext, query: &str) -> Result<ExecuteResultData> {
    let parsed_query = parse_query(query)?;
    let interpreted_query = interpret(parsed_query)?;
    match interpreted_query {
        InterpretedQuery::ListMetrics(database_name, output_condition, query_setting) => {
            let (db_config, db_dir) = to_db_config_and_db_dir(database_name, ctx, query_setting)?;
            let db_dir = db_dir.display().to_string();
            let (metrics, column_names) =
                metrics_list::execute_metrics_list(Some(&db_dir), &db_config).await?;

            Ok(ExecuteResultData::MetricsList(
                metrics,
                OutputConditionResult {
                    output_condition,
                    column_names: Some(column_names),
                },
            ))
        }

        InterpretedQuery::DescribeMetrics(database_name, describe_condition, query_setting) => {
            let (db_config, db_dir) = to_db_config_and_db_dir(database_name, ctx, query_setting)?;
            let db_dir = db_dir.display().to_string();
            let (df, column_names) = describe_metrics::execute_describe_metrics(
                &db_dir,
                &db_config,
                describe_condition.metrics_filter,
                false,
            )
            .await?;

            Ok(ExecuteResultData::DescribeMetrics(
                df,
                OutputConditionResult {
                    output_condition: describe_condition.output_condition,
                    column_names: Some(column_names),
                },
            ))
        }

        InterpretedQuery::DescribeBlockList(database_name, describe_condition, query_setting) => {
            let (db_config, db_dir) = to_db_config_and_db_dir(database_name, ctx, query_setting)?;
            let db_dir = db_dir.display().to_string();
            let (df, column_names) = describe_metrics::execute_describe_metrics(
                &db_dir,
                &db_config,
                describe_condition.metrics_filter,
                true,
            )
            .await?;

            Ok(ExecuteResultData::DescribeMetrics(
                df,
                OutputConditionResult {
                    output_condition: describe_condition.output_condition,
                    column_names: Some(column_names),
                },
            ))
        }

        InterpretedQuery::SearchMetrics(database_name, query_condition, query_setting) => {
            let (db_config, db_dir) = to_db_config_and_db_dir(database_name, ctx, query_setting)?;
            let db_dir = db_dir.display().to_string();

            let (query_result_df, column_names) =
                search_metrics::execute_search_metrics(&db_dir, &db_config, &query_condition)
                    .await?;

            Ok(ExecuteResultData::SearchMetrics(
                query_result_df,
                OutputConditionResult {
                    output_condition: query_condition.output_condition,
                    column_names,
                },
            ))
        }
    }
}

fn to_db_config_and_db_dir(
    database_name: Option<DatabaseName>,
    ctx: &DBContext,
    query_setting: QuerySetting,
) -> Result<(DBConfig, PathBuf)> {
    let database = match ctx.get_database(database_name.as_ref().map(|name| name.as_str())) {
        Ok(database) => match database {
            None => {
                return Err(EvalError::NoDatabaseFound(
                    "no database definitions".to_string(),
                ))
            }
            Some(database) => database,
        },
        Err(e) => return Err(EvalError::NoDatabaseFound(format!("{}", e))),
    };
    let db_dir = database.as_local_db_dir(&ctx.data_dir);

    Ok((
        DBConfig {
            cache_setting: query_setting.cache_setting,
            cloud_storage: database.cloud_storage.clone(),
            cloud_setting: query_setting.cloud_setting,
        },
        db_dir,
    ))
}

pub type Result<T> = std::result::Result<T, EvalError>;
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

    #[error("polars dataframe error: {0}")]
    PolarsError(#[from] PolarsError),

    #[error("no db dir")]
    DBDirNotSet,

    #[error("no database found:{0}")]
    NoDatabaseFound(String),
}
