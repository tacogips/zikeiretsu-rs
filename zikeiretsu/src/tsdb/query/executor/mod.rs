pub mod describe_metrics;
pub mod interface;
pub mod metrics_list;
pub mod output;
pub mod search_metrics;

use crate::tsdb::data_types::{ArrowConvatibleDataFrame, ArrowConvatibleDataFrameError};
use crate::tsdb::engine::EngineError;
use crate::tsdb::lexer::{interpret, DatabaseName, InterpretedQuery, LexerError, OutputError};
use crate::tsdb::query::parser::{parse_query, ParserError};
use crate::tsdb::query::QuerySetting;
use crate::tsdb::{DBConfig, DBContext, TimeSeriesDataFrame};
use arrow::error::ArrowError;
use arrow::record_batch::*;
pub use interface::*;
use std::io::Error as IoError;
use std::path::PathBuf;
use thiserror::Error;

pub use crate::describe_metrics::MetricsDescribe;
pub use crate::tsdb::Metrics;
pub use crate::DataFrame;
pub use crate::OutputCondition;

use crate::tsdb::dataframe::DataframeError;
use parquet::errors::ParquetError;

#[derive(Debug, PartialEq)]
pub struct ExecuteResult {
    data: Option<ExecuteResultData>,
    error_message: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct ExecuteResultData {
    pub records: Option<RecordBatch>,
    pub output_condition: OutputCondition,
}

pub async fn execute_query(ctx: &DBContext, query: &str) -> ExecuteResult {
    match inner_execute_query(ctx, query).await {
        Ok(data) => ExecuteResult {
            data: Some(data),
            error_message: None,
        },
        Err(e) => ExecuteResult {
            data: None,
            error_message: Some(format!("{e}")),
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
            let metrics = metrics_list::execute_metrics_list(Some(&db_dir), &db_config).await?;

            Ok(ExecuteResultData {
                records: Some(metrics.as_arrow_record_batchs(false, None).await?),
                output_condition,
            })
        }

        InterpretedQuery::DescribeMetrics(database_name, describe_condition, query_setting) => {
            let (db_config, db_dir) = to_db_config_and_db_dir(database_name, ctx, query_setting)?;
            let db_dir = db_dir.display().to_string();
            let df = describe_metrics::execute_describe_metrics(
                &db_dir,
                &db_config,
                describe_condition.metrics_filter,
                false,
            )
            .await?;

            Ok(ExecuteResultData {
                records: Some(df.as_arrow_record_batchs(false, None).await?),
                output_condition: describe_condition.output_condition,
            })
        }

        InterpretedQuery::DescribeBlockList(database_name, describe_condition, query_setting) => {
            let (db_config, db_dir) = to_db_config_and_db_dir(database_name, ctx, query_setting)?;
            let db_dir = db_dir.display().to_string();
            let df = describe_metrics::execute_describe_metrics(
                &db_dir,
                &db_config,
                describe_condition.metrics_filter,
                true,
            )
            .await?;

            Ok(ExecuteResultData {
                records: Some(df.as_arrow_record_batchs(false, None).await?),
                output_condition: describe_condition.output_condition,
            })
        }

        InterpretedQuery::SearchMetrics(database_name, query_condition, query_setting) => {
            let (db_config, db_dir) = to_db_config_and_db_dir(database_name, ctx, query_setting)?;
            let db_dir = db_dir.display().to_string();

            let query_result_df =
                search_metrics::execute_search_metrics(&db_dir, &db_config, &query_condition)
                    .await?;

            match query_result_df {
                None => Ok(ExecuteResultData {
                    records: None,
                    output_condition: query_condition.output_condition,
                }),
                Some(df) => Ok(ExecuteResultData {
                    records: Some(
                        df.as_arrow_record_batchs(
                            query_condition.format_datetime,
                            Some(&query_condition.timezone),
                        )
                        .await?,
                    ),
                    output_condition: query_condition.output_condition,
                }),
            }
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
                return Err(ExecuteError::NoDatabaseFound(
                    "no database definitions".to_string(),
                ))
            }
            Some(database) => database,
        },
        Err(e) => return Err(ExecuteError::NoDatabaseFound(format!("{}", e))),
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

pub type Result<T> = std::result::Result<T, ExecuteError>;
#[derive(Error, Debug)]
pub enum ExecuteError {
    #[error("repl read line error: {0}")]
    IoError(#[from] IoError),

    #[error("dataframe error: {0}")]
    DataframeError(#[from] DataframeError),

    #[error("parser error: {0}")]
    ParserError(#[from] ParserError),

    #[error("lexer error: {0}")]
    LexerError(#[from] LexerError),

    #[error("output error: {0}")]
    OutputError(#[from] OutputError),

    #[error("engine error: {0}")]
    EngineError(#[from] EngineError),

    #[error("arrow table error: {0}")]
    ArrowConvatibleDataFrameError(#[from] ArrowConvatibleDataFrameError),

    #[error("serde json error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("metrics not found: {0}")]
    MetricsNotFoundError(String),

    #[error("no db dir")]
    DBDirNotSet,

    #[error("no database found: {0}")]
    NoDatabaseFound(String),

    #[error("arrow error: {0}")]
    ArrowError(#[from] ArrowError),

    #[error("parquet error: {0}")]
    ParquetError(#[from] ParquetError),
}
