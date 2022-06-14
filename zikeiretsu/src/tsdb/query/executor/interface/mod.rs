mod arrow_flight_client;
mod arrow_flight_server;
use super::{execute_query, output::*, ExecuteError};
use crate::tsdb::engine::DBContext;
use arrow::record_batch::*;
use async_trait::async_trait;
use thiserror::Error;

pub use arrow_flight_client::*;
pub use arrow_flight_server::*;

pub type Result<T> = std::result::Result<T, ExecutorInterfaceError>;

#[derive(Error, Debug)]
pub enum ExecutorInterfaceError {
    #[error("{0}")]
    ExecuteError(#[from] ExecuteError),
}

#[async_trait]
pub trait ExecutorInterface {
    async fn execute_query(&mut self, ctx: &DBContext, query: &str) -> Result<Option<RecordBatch>>;
}

pub struct AdhocExecutorInterface;

#[async_trait]
impl ExecutorInterface for AdhocExecutorInterface {
    async fn execute_query(&mut self, ctx: &DBContext, query: &str) -> Result<Option<RecordBatch>> {
        match execute_query(ctx, query).await {
            Err(e) => {
                eprintln!("{}", e);
                Ok(None)
            }
            Ok(result) => {
                if let Some(records) = result.records {
                    let batch_record_if_not_spent =
                        output_records(records, result.output_condition).await?;
                    Ok(batch_record_if_not_spent)
                } else {
                    println!("[empty]");
                    Ok(None)
                }
            }
        }
    }
}
