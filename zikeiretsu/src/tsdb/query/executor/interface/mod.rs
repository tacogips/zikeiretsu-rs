mod arrow_flight_server;
use super::{execute_query, output::*, ExecuteError};
use crate::tsdb::engine::DBContext;
use async_trait::async_trait;
use thiserror::Error;

pub use arrow_flight_server::*;

pub type Result<T> = std::result::Result<T, ExecutorInterfaceError>;

#[derive(Error, Debug)]
pub enum ExecutorInterfaceError {
    #[error("{0}")]
    ExecuteError(#[from] ExecuteError),
}

#[async_trait]
pub trait ExecutorInterface {
    async fn execute_query(&self, ctx: &DBContext, query: &str) -> Result<()>;
}

pub struct AdhocExecutorInterface;

#[async_trait]
impl ExecutorInterface for AdhocExecutorInterface {
    async fn execute_query(&self, ctx: &DBContext, query: &str) -> Result<()> {
        match execute_query(ctx, query).await {
            Err(e) => {
                eprintln!("{}", e);
            }
            Ok(result) => {
                if let Some(records) = result.records {
                    output_records(records, result.output_condition).await?
                } else {
                    println!("[empty]")
                }
            }
        }
        Ok(())
    }
}
