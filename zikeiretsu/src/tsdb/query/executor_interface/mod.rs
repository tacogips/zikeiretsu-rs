use super::executor::{execute_query, output::*, EvalError};
use crate::tsdb::engine::DBContext;
use async_trait::async_trait;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ExecutorInterfaceError>;

#[derive(Error, Debug)]
pub enum ExecutorInterfaceError {
    #[error("{0}")]
    EvalError(#[from] EvalError),
}

#[async_trait]
pub trait ExecutorInterface {
    async fn execute_query(&self, ctx: &DBContext, query: &str) -> Result<()>;
}

pub struct AdhocExecutorInterface;

#[async_trait]
impl ExecutorInterface for AdhocExecutorInterface {
    async fn execute_query(&self, ctx: &DBContext, query: &str) -> Result<()> {
        let result = execute_query(ctx, query).await;
        output_execute_result(result).await?;
        Ok(())
    }
}
