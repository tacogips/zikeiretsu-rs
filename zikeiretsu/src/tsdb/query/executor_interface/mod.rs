use super::executor::execute_results::*;
use super::executor::output::*;
use crate::tsdb::engine::DBContext;
use async_trait::async_trait;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ExecutorInterfaceError>;

#[derive(Error, Debug)]
pub enum ExecutorInterfaceError {}

#[async_trait]
pub trait ExecutorInterface {
    async fn execute_query(&self, ctx: &DBContext, query: &str) -> Result<()>;
}

pub struct AdhocExecutorInterface {}

#[async_trait]
impl ExecutorInterface for AdhocExecutorInterface {
    async fn execute_query(&self, ctx: &DBContext, query: &str) -> Result<()> {
        Ok(())
    }
}

////TODO(tacogips) remove
//let mut p_df = series
//    .as_polar_dataframes(Some(vec!["metrics".to_string()]), None)
//    .await?;

//if let Some(output_condition) = output_condition {
//    output_with_condition!(output_condition, p_df);
//}
//
////-    let mut p_df = df.as_polar_dataframes(Some(column_names), None).await?;
//-
//-    if let Some(output_condition) = output_condition {
//-        output_with_condition!(output_condition, p_df);
//-    }
//-    Ok(describes)
//
//-        None => Ok(None),
//-        Some(dataframe) => {
//-            let mut p_df = dataframe
//-                .as_polar_dataframes(condition.field_names, Some(&condition.timezon>
//-                .await?;
//-
//-            if let Some(output_condition) = condition.output_condition {
//-                output_with_condition!(output_condition, p_df);
//-            }
//-            Ok(Some(p_df))
//-        }
