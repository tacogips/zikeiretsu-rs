use super::super::{ExecuteResult, ExecuteResultData, Result};
use super::format::output_with_condition;
use crate::tsdb::data_types::PolarsConvatibleDataFrame;
use crate::OutputCondition;
use arrow::record_batch::RecordBatch;

pub async fn output_execute_result(result: ExecuteResult) -> Result<()> {
    if let Some(error_message) = result.error_message {
        eprintln!("{}", error_message);
        Ok(())
    } else {
        match result.data {
            Some(ExecuteResultData {
                records,
                output_condition,
            }) => match records {
                None => {
                    log::error!("empty result data")
                }
                Some(records) => output_records(records, output_condition).await?,
            },
            None => {
                log::error!("empty result data")
            }
        };
        Ok(())
    }
}

async fn output_records(df: RecordBatch, condition: OutputCondition) -> Result<()> {
    output_with_condition!(condition, df);
    Ok(())
}
