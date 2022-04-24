use super::super::{ExecuteResult, ExecuteResultData, Result};
use super::format::output_with_condition;
use crate::tsdb::data_types::PolarsConvatibleDataFrame;
use crate::OutputCondition;

pub async fn output_execute_result(result: ExecuteResult) -> Result<()> {
    if let Some(error_message) = result.error_message {
        eprintln!("{}", error_message);
        Ok(())
    } else {
        match result.data {
            Some(ExecuteResultData::MetricsList(df, condition)) => {
                output_dataframe(df, condition).await?
            }
            Some(ExecuteResultData::DescribeMetrics(df, condition)) => {
                output_dataframe(df, condition).await?
            }
            Some(ExecuteResultData::DescribeBlokList(df, condition)) => {
                output_dataframe(df, condition).await?
            }
            Some(ExecuteResultData::SearchMetrics(ts_df, condition)) => match ts_df {
                Some(ts_df) => output_dataframe(ts_df, condition).await?,
                None => println!("[data not found]"),
            },
            None => {
                log::error!("empty result data")
            }
        };
        Ok(())
    }
}

async fn output_dataframe<DF>(df: DF, condition: OutputCondition) -> Result<()>
where
    DF: PolarsConvatibleDataFrame + Sync,
{
    let mut p_df = df.as_polar_dataframes(None).await?;
    output_with_condition!(condition, p_df);
    Ok(())
}
