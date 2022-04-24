use super::super::{ExecuteResult, ExecuteResultData, Result};
use crate::tsdb::data_types::dataseries_ref::DataSeriesRefs;
use crate::{DataFrame, OutputCondition, TimeSeriesDataFrame};

pub async fn output_execute_result(result: ExecuteResult) {
    if result.is_error() {
    } else {
        match result.data {
            Some(ExecuteResultData::MetricsList(df, conditon)) => {}
            Some(ExecuteResultData::DescribeMetrics(df, condition)) => {}
            Some(ExecuteResultData::SearchMetrics(ts_df, condition)) => {}
            None => {}
        }
    }
}

async fn output_metrics_list(df: DataFrame, condition: OutputCondition) -> Result<()> {
    let mut p_df = df.as_polar_dataframes(None).await?;
    Ok(())
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
