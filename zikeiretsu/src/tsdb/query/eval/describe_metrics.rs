use super::output::*;
use super::EvalError;
use crate::tsdb::engine::{Engine, EngineError};
use crate::tsdb::query::lexer::{OutputCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::{block_list, Metrics};
use crate::tsdb::{
    DataFrame, DataSeries, DataSeriesRefs, SeriesValues, StringDataSeriesRefs, StringSeriesRef,
    TimeSeriesDataFrame, TimestampNano, TimestampSec,
};
use futures::{future, Future, FutureExt};
use serde::Serialize;

pub async fn execute_describe_metrics(
    ctx: &DBContext,
    metrics_filter: Option<Metrics>,
    output_condition: Option<OutputCondition>,
) -> Result<Vec<MetricsDescribe>, EvalError> {
    let metricses = Engine::list_metrics(Some(&ctx.db_dir), &ctx.db_config).await?;
    let metricses = match metrics_filter {
        Some(metrics_filter) => metricses
            .into_iter()
            .find(|each| *each == metrics_filter)
            .map_or(
                Err(EvalError::MetricsNotFoundError(format!(
                    "{}",
                    metrics_filter
                ))),
                |found| Ok(vec![found]),
            )?,
        None => metricses,
    };
    if metricses.is_empty() {
        return Err(EvalError::MetricsNotFoundError("[empty]".to_string()));
    }

    let describes = load_metrics_describes(&ctx, metricses).await?;
    let df = describes_to_dataframe(describes.as_slice())?;
    let p_df = df
        .as_polar_dataframes(
            Some(vec![
                "metrics".to_string(),
                "updated_at".to_string(),
                "from".to_string(),
                "end".to_string(),
            ]),
            None,
        )
        .await?;

    if let Some(output_condition) = output_condition {
        output_with_condition!(output_condition, p_df);
    }
    Ok(describes)
}

async fn load_metrics_describes(
    ctx: &DBContext,
    metricses: Vec<Metrics>,
) -> Result<Vec<MetricsDescribe>, EvalError> {
    let metrics_descibes = metricses.into_iter().map(|metrics| async move {
        Engine::block_list_data(&ctx.db_dir, &metrics, &ctx.db_config)
            .await
            .and_then(|block_list| {
                Ok(MetricsDescribe {
                    metrics,
                    block_list,
                })
            })
    });
    let describes = future::join_all(metrics_descibes)
        .await
        .into_iter()
        .collect::<Result<Vec<MetricsDescribe>, EngineError>>()?;

    Ok(describes)
}

//TODO(tacogips) return DataFrameRef instead
fn describes_to_dataframe<'a>(describes: &[MetricsDescribe]) -> Result<DataFrame, EvalError> {
    let mut metrics_names = Vec::<String>::new();
    let mut update_ats = Vec::<TimestampNano>::new();
    let mut data_range_starts = Vec::<TimestampSec>::new();
    let mut data_range_ends = Vec::<TimestampSec>::new();

    for each_descirbe in describes.into_iter() {
        metrics_names.push(each_descirbe.metrics.to_string());
        update_ats.push(each_descirbe.block_list.updated_timestamp_sec);
        match each_descirbe.block_list.range() {
            Some((start, end)) => {
                data_range_starts.push(start.clone());
                data_range_ends.push(end.clone());
            }
            None => {
                data_range_starts.push(TimestampSec::zero());
                data_range_ends.push(TimestampSec::zero());
            }
        }
    }

    let mut data_serieses: Vec<DataSeries> = vec![];

    //    "metrics"
    //    "updated_at"
    //    "from"
    //    "end"
    data_serieses.push(SeriesValues::String(metrics_names).into());
    data_serieses.push(SeriesValues::TimestampNano(update_ats).into());
    data_serieses.push(SeriesValues::TimestampSec(data_range_starts).into());
    data_serieses.push(SeriesValues::TimestampSec(data_range_ends).into());

    Ok(DataFrame::new(data_serieses))
}

//pub async fn execute(describe_database_condition: DescribeDatabaseCondition) -> Result<()> {
//    let metricses = Zikeiretsu::list_metrics(
//        Some(describe_database_condition.db_dir.clone()),
//        &describe_database_condition.setting,
//    )
//    .await?;
//
//    let mut describes = Vec::<DatabaseDescribe>::new();
//    for metrics in metricses.into_iter() {
//        let block_list = Zikeiretsu::block_list_data(
//            &describe_database_condition.db_dir,
//            &metrics,
//            &describe_database_condition.setting,
//        )
//        .await?;
//
//        describes.push(DatabaseDescribe {
//            metrics,
//            block_list,
//        });
//    }
//
//    match describe_database_condition.output_setting.format {
//        output::OutputFormat::Json => {
//            let json_str = serde_json::to_string(&describes)
//                .map_err(|e| output::OutputError::SerdeJsonError(e))?;
//            describe_database_condition
//                .output_setting
//                .destination
//                .write(vec![json_str])?
//        }
//        output::OutputFormat::Tsv => describe_database_condition
//            .output_setting
//            .destination
//            .write(DatabaseDescribe::to_strs(describes))?,
//    };
//    Ok(())
//}
//

pub struct MetricsDescribe {
    metrics: Metrics,
    block_list: block_list::BlockList,
}
