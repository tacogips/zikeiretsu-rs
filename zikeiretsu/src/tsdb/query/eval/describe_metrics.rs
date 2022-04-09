use super::output::*;
use super::EvalError;
use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{OutputCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::{block_list, Metrics};
use crate::tsdb::{DataFrame, DataSeriesRefs, StringDataSeriesRefs, StringSeriesRef};
use serde::Serialize;

pub async fn execute_describe_metrics(
    ctx: &DBContext,
    metrics_filter: Option<Metrics>,
    output_condition: Option<OutputCondition>,
) -> Result<Vec<Metrics>, EvalError> {
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

    let metricses_strs = metricses
        .clone()
        .into_iter()
        .map(|m| m.into_inner())
        .collect();
    let mut series = StringDataSeriesRefs::default();
    series.push(&metricses_strs);

    let p_df = series
        .as_polar_dataframes(Some(vec!["metrics".to_string()]), None)
        .await?;

    if let Some(output_condition) = output_condition {
        output_with_condition!(output_condition, p_df);
    }
    Ok(metricses)
}

pub async fn load_metrics_describes(metrics: &[Metrics]) -> Result<(), EvalError> {
    unimplemented!()
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

struct MetrcisDescribe {
    metrics: Metrics,
    block_list: block_list::BlockList,
}

impl MetrcisDescribe {
    fn datetime_range(&self) -> Vec<String> {
        unimplemented!()
        //let mut result = Vec::<String>::new();
        //for each in describes {
        //    result.push(format!(
        //        "{metrics}\tupdated at:{updated_at}",
        //        metrics = each.metrics,
        //        updated_at = each.block_list.updated_timestamp_sec
        //    ));
        //    for ts in each.block_list.block_timestamps {
        //        result.push(format!(
        //            "\t{since}\t{until}",
        //            since = ts.since_sec,
        //            until = ts.until_sec
        //        ));
        //    }
        //}
        //result
    }
}
