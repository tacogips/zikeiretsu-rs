use super::output::*;
use super::EvalError;
use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{OutputCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::{block_list, Metrics};
use crate::tsdb::{DataSeriesRefs, StringDataSeriesRefs, StringSeriesRef};
use serde::Serialize;

fn convert_to_metrics_list_serieses<'a>(metricses: &'a Vec<Metrics>) -> StringDataSeriesRefs<'a> {
    unimplemented!()
}

pub async fn execute_metrics_list(
    ctx: &DBContext,
    output_condition: OutputCondition,
) -> Result<(), EvalError> {
    let metricses = Engine::list_metrics(Some(&ctx.db_dir), &ctx.db_config).await?;
    let series = convert_to_metrics_list_serieses(&metricses);
    let mut p_df = series.as_polar_dataframes(Some(&["metrics"]), None).await?;

    match output_condition.output_wirter()? {
        OutputWriter::Stdout => {
            let out = std::io::stdout();
            let mut out = std::io::BufWriter::new(out.lock());
            let mut destination =
                new_data_series_refs_vec_output(&output_condition.output_format, out);
            destination.output(&p_df)?;
        }
        OutputWriter::File(f) => {
            let mut out = std::io::BufWriter::new(f);
            let mut destination =
                new_data_series_refs_vec_output::<_>(&output_condition.output_format, out);
            destination.output(&p_df)?;
        }
    }

    Ok(())
}

//use super::{operation::*, Result};
//
//use ::zikeiretsu::*;
//use serde::Serialize;
//
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
//#[derive(Serialize)]
//struct DatabaseDescribe {
//    metrics: Metrics,
//    block_list: block_list::BlockList,
//}
//
//impl DatabaseDescribe {
//    fn to_strs(describes: Vec<DatabaseDescribe>) -> Vec<String> {
//        let mut result = Vec::<String>::new();
//        for each in describes {
//            result.push(format!(
//                "{metrics}\tupdated at:{updated_at}",
//                metrics = each.metrics,
//                updated_at = each.block_list.updated_timestamp_sec
//            ));
//            for ts in each.block_list.block_timestamps {
//                result.push(format!(
//                    "\t{since}\t{until}",
//                    since = ts.since_sec,
//                    until = ts.until_sec
//                ));
//            }
//        }
//        result
//    }
//}
