use super::{operation::*, Result};

use ::zikeiretsu::*;
use serde::Serialize;

pub async fn execute(describe_database_condition: DescribeDatabaseCondition) -> Result<()> {
    let metricses = Zikeiretsu::list_metrics(
        Some(describe_database_condition.db_dir.clone()),
        &describe_database_condition.setting,
    )
    .await?;

    let mut describes = Vec::<DatabaseDescribe>::new();
    for metrics in metricses.into_iter() {
        let block_list = Zikeiretsu::block_list_data(
            &describe_database_condition.db_dir,
            &metrics,
            &describe_database_condition.setting,
        )
        .await?;

        describes.push(DatabaseDescribe {
            metrics,
            block_list,
        });
    }

    match describe_database_condition.output_setting.format {
        output::OutputFormat::Json => {
            let json_str = serde_json::to_string(&describes)
                .map_err(|e| output::OutputError::SerdeJsonError(e))?;
            describe_database_condition
                .output_setting
                .destination
                .write(vec![json_str])?
        }
        output::OutputFormat::Tsv => describe_database_condition
            .output_setting
            .destination
            .write(DatabaseDescribe::to_strs(describes))?,
    };
    Ok(())
}

#[derive(Serialize)]
struct DatabaseDescribe {
    metrics: Metrics,
    block_list: block_list::BlockList,
}

impl DatabaseDescribe {
    fn to_strs(describes: Vec<DatabaseDescribe>) -> Vec<String> {
        let mut result = Vec::<String>::new();
        for each in describes {
            result.push(format!(
                "{}\tupdated at:{}",
                each.metrics, each.block_list.updated_timestamp_sec
            ));
            for ts in each.block_list.block_timestamps {
                result.push(format!("\t{}\t{}", ts.since_sec, ts.until_sec));
            }
        }
        result
    }
}
