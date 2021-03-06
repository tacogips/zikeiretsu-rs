use super::ExecuteError;
use crate::tsdb::engine::{Engine, EngineError};
use crate::tsdb::DBConfig;
use crate::tsdb::{block_list, Metrics};
use crate::tsdb::{DataFrame, DataSeries, SeriesValues, TimestampNano, TimestampSec};
use futures::future;
use serde::{Deserialize, Serialize};

pub async fn execute_describe_metrics(
    database_name: &str,
    db_dir: &str,
    db_config: &DBConfig,
    metrics_filter: Option<Metrics>,
    show_block_list: bool,
) -> Result<DataFrame, ExecuteError> {
    let metricses = Engine::list_metrics(Some(&db_dir), db_config).await?;
    let metricses = match metrics_filter {
        Some(metrics_filter) => metricses
            .into_iter()
            .find(|each| *each == metrics_filter)
            .map_or(
                Err(ExecuteError::MetricsNotFoundError(format!(
                    "{}",
                    metrics_filter
                ))),
                |found| Ok(vec![found]),
            )?,
        None => metricses,
    };
    if metricses.is_empty() {
        return Err(ExecuteError::MetricsNotFoundError("[empty]".to_string()));
    }

    let describes = load_metrics_describes(database_name, db_dir, db_config, metricses).await?;
    let df = if show_block_list {
        describes_to_dataframe_with_block_list(describes.as_slice())?
    } else {
        describes_to_dataframe(describes.as_slice())?
    };
    Ok(df)
}

async fn load_metrics_describes(
    database_name: &str,
    db_dir: &str,
    db_config: &DBConfig,
    metricses: Vec<Metrics>,
) -> Result<Vec<MetricsDescribe>, ExecuteError> {
    let metrics_descibes = metricses.into_iter().map(|metrics| async move {
        Engine::block_list_data(database_name, db_dir, &metrics, db_config)
            .await
            .map(|block_list| MetricsDescribe {
                metrics,
                block_list,
            })
    });
    let describes = future::join_all(metrics_descibes)
        .await
        .into_iter()
        .collect::<Result<Vec<MetricsDescribe>, EngineError>>()?;

    Ok(describes)
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct MetricsDescribe {
    metrics: Metrics,
    block_list: block_list::BlockList,
}

//TODO(tacogips) return DataFrameRef instead
fn describes_to_dataframe(describes: &[MetricsDescribe]) -> Result<DataFrame, ExecuteError> {
    let mut metrics_names = Vec::<String>::new();
    let mut update_ats = Vec::<TimestampNano>::new();
    let mut block_num = Vec::<u64>::new();
    let mut data_range_starts = Vec::<TimestampSec>::new();
    let mut data_range_ends = Vec::<TimestampSec>::new();

    for each_descirbe in describes.iter() {
        metrics_names.push(each_descirbe.metrics.to_string());
        update_ats.push(each_descirbe.block_list.updated_timestamp_sec);
        block_num.push(each_descirbe.block_list.block_num() as u64);
        match each_descirbe.block_list.range() {
            Some((start, end)) => {
                data_range_starts.push(*start);
                data_range_ends.push(*end);
            }
            None => {
                data_range_starts.push(TimestampSec::zero());
                data_range_ends.push(TimestampSec::zero());
            }
        }
    }

    let data_serieses: Vec<DataSeries> = vec![
        SeriesValues::String(metrics_names).into(),
        SeriesValues::TimestampNano(update_ats).into(),
        SeriesValues::UInt64(block_num).into(),
        SeriesValues::TimestampSec(data_range_starts).into(),
        SeriesValues::TimestampSec(data_range_ends).into(),
    ];

    Ok(DataFrame::new(
        data_serieses,
        Some(vec![
            "metrics".to_string(),
            "updated_at".to_string(),
            "block_num".to_string(),
            "from".to_string(),
            "end".to_string(),
        ]),
    ))
}

//TODO(tacogips) return DataFrameRef instead
fn describes_to_dataframe_with_block_list(
    describes: &[MetricsDescribe],
) -> Result<DataFrame, ExecuteError> {
    let mut metrics_names = Vec::<String>::new();
    let mut update_ats = Vec::<TimestampNano>::new();
    let mut block_num = Vec::<u64>::new();

    let mut seq = Vec::<u64>::new();
    let mut block_list_start = Vec::<TimestampSec>::new();
    let mut block_list_end = Vec::<TimestampSec>::new();

    for each_descirbe in describes.iter() {
        for (idx, each_meta) in each_descirbe.block_list.block_meta_infos.iter().enumerate() {
            metrics_names.push(each_descirbe.metrics.to_string());
            update_ats.push(each_descirbe.block_list.updated_timestamp_sec);
            block_num.push(each_descirbe.block_list.block_num() as u64);
            seq.push(idx as u64 + 1);
            block_list_start.push(each_meta.block_timestamp.since_sec);
            block_list_end.push(each_meta.block_timestamp.until_sec);
        }
    }

    let data_serieses: Vec<DataSeries> = vec![
        SeriesValues::String(metrics_names).into(),
        SeriesValues::TimestampNano(update_ats).into(),
        SeriesValues::UInt64(block_num).into(),
        SeriesValues::UInt64(seq).into(),
        SeriesValues::TimestampSec(block_list_start).into(),
        SeriesValues::TimestampSec(block_list_end).into(),
    ];

    Ok(DataFrame::new(
        data_serieses,
        Some(vec![
            "metrics".to_string(),
            "updated_at".to_string(),
            "block_num".to_string(),
            "seq".to_string(),
            "block_list_start".to_string(),
            "block_list_end".to_string(),
        ]),
    ))
}
