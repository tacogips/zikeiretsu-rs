use super::output::*;
use super::EvalError;
use crate::tsdb::data_types::{DataFrame, DataSeries, SeriesValues};
use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{OutputCondition, OutputWriter};
use crate::tsdb::DBConfig;
use crate::tsdb::Metrics;
use crate::tsdb::{DataSeriesRefs, StringDataSeriesRefs};

pub async fn execute_metrics_list(
    db_dir: Option<&str>,
    db_config: &DBConfig,
) -> Result<DataFrame, EvalError> {
    let metricses = Engine::list_metrics(db_dir, db_config).await?;
    let metricses_strs = metricses
        .clone()
        .into_iter()
        .map(|m| m.into_inner())
        .collect();

    let metrics = DataFrame::new(
        vec![DataSeries::new(SeriesValues::String(metricses_strs))],
        Some(vec!["metrics".to_string()]),
    );

    Ok(metrics)
}
