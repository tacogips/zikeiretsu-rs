use super::output::*;
use super::EvalError;
use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{OutputCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::{block_list, Metrics};
use crate::tsdb::{DataSeriesRefs, StringDataSeriesRefs, StringSeriesRef};
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
