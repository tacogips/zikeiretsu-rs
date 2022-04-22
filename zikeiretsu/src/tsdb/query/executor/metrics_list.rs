use super::output::*;
use super::EvalError;
use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{OutputCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::DBConfig;
use crate::tsdb::Metrics;
use crate::tsdb::{DataSeriesRefs, StringDataSeriesRefs};

pub async fn execute_metrics_list(
    ctx: &DBContext,
    db_config: &DBConfig,
    output_condition: Option<OutputCondition>,
) -> Result<Vec<Metrics>, EvalError> {
    let metricses = Engine::list_metrics(ctx.data_dir.as_ref(), &db_config).await?;
    let metricses_strs = metricses
        .clone()
        .into_iter()
        .map(|m| m.into_inner())
        .collect();
    let mut series = StringDataSeriesRefs::default();
    series.push(&metricses_strs);

    let mut p_df = series
        .as_polar_dataframes(Some(vec!["metrics".to_string()]), None)
        .await?;

    if let Some(output_condition) = output_condition {
        output_with_condition!(output_condition, p_df);
    }
    Ok(metricses)
}
