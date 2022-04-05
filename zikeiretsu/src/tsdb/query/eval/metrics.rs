use super::output::*;
use super::EvalError;

use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{InterpretedQueryCondition, OutputCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::{block_list, Metrics};
use crate::tsdb::{DataSeriesRefs, StringDataSeriesRefs, StringSeriesRef};
use polars::prelude::{DataFrame as PDataFrame, Series as PSeries};
use serde::Serialize;

pub async fn execute_search_metrics(
    ctx: &DBContext,
    condition: InterpretedQueryCondition,
) -> Result<Option<PDataFrame>, EvalError> {
    let store = Engine::search(
        &ctx.db_dir,
        condition.metrics.clone(),
        condition
            .field_selectors
            .as_ref()
            .map(|indices| indices.as_slice()),
        &condition.search_condition,
        &ctx.db_config,
    )
    .await?;
    match store {
        None => Ok(None),
        Some(store) => {
            let p_df = store
                .as_dataframe()
                .as_polar_dataframes(condition.field_names, Some(&condition.timezone))
                .await?;

            if let Some(output_condition) = condition.output_condition {
                output_with_condition!(output_condition, p_df);
            }
            Ok(Some(p_df))
        }
    }
}
