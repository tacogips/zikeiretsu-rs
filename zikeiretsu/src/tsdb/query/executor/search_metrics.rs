use super::output::*;
use super::EvalError;

use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{InterpretedQueryCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::DBConfig;
use crate::tsdb::DataSeriesRefs;
use polars::prelude::DataFrame as PDataFrame;

pub async fn execute_search_metrics(
    db_dir: &str,
    db_config: &DBConfig,
    condition: InterpretedQueryCondition,
) -> Result<Option<PDataFrame>, EvalError> {
    let store = Engine::search(
        &db_dir,
        &condition.metrics,
        condition
            .field_selectors
            .as_ref()
            .map(|indices| indices.as_slice()),
        &condition.datetime_search_condition,
        &db_config,
    )
    .await?;
    match store {
        None => Ok(None),
        Some(store) => {
            let mut p_df = store
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
