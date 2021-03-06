use super::ExecuteError;

use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::InterpretedQueryCondition;
use crate::tsdb::DBConfig;
use crate::tsdb::TimeSeriesDataFrame;

pub async fn execute_search_metrics(
    database_name: &str,
    db_dir: &str,
    db_config: &DBConfig,
    condition: &InterpretedQueryCondition,
) -> Result<Option<TimeSeriesDataFrame>, ExecuteError> {
    let dataframe = Engine::search(
        database_name,
        &db_dir,
        &condition.metrics,
        condition.field_selectors.as_deref(),
        &condition.datetime_search_condition,
        db_config,
    )
    .await?;
    match dataframe {
        None => Ok(None),
        Some(mut dataframe) => {
            dataframe.set_column_names(condition.field_names.clone());
            Ok(Some(dataframe))
        }
    }
}
