use super::output::*;
use super::EvalError;
use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{OutputCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::{block_list, Metrics};
use crate::tsdb::{DataSeriesRefs, StringDataSeriesRefs, StringSeriesRef};
use serde::Serialize;

pub async fn execute_metrics_list(
    ctx: &DBContext,
    output_condition: OutputCondition,
) -> Result<(), EvalError> {
    let metricses = Engine::list_metrics(Some(&ctx.db_dir), &ctx.db_config).await?;
    let metricses = metricses.into_iter().map(|m| m.into_inner()).collect();
    let mut series = StringDataSeriesRefs::default();
    series.push(&metricses);

    let p_df = series.as_polar_dataframes(Some(&["metrics"]), None).await?;

    match output_condition.output_wirter()? {
        OutputWriter::Stdout => {
            let out = std::io::stdout();
            let out = std::io::BufWriter::new(out.lock());
            let mut destination =
                new_data_series_refs_vec_output(&output_condition.output_format, out);
            destination.output(&p_df)?;
        }
        OutputWriter::File(f) => {
            let out = std::io::BufWriter::new(f);
            let mut destination =
                new_data_series_refs_vec_output::<_>(&output_condition.output_format, out);
            destination.output(&p_df)?;
        }
    }

    Ok(())
}
