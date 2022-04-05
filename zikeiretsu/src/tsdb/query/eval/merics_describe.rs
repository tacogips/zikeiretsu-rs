use super::output::*;
use super::EvalError;
use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{OutputCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::{block_list, Metrics};
use crate::tsdb::{StringDataSeriesRefs, StringSeriesRef};
use serde::Serialize;

#[derive(Serialize)]
struct DatabaseDescribe {
    metrics: Metrics,
    block_list: block_list::BlockList,
}

impl DatabaseDescribe {
    pub fn as_metrics_list_serieses<'a>(
        metrics: &'a Vec<DatabaseDescribe>,
    ) -> StringDataSeriesRefs<'a> {
        unimplemented!()
    }
}

//pub async fn execute_metrics_list(
//    ctx: &DBContext,
//    output_condition: OutputCondition,
//) -> Result<(), EvalError> {
//    let metricses = Engine::list_metrics(Some(&ctx.db_dir), &ctx.db_config).await?;
//
//    let mut metrics = Vec::<Metrics>::new();
//
//    for metrics in metricses.into_iter() {
//        let block_list =
//            Engine::block_list_data(&ctx.db_dir.clone(), &metrics, &ctx.db_config).await?;
//
//        describes.push(DatabaseDescribe {
//            metrics,
//            block_list,
//        });
//    }
//
