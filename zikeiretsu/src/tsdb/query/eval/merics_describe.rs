use super::output::*;
use super::EvalError;
use crate::tsdb::engine::Engine;
use crate::tsdb::query::lexer::{OutputCondition, OutputWriter};
use crate::tsdb::query::DBContext;
use crate::tsdb::{block_list, Metrics};
use crate::tsdb::{StringDataSeriesRefs, StringSeriesRef};
use serde::Serialize;

//TODO(tacogips) describes not supported yet
//#[derive(Serialize)]
//struct DatabaseDescribe {
//    metrics: Metrics,
//    block_list: block_list::BlockList,
//}
//
//impl DatabaseDescribe {
//    pub fn as_metrics_list_serieses<'a>(
//        metrics: &'a Vec<DatabaseDescribe>,
//    ) -> StringDataSeriesRefs<'a> {
//        unimplemented!()
//    }
//}
//
