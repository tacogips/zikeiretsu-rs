use super::{DataSeriesRefsOutput, EvalResult};
use crate::tsdb::DataSeriesRefs;
use async_trait::async_trait;
use chrono::FixedOffset;
use polars::prelude::{DataFrame as PDataFrame, Series as PSeries};
use std::io::Write as IoWrite;
use std::marker::PhantomData;

pub struct JsonDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> DataSeriesRefsOutput for JsonDfOutput<Dest> {
    fn output(&mut self, df: &PDataFrame) -> EvalResult<()> {
        write!(self.0, "{:?}", serde_json::to_string(&df)?)?;
        Ok(())
    }
}
