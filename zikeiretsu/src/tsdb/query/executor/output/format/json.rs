use super::PolarsConvatibleDataFrameOutput;
use crate::tsdb::query::executor::Result as EvalResult;
use polars::prelude::DataFrame as PDataFrame;
use std::io::Write as IoWrite;

pub struct JsonDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> PolarsConvatibleDataFrameOutput for JsonDfOutput<Dest> {
    fn output(&mut self, df: &mut PDataFrame) -> EvalResult<()> {
        write!(self.0, "{}", serde_json::to_string(&df)?)?;
        Ok(())
    }
}
