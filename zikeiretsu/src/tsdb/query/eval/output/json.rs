use super::{DataSeriesRefsOutput, EvalResult};
use polars::prelude::DataFrame as PDataFrame;
use std::io::Write as IoWrite;

pub struct JsonDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> DataSeriesRefsOutput for JsonDfOutput<Dest> {
    fn output(&mut self, df: &PDataFrame) -> EvalResult<()> {
        write!(self.0, "{}", serde_json::to_string(&df)?)?;
        Ok(())
    }
}
