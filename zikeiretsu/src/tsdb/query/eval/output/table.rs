use super::{DataSeriesRefsOutput, EvalResult};
use polars::prelude::DataFrame as PDataFrame;
use std::io::Write as IoWrite;

pub struct TableDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> DataSeriesRefsOutput for TableDfOutput<Dest> {
    fn output(&mut self, df: &PDataFrame) -> EvalResult<()> {
        write!(self.0, "{:?}", df)?;
        Ok(())
    }
}
