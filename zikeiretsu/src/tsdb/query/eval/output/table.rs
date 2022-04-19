use super::{DataSeriesRefsOutput, EvalResult};
use polars::prelude::DataFrame as PDataFrame;
use std::io::Write as IoWrite;

pub struct TableDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> DataSeriesRefsOutput for TableDfOutput<Dest> {
    fn output(&mut self, df: &mut PDataFrame) -> EvalResult<()> {
        write!(self.0, "{}", df)?;
        Ok(())
    }
}
