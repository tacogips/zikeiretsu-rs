use super::PolarsConvatibleDataFrameOutput;
use crate::tsdb::query::executor::Result as ExecuteResult;
use polars::prelude::DataFrame as PDataFrame;
use std::io::Write as IoWrite;

pub struct TableDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> PolarsConvatibleDataFrameOutput for TableDfOutput<Dest> {
    fn output(&mut self, df: &mut PDataFrame) -> ExecuteResult<()> {
        write!(self.0, "{}", df)?;
        Ok(())
    }
}
