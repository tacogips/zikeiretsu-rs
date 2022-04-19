use super::{DataSeriesRefsOutput, EvalResult};
use polars::prelude::{DataFrame as PDataFrame, ParquetWriter};
use std::io::Write as IoWrite;

pub struct TableDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> DataSeriesRefsOutput for TableDfOutput<Dest> {
    fn output(&mut self, df: &mut PDataFrame) -> EvalResult<()> {
        ParquetWriter::new(self.0)
            .with_statistics(true)
            .finish(df)?;

        write!(self.0, "{}", df)?;
        Ok(())
    }
}
