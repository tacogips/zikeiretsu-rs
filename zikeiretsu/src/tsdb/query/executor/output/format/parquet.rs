use super::PolarsConvatibleDataFrameOutput;
use crate::tsdb::query::executor::Result as EvalResult;
use polars::prelude::{DataFrame as PDataFrame, ParquetWriter};
use std::io::Write as IoWrite;

pub struct ParquetDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> PolarsConvatibleDataFrameOutput for ParquetDfOutput<Dest> {
    fn output(&mut self, df: &mut PDataFrame) -> EvalResult<()> {
        ParquetWriter::new(&mut self.0)
            .with_statistics(true)
            .finish(df)?;

        Ok(())
    }
}
