use super::ArrowDataFrameOutput;
use crate::tsdb::query::executor::Result as ExecuteResult;
use arrow::record_batch::RecordBatch;
use polars::prelude::{DataFrame as PDataFrame, ParquetWriter};
use std::io::Write as IoWrite;

pub struct ParquetDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> ArrowDataFrameOutput for ParquetDfOutput<Dest> {
    fn output(&mut self, df: &[RecordBatch]) -> ExecuteResult<()> {
        ParquetWriter::new(&mut self.0)
            .with_statistics(true)
            .finish(df)?;

        Ok(())
    }
}
