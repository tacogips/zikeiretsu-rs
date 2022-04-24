use super::ArrowDataFrameOutput;
use crate::tsdb::query::executor::Result as ExecuteResult;
use polars::prelude::DataFrame as PDataFrame;
use std::io::Write as IoWrite;

use arrow::record_batch::RecordBatch;
pub struct JsonDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> ArrowDataFrameOutput for JsonDfOutput<Dest> {
    fn output(&mut self, df: &[RecordBatch]) -> ExecuteResult<()> {
        write!(self.0, "{}", serde_json::to_string(&df)?)?;
        Ok(())
    }
}
