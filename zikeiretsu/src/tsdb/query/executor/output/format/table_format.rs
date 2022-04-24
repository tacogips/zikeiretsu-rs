use super::ArrowDataFrameOutput;
use crate::tsdb::query::executor::Result as ExecuteResult;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use std::io::Write as IoWrite;

pub struct TableDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> ArrowDataFrameOutput for TableDfOutput<Dest> {
    fn output(&mut self, record: RecordBatch) -> ExecuteResult<()> {
        write!(self.0, "{}", pretty_format_batches(&[record])?)?;
        Ok(())
    }
}
