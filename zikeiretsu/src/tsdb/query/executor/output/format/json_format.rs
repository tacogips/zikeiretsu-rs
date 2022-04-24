use super::ArrowDataFrameOutput;
use crate::tsdb::query::executor::Result as ExecuteResult;
use arrow::json;
use arrow::record_batch::RecordBatch;
use std::io::Write as IoWrite;
pub struct JsonDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> ArrowDataFrameOutput for JsonDfOutput<Dest> {
    fn output(&mut self, record_batch: RecordBatch) -> ExecuteResult<()> {
        let json_objs: Vec<serde_json::Value> =
            json::writer::record_batches_to_json_rows(&[record_batch])?
                .into_iter()
                .map(|obj| serde_json::Value::Object(obj))
                .collect();

        write!(self.0, "{:?}", json_objs)?;
        Ok(())
    }
}
