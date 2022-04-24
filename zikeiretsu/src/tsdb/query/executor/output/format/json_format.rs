use super::ArrowDataFrameOutput;
use crate::tsdb::query::executor::Result as ExecuteResult;
use arrow::json;
use arrow::record_batch::RecordBatch;
use std::io::Write as IoWrite;
pub struct JsonDfOutput<Dest: IoWrite>(pub Dest);

impl<Dest: IoWrite> ArrowDataFrameOutput for JsonDfOutput<Dest> {
    fn output(&mut self, record_batch: RecordBatch) -> ExecuteResult<()> {
        let mut value_map = serde_json::Map::new();
        let schema = record_batch.schema();
        let field_names = schema.fields().iter().map(|field| field.name());
        for (field, column_name) in record_batch.columns().into_iter().zip(field_names) {
            let field_array_value = json::writer::array_to_json_array(field)?;
            value_map.insert(
                column_name.to_string(),
                serde_json::Value::Array(field_array_value),
            );
        }

        write!(self.0, "{}", serde_json::Value::Object(value_map))?;
        Ok(())
    }
}
