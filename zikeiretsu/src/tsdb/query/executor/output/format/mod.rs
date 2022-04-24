pub mod json_format;
pub mod parquet_format;
pub mod table_format;

pub use json_format::*;
pub use parquet_format::*;
pub use table_format::*;

use crate::tsdb::query::executor::Result as ExecuteResult;

use arrow::record_batch::RecordBatch;

pub trait ArrowDataFrameOutput {
    fn output(&mut self, record_batch: RecordBatch) -> ExecuteResult<()>;
}
