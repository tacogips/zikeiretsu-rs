pub mod json;
pub mod parquet;
pub mod table;

pub use json::*;
pub use parquet::*;
pub use table::*;

use crate::tsdb::query::executor::Result as ExecuteResult;
use crate::tsdb::query::lexer::OutputFormat;
use std::io::Write as IoWrite;

use arrow::record_batch::RecordBatch;

pub trait ArrowDataFrameOutput {
    fn output(&mut self, data: &[RecordBatch]) -> ExecuteResult<()>;
}

pub fn new_data_series_refs_vec_output<'d, Dest>(
    format: &OutputFormat,
    output_dest: Dest,
) -> Box<dyn ArrowDataFrameOutput + 'd>
where
    Dest: 'd + IoWrite,
{
    match format {
        OutputFormat::Json => Box::new(JsonDfOutput(output_dest)),
        OutputFormat::DataFrame => Box::new(TableDfOutput(output_dest)),
        OutputFormat::Parquet => Box::new(ParquetDfOutput(output_dest)),
    }
}

////use arrow::datatypes::Ba;
//use arrow::record_batch::RecordBatch;
//use arrow::util::pretty_format_batches;

macro_rules! output_with_condition {
    ($output_condition:expr, $record_batch:expr) => {{
        match $output_condition.output_wirter()? {
            crate::tsdb::lexer::OutputWriter::Stdout => {
                let out = std::io::stdout();
                let out = std::io::BufWriter::new(out.lock());
                let mut destination =
                    crate::tsdb::executor::output::new_data_series_refs_vec_output(
                        &$output_condition.output_format,
                        out,
                    );
                destination.output(&[$record_batch])?;
            }
            crate::tsdb::lexer::OutputWriter::File(f) => {
                let out = std::io::BufWriter::new(f);
                let mut destination =
                    crate::tsdb::executor::output::new_data_series_refs_vec_output::<_>(
                        &$output_condition.output_format,
                        out,
                    );
                destination.output(&[$record_batch])?;
            }
        }
    }};
}

pub(crate) use output_with_condition;
