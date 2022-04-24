pub mod json;
pub mod parquet;
pub mod table;

pub use json::*;
pub use parquet::*;
pub use table::*;

use crate::tsdb::query::executor::Result as ExecuteResult;
use crate::tsdb::query::lexer::OutputFormat;
use polars::prelude::DataFrame as PDataFrame;
use std::io::Write as IoWrite;

pub trait PolarsConvatibleDataFrameOutput {
    fn output(&mut self, data: &mut PDataFrame) -> ExecuteResult<()>;
}

pub fn new_data_series_refs_vec_output<'d, Dest>(
    format: &OutputFormat,
    output_dest: Dest,
) -> Box<dyn PolarsConvatibleDataFrameOutput + 'd>
where
    Dest: 'd + IoWrite,
{
    match format {
        OutputFormat::Json => Box::new(JsonDfOutput(output_dest)),
        OutputFormat::DataFrame => Box::new(TableDfOutput(output_dest)),
        OutputFormat::Parquet => Box::new(ParquetDfOutput(output_dest)),
    }
}

macro_rules! output_with_condition {
    ($output_condition:expr, $df:expr) => {{
        match $output_condition.output_wirter()? {
            crate::tsdb::lexer::OutputWriter::Stdout => {
                let out = std::io::stdout();
                let out = std::io::BufWriter::new(out.lock());
                let mut destination =
                    crate::tsdb::executor::output::new_data_series_refs_vec_output(
                        &$output_condition.output_format,
                        out,
                    );
                destination.output(&mut $df)?;
            }
            crate::tsdb::lexer::OutputWriter::File(f) => {
                let out = std::io::BufWriter::new(f);
                let mut destination =
                    crate::tsdb::executor::output::new_data_series_refs_vec_output::<_>(
                        &$output_condition.output_format,
                        out,
                    );
                destination.output(&mut $df)?;
            }
        }
    }};
}

pub(crate) use output_with_condition;
