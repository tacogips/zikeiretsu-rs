pub mod json;
pub mod parquet;
pub mod table;

pub use json::*;
pub use table::*;

use super::Result as EvalResult;
use crate::tsdb::query::lexer::OutputFormat;
use polars::prelude::DataFrame as PDataFrame;
use std::io::Write as IoWrite;

pub trait DataSeriesRefsOutput {
    fn output(&mut self, data: &mut PDataFrame) -> EvalResult<()>;
}

pub fn new_data_series_refs_vec_output<'d, Dest>(
    format: &OutputFormat,
    output_dest: Dest,
) -> Box<dyn DataSeriesRefsOutput + 'd>
where
    Dest: 'd + IoWrite,
{
    match format {
        OutputFormat::Json => Box::new(JsonDfOutput(output_dest)),
        OutputFormat::DataFrame => Box::new(TableDfOutput(output_dest)),
    }
}

macro_rules! output_with_condition {
    ($output_condition:expr, $df:expr) => {{
        match $output_condition.output_wirter()? {
            OutputWriter::Stdout => {
                let out = std::io::stdout();
                let out = std::io::BufWriter::new(out.lock());
                let mut destination =
                    new_data_series_refs_vec_output(&$output_condition.output_format, out);
                destination.output(&mut $df)?;
            }
            OutputWriter::File(f) => {
                let out = std::io::BufWriter::new(f);
                let mut destination =
                    new_data_series_refs_vec_output::<_>(&$output_condition.output_format, out);
                destination.output(&mut $df)?;
            }
        }
    }};
}

pub(crate) use output_with_condition;
