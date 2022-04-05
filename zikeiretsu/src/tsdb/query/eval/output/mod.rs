pub mod json;
pub mod table;

pub use json::*;
pub use table::*;

use super::Result as EvalResult;
use crate::tsdb::query::lexer::OutputFormat;
use crate::tsdb::DataSeriesRefs;
use async_trait::async_trait;
use polars::prelude::{DataFrame as PDataFrame, Series as PSeries, *};
use std::io::Write as IoWrite;
use std::marker::PhantomData;

use chrono::FixedOffset;

pub trait DataSeriesRefsOutput {
    fn output(&mut self, data: &PDataFrame) -> EvalResult<()>;
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
                destination.output(&$df)?;
            }
            OutputWriter::File(f) => {
                let out = std::io::BufWriter::new(f);
                let mut destination =
                    new_data_series_refs_vec_output::<_>(&$output_condition.output_format, out);
                destination.output(&$df)?;
            }
        }
    }};
}

pub(crate) use output_with_condition;
