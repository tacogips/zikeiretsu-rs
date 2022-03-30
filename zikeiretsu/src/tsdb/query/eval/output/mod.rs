pub mod json;
pub mod table;

pub use json::*;
pub use table::*;

use super::Result as EvalResult;
use crate::tsdb::query::lexer::OutputFormat;
use crate::tsdb::DataFrame;
use async_trait::async_trait;
use std::io::Write as IoWrite;
use std::path::PathBuf;

use chrono::FixedOffset;
#[async_trait]
pub trait DataFrameOutput {
    async fn output(
        &mut self,
        data: DataFrame,
        column_names: Option<&[&str]>,
        timezone: FixedOffset,
    ) -> EvalResult<()>;
}

pub fn new_dataframe_output<'d, Dest: 'd + IoWrite + Send + Sync>(
    format: OutputFormat,
    output_dest: Dest,
) -> Box<dyn DataFrameOutput + 'd> {
    match format {
        OutputFormat::Json => Box::new(JsonDfOutput(output_dest)),
        OutputFormat::Table => Box::new(TableDfOutput(output_dest)),
    }
}

//pub struct OutputCondition {
//    pub output_format: OutputFormat,
//    pub output_file_path: Option<PathBuf>,
//}
//
//pub struct InterpretedQueryCondition {
//    pub metrics: Metrics,
//    pub field_selectors: Option<Vec<usize>>,
//    pub search_condition: DatapointSearchCondition,
//    pub output_format: OutputFormat,
//    pub output_file_path: Option<PathBuf>,
//    pub timezone: FixedOffset,
//}
