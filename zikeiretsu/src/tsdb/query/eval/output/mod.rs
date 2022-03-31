pub mod json;
pub mod table;

pub use json::*;
pub use table::*;

use super::Result as EvalResult;
use crate::tsdb::query::lexer::OutputFormat;
use crate::tsdb::DataSeriesRefs;
use async_trait::async_trait;
use std::io::Write as IoWrite;
use std::marker::PhantomData;
use std::path::PathBuf;

use chrono::FixedOffset;
#[async_trait]
pub trait DataFrameOutput {
    type Data;
    async fn output(
        &mut self,
        data: Self::Data,
        column_names: Option<&[&str]>,
        timezone: &FixedOffset,
    ) -> EvalResult<()>;
}

pub fn new_dataframe_output<'d, Dest: 'd + IoWrite + Send + Sync, Data: 'd + Send + Sync>(
    format: OutputFormat,
    output_dest: Dest,
) -> Box<dyn DataFrameOutput<Data = Data> + 'd>
where
    Data: DataSeriesRefs,
{
    match format {
        OutputFormat::Json => Box::new(JsonDfOutput(output_dest, PhantomData)),
        OutputFormat::Table => Box::new(TableDfOutput(output_dest, PhantomData)),
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
//
//
//#[derive(Error, Debug)]
//pub enum OutputError {
//    #[error("io error: {0} ")]
//    IOError(#[from] std::io::Error),
//
//    #[error("invalid output formt: {0} ")]
//    InvalidOutputFormat(String),
//
//    #[error("invalid output destination: {0} ")]
//    InvalidOutputDestination(String),
//
//    #[error("invalid json: {0} ")]
//    SerdeJsonError(#[from] serde_json::Error),
//}
//
//type Result<T> = std::result::Result<T, OutputError>;
//
//pub enum OutputFormat {
//    Json,
//    Tsv,
//}
//
//pub struct OutputSetting {
//    pub format: OutputFormat,
//    pub destination: OutputDestination,
//}
//
//fn write_to_stdout<I: IntoIterator<Item = D>, D: std::fmt::Display>(datas: I) -> Result<()> {
//    let out = stdout();
//    let mut out = BufWriter::new(out.lock());
//
//    for each in datas {
//        writeln!(out, "{data}", data = each)?;
//    }
//    Ok(())
//}
//
//fn write_to_file<'a, I: IntoIterator<Item = D>, D: std::fmt::Display>(
//    p: &'a Path,
//    datas: I,
//) -> Result<()> {
//    let dest = File::create(p)?;
//    let mut dest = BufWriter::new(dest);
//
//    for each_data in datas {
//        dest.write(format!("{data}", data = each_data).as_bytes())?;
//    }
//
//    dest.flush()?;
//    Ok(())
//}
//
//pub enum OutputDestination {
//    Stdout,
//    File(PathBuf),
//}
