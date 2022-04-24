use super::ArrowDataFrameOutput;
use crate::tsdb::query::executor::Result as ExecuteResult;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
//use polars::prelude::{DataFrame as PDataFrame, ParquetWriter};
//use std::io::Write as IoWrite;
use std::fs::File;

pub struct ParquetDfOutput(pub File);

impl ArrowDataFrameOutput for ParquetDfOutput {
    fn output(&mut self, record: RecordBatch) -> ExecuteResult<()> {
        // Default writer properties
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(self.0, record.schema(), Some(props)).unwrap();

        writer.write(&record)?;
        writer.close()?;
        Ok(())
    }
}
