use crate::tsdb::query::executor::Result as ExecuteResult;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;

pub struct ParquetDfOutput(pub File);

impl ParquetDfOutput {
    pub fn output(self, record: RecordBatch) -> ExecuteResult<()> {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(self.0, record.schema(), Some(props)).unwrap();

        writer.write(&record)?;
        writer.close()?;
        Ok(())
    }
}
