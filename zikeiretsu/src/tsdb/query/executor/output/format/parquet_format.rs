use crate::tsdb::query::executor::Result as ExecuteResult;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;

pub struct ParquetOutput {
    pub file: File,
    pub snappy_compress: bool,
}

impl ParquetOutput {
    pub fn output(self, record: RecordBatch) -> ExecuteResult<()> {
        let props = if self.snappy_compress {
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build()
        } else {
            WriterProperties::builder().build()
        };

        let mut writer = ArrowWriter::try_new(self.file, record.schema(), Some(props)).unwrap();

        writer.write(&record)?;
        writer.close()?;
        Ok(())
    }
}
