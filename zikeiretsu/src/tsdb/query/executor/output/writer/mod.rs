use super::super::Result;
use super::format::*;
use crate::tsdb::query::lexer::OutputFormat;
use crate::OutputCondition;
use arrow::record_batch::RecordBatch;

pub async fn output_records(
    record_batch: RecordBatch,
    output_condition: OutputCondition,
) -> Result<()> {
    match output_condition.output_wirter()? {
        crate::tsdb::lexer::OutputWriter::Stdout => {
            let out = std::io::stdout();
            let out = std::io::BufWriter::new(out.lock());

            let mut destination: Box<dyn ArrowDataFrameOutput> =
                match &output_condition.output_format {
                    OutputFormat::Json => Box::new(JsonDfOutput(out)),
                    OutputFormat::Table => Box::new(TableDfOutput(out)),
                    r => panic!("inalid output format for stdout. this should be a bug. {r:?}"),
                };

            destination.output(record_batch)?;
        }
        crate::tsdb::lexer::OutputWriter::File(file) => {
            let mut destination: Box<dyn ArrowDataFrameOutput> =
                match &output_condition.output_format {
                    OutputFormat::Json => {
                        let out = std::io::BufWriter::new(file);
                        Box::new(JsonDfOutput(out))
                    }
                    OutputFormat::Table => {
                        let out = std::io::BufWriter::new(file);
                        Box::new(TableDfOutput(out))
                    }
                    OutputFormat::Parquet => {
                        ParquetOutput {
                            file,
                            snappy_compress: false,
                        }
                        .output(record_batch)?;
                        return Ok(());
                    }

                    OutputFormat::ParquetSnappy => {
                        ParquetOutput {
                            file,
                            snappy_compress: true,
                        }
                        .output(record_batch)?;
                        return Ok(());
                    }
                };

            destination.output(record_batch)?;
        }
    }

    Ok(())
}
