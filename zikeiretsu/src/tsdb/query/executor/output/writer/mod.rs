use super::super::{ExecuteResult, ExecuteResultData, Result};
use super::format::*;
use crate::tsdb::query::lexer::OutputFormat;
use crate::OutputCondition;
use arrow::record_batch::RecordBatch;
use std::rc::Rc;

pub async fn output_execute_result(result: ExecuteResult) -> Result<()> {
    if let Some(error_message) = result.error_message {
        eprintln!("{}", error_message);
        Ok(())
    } else {
        match result.data {
            Some(ExecuteResultData {
                records,
                output_condition,
            }) => match records {
                None => {
                    log::error!("empty result data")
                }
                Some(records) => output_records(records, output_condition).await?,
            },
            None => {
                log::error!("empty result data")
            }
        };
        Ok(())
    }
}

async fn output_records(
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
                    OutputFormat::DataFrame => Box::new(TableDfOutput(out)),
                    r => panic!("inalid output format for stdout. this should be a bug. {r:?}"),
                };

            destination.output(record_batch)?;
        }
        crate::tsdb::lexer::OutputWriter::File(f) => {
            let mut destination: Box<dyn ArrowDataFrameOutput> =
                match &output_condition.output_format {
                    OutputFormat::Json => {
                        let out = std::io::BufWriter::new(f);
                        Box::new(JsonDfOutput(out))
                    }
                    OutputFormat::DataFrame => {
                        let out = std::io::BufWriter::new(f);
                        Box::new(TableDfOutput(out))
                    }
                    OutputFormat::Parquet => {
                        ParquetDfOutput(f).output(record_batch)?;
                        return Ok(());
                    }
                };

            destination.output(record_batch)?;
        }
    }

    Ok(())
}
