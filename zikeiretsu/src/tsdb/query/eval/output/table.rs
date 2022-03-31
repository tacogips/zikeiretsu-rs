use super::{DataFrameOutput, EvalResult};
use crate::tsdb::DataFrame as ZDataFrame;
use async_trait::async_trait;
use chrono::FixedOffset;
use std::io::Write as IoWrite;

pub struct TableDfOutput<Dest: IoWrite + Send + Sync>(pub Dest);

#[async_trait]
impl<Dest: IoWrite + Send + Sync> DataFrameOutput for TableDfOutput<Dest> {
    async fn output(
        &mut self,
        data: ZDataFrame,
        column_names: Option<&[&str]>,
        timezone: chrono::FixedOffset,
    ) -> EvalResult<()> {
        let df = data.as_polars_dataframe(column_names, &timezone).await?;
        write!(self.0, "{:?}", df)?;
        Ok(())
    }
}
