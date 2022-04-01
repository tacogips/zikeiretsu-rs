use super::{DataSeriesRefsOutput, EvalResult};
use crate::tsdb::DataSeriesRefs;
use async_trait::async_trait;
use chrono::FixedOffset;
use std::io::Write as IoWrite;
use std::marker::PhantomData;

pub struct JsonDfOutput<Dest: IoWrite + Send + Sync, DSR: Send + Sync>(
    pub Dest,
    pub PhantomData<DSR>,
);

#[async_trait]
impl<Dest: IoWrite + Send + Sync, DSR: DataSeriesRefs + Send + Sync> DataSeriesRefsOutput
    for JsonDfOutput<Dest, DSR>
{
    type Data = DSR;
    async fn output(
        &mut self,
        data: Self::Data,
        column_names: Option<&[&str]>,
        timezone: Option<&FixedOffset>,
    ) -> EvalResult<()> {
        let df = data.as_polar_dataframes(column_names, timezone).await?;

        write!(self.0, "{:?}", serde_json::to_string(&df)?)?;
        Ok(())
    }
}
