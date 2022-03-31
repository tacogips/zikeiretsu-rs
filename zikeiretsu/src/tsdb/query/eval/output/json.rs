use super::{DataFrameOutput, EvalResult};
use crate::tsdb::DataFrame as ZDataFrame;
use crate::tsdb::DataSeriesRefs;
use async_trait::async_trait;
use std::io::Write as IoWrite;
use std::marker::PhantomData;

pub struct JsonDfOutput<Dest: IoWrite + Send + Sync, DSR: Send + Sync>(
    pub Dest,
    pub PhantomData<DSR>,
);

#[async_trait]
impl<Dest: IoWrite + Send + Sync, DSR: DataSeriesRefs + Send + Sync> DataFrameOutput
    for JsonDfOutput<Dest, DSR>
{
    type Data = DSR;
    async fn output(
        &mut self,
        data: Self::Data,
        column_names: Option<&[&str]>,
        timezone: &chrono::FixedOffset,
    ) -> EvalResult<()> {
        //TODO(tacogips)
        unimplemented!()
    }
}
