use super::{DataFrameOutput, EvalResult};
use crate::tsdb::DataFrame as ZDataFrame;
use async_trait::async_trait;
use std::io::Write as IoWrite;

pub struct JsonDfOutput<Dest: IoWrite + Send + Sync>(pub Dest);

#[async_trait]
impl<Dest: IoWrite + Send + Sync> DataFrameOutput for JsonDfOutput<Dest> {
    async fn output(
        &mut self,
        data: ZDataFrame,
        column_names: Option<&[&str]>,
        timezone: chrono::FixedOffset,
    ) -> EvalResult<()> {
        //TODO(tacogips)
        unimplemented!()
    }
}
