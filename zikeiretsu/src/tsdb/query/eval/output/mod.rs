pub mod json;
pub mod table;

pub use super::Result as EvalResult;

use crate::tsdb::DataFrame;
use async_trait::async_trait;

use chrono::FixedOffset;
#[async_trait]
pub trait Output {
    async fn output(
        &mut self,
        data: DataFrame,
        column_names: Option<&[&str]>,
        timezone: FixedOffset,
    ) -> EvalResult<()>;
}
