use super::DataSeriesRef;
use async_trait::async_trait;
use chrono::FixedOffset;
use futures::future::join_all;
use polars::prelude::{DataFrame as PDataFrame, Series as PSeries, *};
use thiserror::*;

pub type Result<T> = std::result::Result<T, PolarsConvatibleDataFrameError>;

#[derive(Error, Debug)]
pub enum PolarsConvatibleDataFrameError {
    #[error("polars error :{0}")]
    PolarsError(#[from] PolarsError),

    #[error("unmatched number of column names . field of df:{0}, columns:{1}")]
    UnmatchedColumnNameNumber(usize, usize),
}

#[async_trait]
pub trait PolarsConvatibleDataFrame {
    fn as_data_serieses_ref_vec(&self) -> Vec<DataSeriesRef<'_>>;
    fn column_names(&self) -> Option<&Vec<String>>;

    async fn as_polar_dataframes(&self, timezone: Option<&FixedOffset>) -> Result<PDataFrame> {
        let data_series_vec = self.as_data_serieses_ref_vec();
        let field_names: Vec<String> = match self.column_names() {
            Some(column_names) => {
                if data_series_vec.len() != column_names.len() {
                    return Err(PolarsConvatibleDataFrameError::UnmatchedColumnNameNumber(
                        data_series_vec.len(),
                        column_names.len(),
                    ));
                }
                column_names.iter().map(|name| name.to_string()).collect()
            }
            None => (0..data_series_vec.len())
                .into_iter()
                .map(|e| e.to_string())
                .collect(),
        };

        let serieses = field_names
            .iter()
            .zip(data_series_vec.iter())
            .map(|(field_name, each_series)| each_series.as_polar_series(field_name, timezone));

        let serieses = join_all(serieses)
            .await
            .into_iter()
            .collect::<Vec<PSeries>>();
        Ok(PDataFrame::new(serieses)?)
    }
}
