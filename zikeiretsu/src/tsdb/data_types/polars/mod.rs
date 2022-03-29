use super::dataframe::{DataframeError, Result as DataFrameResult};
use crate::tsdb::TimestampNano;
use crate::tsdb::{DataFrame as ZDataFrame, DataSeries as ZDataSeries};
use futures::future::{join, join_all};
use polars::prelude::DataFrame as PDataFrame;
use polars::prelude::Series as PSeries;

pub async fn zdata_frame_to_dataframe(
    data_frame: ZDataFrame,
    column_names: Option<&[&str]>,
) -> DataFrameResult<PDataFrame> {
    //Vec<TimestampNano>

    let field_names: Vec<String> = match column_names {
        Some(column_names) => {
            if data_frame.fields_len() != column_names.len() {
                return Err(DataframeError::UnmatchedColumnNameNumber(
                    data_frame.fields_len(),
                    column_names.len(),
                ));
            }
            column_names.into_iter().map(|s| s.to_string()).collect()
        }
        None => (0..data_frame.fields_len())
            .into_iter()
            .map(|e| e.to_string())
            .collect(),
    };

    let tss = tokio::task::spawn(to_timestamp_series(data_frame.timestamp_nanos));
    let serieses = field_names
        .iter()
        .zip(data_frame.data_serieses)
        .map(|(field_name, each_series)| zdata_series_to_series(field_name, each_series));

    let serieses: Vec<PSeries> = join_all(serieses)
        .await
        .into_iter()
        .collect::<DataFrameResult<Vec<PSeries>>>()?;
    let tss: PSeries = tss.await??;

    unimplemented!()
}

pub async fn to_timestamp_series(series: Vec<TimestampNano>) -> DataFrameResult<PSeries> {
    unimplemented!()
}

pub async fn zdata_series_to_series(
    field_name: &str,
    series: ZDataSeries,
) -> DataFrameResult<PSeries> {
    unimplemented!()
}
