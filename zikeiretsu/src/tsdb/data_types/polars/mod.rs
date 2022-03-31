use super::dataframe::{DataframeError, Result as DataFrameResult};
use super::dataseries::SeriesValues;
use crate::tsdb::TimestampNano;
use crate::tsdb::{data_types::FieldValue, DataFrame as ZDataFrame, DataSeries as ZDataSeries};
use chrono::FixedOffset;
use futures::future::join_all;
use polars::prelude::{DataFrame as PDataFrame, Series as PSeries, *};

pub async fn zdata_frame_to_dataframe(
    data_frame: &ZDataFrame,
    column_names: Option<&[&str]>,
    timezone: &FixedOffset,
) -> DataFrameResult<PDataFrame> {
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

    let serieses =
        field_names
            .iter()
            .zip(data_frame.data_serieses.iter())
            .map(|(field_name, each_series)| {
                zdata_series_to_series(field_name, &each_series, timezone)
            });

    let serieses: Vec<PSeries> = join_all(serieses)
        .await
        .into_iter()
        .collect::<DataFrameResult<Vec<PSeries>>>()?;
    Ok(PDataFrame::new(serieses)?)
}

pub async fn zdata_series_to_series(
    field_name: &str,
    series: &ZDataSeries,
    tz: &FixedOffset,
) -> DataFrameResult<PSeries> {
    match &series.values {
        SeriesValues::Float64(vs) => Ok(PSeries::new(field_name, vs)),
        SeriesValues::Bool(vs) => Ok(PSeries::new(field_name, vs)),
        SeriesValues::Vacant(_) => Ok(PSeries::new_empty(field_name, &DataType::Null)),
        SeriesValues::String(vs) => Ok(PSeries::new(field_name, vs)),
        SeriesValues::TimestampNano(timestamps) => Ok(PSeries::new(
            field_name,
            timestamps
                .into_iter()
                .map(|ts| ts.as_formated_datetime(tz))
                .collect::<Vec<String>>(),
        )),
    }
}
