use super::dataframe::{DataframeError, Result as DataFrameResult};
use crate::tsdb::TimestampNano;
use crate::tsdb::{data_types::FieldValue, DataFrame as ZDataFrame, DataSeries as ZDataSeries};
use chrono::FixedOffset;
use futures::future::join_all;
use polars::prelude::{DataFrame as PDataFrame, Series as PSeries, *};

pub async fn zdata_frame_to_dataframe(
    data_frame: ZDataFrame,
    column_names: Option<&[&str]>,
    timezone: FixedOffset,
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

    let tss = tokio::task::spawn(async move {
        to_timestamp_series(data_frame.timestamp_nanos, timezone).await
    });
    let serieses = field_names
        .iter()
        .zip(data_frame.data_serieses)
        .map(|(field_name, each_series)| zdata_series_to_series(field_name, each_series));

    let mut datas: Vec<PSeries> = join_all(serieses)
        .await
        .into_iter()
        .collect::<DataFrameResult<Vec<PSeries>>>()?;
    let mut serieses: Vec<PSeries> = vec![tss.await??];
    serieses.append(&mut datas);
    Ok(PDataFrame::new(serieses)?)
}

pub async fn to_timestamp_series(
    timestamps: Vec<TimestampNano>,
    tz: FixedOffset,
) -> DataFrameResult<PSeries> {
    Ok(PSeries::new(
        "ts",
        timestamps
            .into_iter()
            .map(|ts| ts.as_formated_datetime(&tz))
            .collect::<Vec<String>>(),
    ))
}

pub async fn zdata_series_to_series(
    field_name: &str,
    series: ZDataSeries,
) -> DataFrameResult<PSeries> {
    if series.is_empty() {
        Ok(PSeries::new_empty(field_name, &DataType::Float64))
    } else {
        match series.values.get(0).unwrap() {
            FieldValue::Float64(_) => Ok(PSeries::new(
                field_name,
                series
                    .values
                    .into_iter()
                    .map(|each| each.as_f64().unwrap())
                    .collect::<Vec<f64>>(),
            )),
            FieldValue::Bool(_) => Ok(PSeries::new(
                field_name,
                series
                    .values
                    .into_iter()
                    .map(|each| each.as_bool().unwrap())
                    .collect::<Vec<bool>>(),
            )),
        }
    }
}
