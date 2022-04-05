use super::field::*;
use crate::tsdb::datetime::*;
use async_trait::async_trait;
use chrono::FixedOffset;
use futures::future::join_all;
use polars::prelude::{DataFrame as PDataFrame, Series as PSeries, *};
use serde::Serialize;
use thiserror::*;

#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum SeriesValuesRef<'a> {
    Vacant(usize),
    Float64(&'a [f64]),
    UInt64(&'a [u64]),
    Bool(&'a [bool]),
    String(&'a [String]),
    TimestampNano(&'a [TimestampNano]),
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct DataSeriesRef<'a> {
    pub values: SeriesValuesRef<'a>,
}

impl<'a> DataSeriesRef<'a> {
    pub fn new(values: SeriesValuesRef<'a>) -> Self {
        Self { values }
    }

    pub fn get(&self, index: usize) -> Option<FieldValue> {
        match &self.values {
            SeriesValuesRef::Float64(vs) => vs.get(index).map(|v| FieldValue::Float64(*v)),
            SeriesValuesRef::Bool(vs) => vs.get(index).map(|v| FieldValue::Bool(*v)),
            _ => None,
        }
    }

    pub async fn as_polar_series(&self, field_name: &str, tz: Option<&FixedOffset>) -> PSeries {
        match &self.values {
            SeriesValuesRef::Float64(vs) => PSeries::new(field_name, vs),
            SeriesValuesRef::UInt64(vs) => PSeries::new(field_name, vs),
            SeriesValuesRef::Bool(vs) => PSeries::new(field_name, vs),
            SeriesValuesRef::Vacant(_) => PSeries::new_empty(field_name, &DataType::Null),
            SeriesValuesRef::String(vs) => PSeries::new(field_name, vs),
            SeriesValuesRef::TimestampNano(timestamps) => PSeries::new(
                field_name,
                timestamps
                    .into_iter()
                    .map(|ts| ts.as_formated_datetime(tz))
                    .collect::<Vec<String>>(),
            ),
        }
    }
}

pub type Result<T> = std::result::Result<T, DataSeriesRefsError>;

#[derive(Error, Debug)]
pub enum DataSeriesRefsError {
    #[error("polars error :{0}")]
    PolarsError(#[from] PolarsError),

    #[error("unmatched number of column names . field of df:{0}, columns:{1}")]
    UnmatchedColumnNameNumber(usize, usize),
}

#[async_trait]
pub trait DataSeriesRefs {
    fn as_data_serieses_ref_vec<'a>(&'a self) -> Vec<DataSeriesRef<'a>>;

    async fn as_polar_dataframes(
        &self,
        column_names: Option<&[&str]>,
        timezone: Option<&FixedOffset>,
    ) -> Result<PDataFrame> {
        let data_series_vec = self.as_data_serieses_ref_vec();
        let field_names: Vec<String> = match column_names {
            Some(column_names) => {
                if data_series_vec.len() != column_names.len() {
                    return Err(DataSeriesRefsError::UnmatchedColumnNameNumber(
                        data_series_vec.len(),
                        column_names.len(),
                    ));
                }
                column_names.into_iter().map(|s| s.to_string()).collect()
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

pub type StringDataSeriesRefs<'a> = Vec<&'a Vec<String>>;

impl<'a> DataSeriesRefs for StringDataSeriesRefs<'a> {
    fn as_data_serieses_ref_vec(&self) -> Vec<DataSeriesRef<'a>> {
        let vs: Vec<DataSeriesRef<'_>> = self
            .iter()
            .map(|strs| DataSeriesRef::new(SeriesValuesRef::String(strs)))
            .collect();

        vs
    }
}
