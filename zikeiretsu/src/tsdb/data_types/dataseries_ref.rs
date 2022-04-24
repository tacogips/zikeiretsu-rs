use super::field::*;
use crate::tsdb::datetime::*;
use chrono::FixedOffset;
use polars::prelude::{Series as PSeries, *};
use serde::Serialize;

#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum SeriesValuesRef<'a> {
    Vacant(usize),
    Float64(&'a [f64]),
    UInt64(&'a [u64]),
    Bool(&'a [bool]),
    String(&'a [String]),
    TimestampNano(&'a [TimestampNano]),
    TimestampSec(&'a [TimestampSec]),
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
                    .iter()
                    .map(|ts| ts.as_formated_datetime(tz))
                    .collect::<Vec<String>>(),
            ),

            SeriesValuesRef::TimestampSec(timestamps) => PSeries::new(
                field_name,
                timestamps
                    .iter()
                    .map(|ts| ts.as_formated_datetime(tz))
                    .collect::<Vec<String>>(),
            ),
        }
    }
}
