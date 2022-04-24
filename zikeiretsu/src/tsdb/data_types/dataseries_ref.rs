use super::field::*;
use crate::tsdb::datetime::*;
use chrono::prelude::*;
use chrono::FixedOffset;
use polars::prelude::{DataType as PDataType, Series as PSeries, *};
use serde::Serialize;

use arrow::array::{
    Array, ArrayData, ArrayRef, BooleanArray, Float64Array, Int32Array, Int32Builder, Int64Array,
    ListArray, NullArray, PrimitiveArray, StringArray, StructArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Date64Type, Field, Time64NanosecondType, ToByteSlice};

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

pub enum ArrowSeries {
    UInt64(UInt64Array),
    Float64(Float64Array),
    Bool(BooleanArray),
    String(StringArray),
    TimestampNano(TimestampNanosecondArray),
    TimestampSec(TimestampSecondArray),
    Vacant(NullArray),
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
            SeriesValuesRef::Vacant(_) => PSeries::new_empty(field_name, &PDataType::Null),
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

    pub async fn as_arrow_field(self, tz: Option<&FixedOffset>) -> (Field, ArrayRef) {
        match self.values {
            SeriesValuesRef::Float64(vs) => ArrowSeries::Float64(Float64Array::from(vs.to_vec())),
            SeriesValuesRef::UInt64(vs) => ArrowSeries::UInt64(UInt64Array::from(vs.to_vec())),
            SeriesValuesRef::Bool(vs) => ArrowSeries::Bool(BooleanArray::from(vs.to_vec())),
            SeriesValuesRef::Vacant(num) => ArrowSeries::Vacant(NullArray::new(num)),
            SeriesValuesRef::String(vs) => ArrowSeries::String(StringArray::from(vs.to_vec())),
            SeriesValuesRef::TimestampNano(timestamp_nanos) => {
                ArrowSeries::TimestampNano(TimestampNanosecondArray::from_vec(
                    timestamp_nanos
                        .iter()
                        .map(|each_ts| each_ts.as_i64())
                        .collect(),
                    tz.map(|tz| tz.to_string()),
                ))
            }

            SeriesValuesRef::TimestampSec(timestamp_secs) => {
                ArrowSeries::TimestampSec(TimestampSecondArray::from_vec(
                    timestamp_secs
                        .iter()
                        .map(|each_ts| each_ts.as_i64())
                        .collect(),
                    tz.map(|tz| tz.to_string()),
                ))
            }
        }
    }
}
