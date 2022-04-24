use super::field::*;
use crate::tsdb::datetime::*;
use chrono::prelude::*;
use chrono::FixedOffset;
//use polars::prelude::{DataType as PDataType, Series as PSeries};
use serde::Serialize;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, BooleanArray, Float64Array, Int32Array, Int32Builder, Int64Array,
    ListArray, NullArray, PrimitiveArray, StringArray, StructArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, TimeUnit};
//use arrow::datatypes::{DataType, Date64Type, Field, Time64NanosecondType, ToByteSlice};
use arrow::datatypes::Field;

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

    //    pub async fn as_polar_series(&self, field_name: &str, tz: Option<&FixedOffset>) -> PSeries {
    //        unimplemented!()
    //        //match &self.values {
    //        //    SeriesValuesRef::Float64(vs) => PSeries::new(field_name, vs),
    //        //    SeriesValuesRef::UInt64(vs) => PSeries::new(field_name, vs),
    //        //    SeriesValuesRef::Bool(vs) => PSeries::new(field_name, vs),
    //        //    SeriesValuesRef::Vacant(_) => PSeries::new_empty(field_name, &PDataType::Null),
    //        //    SeriesValuesRef::String(vs) => PSeries::new(field_name, vs),
    //        //    SeriesValuesRef::TimestampNano(timestamps) => PSeries::new(
    //        //        field_name,
    //        //        timestamps
    //        //            .iter()
    //        //            .map(|ts| ts.as_formated_datetime(tz))
    //        //            .collect::<Vec<String>>(),
    //        //    ),
    //
    //        //    SeriesValuesRef::TimestampSec(timestamps) => PSeries::new(
    //        //        field_name,
    //        //        timestamps
    //        //            .iter()
    //        //            .map(|ts| ts.as_formated_datetime(tz))
    //        //            .collect::<Vec<String>>(),
    //        //    ),
    //        //}
    //    }
    //
    pub async fn as_arrow_field(
        &self,
        field_name: &str,
        tz: Option<&FixedOffset>,
    ) -> (Field, ArrayRef) {
        match self.values {
            SeriesValuesRef::Float64(vs) => (
                Field::new(field_name, DataType::Float64, false),
                Arc::new(Float64Array::from(vs.to_vec())),
            ),
            SeriesValuesRef::UInt64(vs) => (
                Field::new(field_name, DataType::UInt64, false),
                Arc::new(UInt64Array::from(vs.to_vec())),
            ),
            SeriesValuesRef::Bool(vs) => (
                Field::new(field_name, DataType::Boolean, false),
                Arc::new(BooleanArray::from(vs.to_vec())),
            ),
            SeriesValuesRef::Vacant(num) => (
                Field::new(field_name, DataType::Null, true),
                Arc::new(NullArray::new(num)),
            ),
            SeriesValuesRef::String(vs) => (
                Field::new(field_name, DataType::Utf8, false),
                Arc::new(StringArray::from(vs.to_vec())),
            ),
            SeriesValuesRef::TimestampNano(timestamp_nanos) => (
                Field::new(field_name, DataType::Time64(TimeUnit::Nanosecond), false),
                Arc::new(TimestampNanosecondArray::from_vec(
                    timestamp_nanos
                        .iter()
                        .map(|each_ts| each_ts.as_i64())
                        .collect(),
                    tz.map(|tz| tz.to_string()),
                )),
            ),

            SeriesValuesRef::TimestampSec(timestamp_secs) => (
                Field::new(field_name, DataType::Time64(TimeUnit::Second), false),
                Arc::new(TimestampSecondArray::from_vec(
                    timestamp_secs
                        .iter()
                        .map(|each_ts| each_ts.as_i64())
                        .collect(),
                    tz.map(|tz| tz.to_string()),
                )),
            ),
        }
    }
}
