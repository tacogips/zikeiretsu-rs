use super::field::*;
use crate::tsdb::datetime::*;
use chrono::FixedOffset;
use serde::Serialize;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, NullArray, StringArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow::datatypes::Field;
use arrow::datatypes::{DataType, TimeUnit};

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

    pub async fn as_arrow_field(
        &self,
        field_name: &str,
        format_timestamp: bool,
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
            SeriesValuesRef::TimestampNano(timestamp_nanos) => {
                if format_timestamp {
                    (
                        Field::new(field_name, DataType::Utf8, false),
                        Arc::new(StringArray::from(
                            timestamp_nanos
                                .iter()
                                .map(|each_ts| each_ts.as_formated_datetime(tz))
                                .collect::<Vec<String>>(),
                        )),
                    )
                } else {
                    (
                        Field::new(
                            field_name,
                            DataType::Timestamp(TimeUnit::Nanosecond, tz.map(|tz| tz.to_string())),
                            false,
                        ),
                        Arc::new(TimestampNanosecondArray::from_vec(
                            timestamp_nanos
                                .iter()
                                .map(|each_ts| each_ts.as_i64())
                                .collect(),
                            tz.map(|tz| tz.to_string()),
                        )),
                    )
                }
            }

            SeriesValuesRef::TimestampSec(timestamp_secs) => {
                if format_timestamp {
                    (
                        Field::new(field_name, DataType::Utf8, false),
                        Arc::new(StringArray::from(
                            timestamp_secs
                                .iter()
                                .map(|each_ts| each_ts.as_formated_datetime(tz))
                                .collect::<Vec<String>>(),
                        )),
                    )
                } else {
                    (
                        Field::new(
                            field_name,
                            DataType::Timestamp(TimeUnit::Second, tz.map(|tz| tz.to_string())),
                            false,
                        ),
                        Arc::new(TimestampSecondArray::from_vec(
                            timestamp_secs
                                .iter()
                                .map(|each_ts| each_ts.as_i64())
                                .collect(),
                            tz.map(|tz| tz.to_string()),
                        )),
                    )
                }
            }
        }
    }
}
