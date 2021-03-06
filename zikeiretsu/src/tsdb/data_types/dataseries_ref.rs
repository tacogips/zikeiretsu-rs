use super::field::*;
use crate::tsdb::datetime::*;
use crate::tsdb::TimeZoneAndOffset;
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
        tz_and_offset: Option<&TimeZoneAndOffset>,
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
                                .map(|each_ts| {
                                    each_ts.as_formated_datetime(
                                        tz_and_offset.map(|e| e.offset).as_ref(),
                                    )
                                })
                                .collect::<Vec<String>>(),
                        )),
                    )
                } else {
                    let offset_nano_seconds = tz_and_offset
                        .map(|tz_and_offset| {
                            tz_and_offset.offset.local_minus_utc() as i64 * 1_000_000_000i64
                        })
                        .unwrap_or(0);
                    (
                        Field::new(
                            field_name,
                            DataType::Timestamp(
                                TimeUnit::Nanosecond,
                                tz_and_offset.map(|tz_and_offset| tz_and_offset.tz.to_string()),
                            ),
                            false,
                        ),
                        Arc::new(TimestampNanosecondArray::from_vec(
                            timestamp_nanos
                                .iter()
                                .map(|each_ts| each_ts.as_i64() + offset_nano_seconds)
                                .collect(),
                            tz_and_offset.map(|tz_and_offset| tz_and_offset.tz.to_string()),
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
                                .map(|each_ts| {
                                    each_ts.as_formated_datetime(
                                        tz_and_offset.map(|e| e.offset).as_ref(),
                                    )
                                })
                                .collect::<Vec<String>>(),
                        )),
                    )
                } else {
                    let offset_seconds = tz_and_offset
                        .map(|tz_and_offset| tz_and_offset.offset.local_minus_utc() as i64)
                        .unwrap_or(0);
                    (
                        Field::new(
                            field_name,
                            DataType::Timestamp(
                                TimeUnit::Second,
                                tz_and_offset.map(|tz_and_offset| tz_and_offset.tz.to_string()),
                            ),
                            false,
                        ),
                        Arc::new(TimestampSecondArray::from_vec(
                            timestamp_secs
                                .iter()
                                .map(|each_ts| each_ts.as_i64() + offset_seconds)
                                .collect(),
                            tz_and_offset.map(|tz_and_offset| tz_and_offset.tz.to_string()),
                        )),
                    )
                }
            }
        }
    }
}
