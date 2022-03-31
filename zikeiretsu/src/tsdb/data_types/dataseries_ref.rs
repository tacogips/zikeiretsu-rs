use super::field::*;
use crate::tsdb::datetime::*;
use serde::Serialize;

#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum SeriesValuesRef<'a> {
    Vacant(usize),
    Float64(&'a [f64]),
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
}

pub trait DataSeriesRefs {
    fn as_data_serieses_ref_vec<'a>(&'a self) -> Vec<DataSeriesRef<'a>>;
}

impl<'a> DataSeriesRefs for Vec<&'a Vec<String>> {
    fn as_data_serieses_ref_vec(&self) -> Vec<DataSeriesRef<'a>> {
        let vs: Vec<DataSeriesRef<'_>> = self
            .iter()
            .map(|strs| DataSeriesRef::new(SeriesValuesRef::String(strs)))
            .collect();

        vs
    }
}
