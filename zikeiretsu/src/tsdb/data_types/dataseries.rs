use super::dataframe::{DataframeError, Result as DataframeResult};
use super::field::*;
use crate::tsdb::util::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub enum SeriesValues {
    Vacant(usize),
    Float64(Vec<f64>),
    Bool(Vec<bool>),
}

impl SeriesValues {
    pub fn len(&self) -> usize {
        match self {
            Self::Float64(vs) => vs.len(),
            Self::Bool(vs) => vs.len(),
            Self::Vacant(len) => len,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Float64(vs) => vs.is_empty(),
            Self::Bool(vs) => vs.is_empty(),
            Self::Vacant(len) => len == 0,
        }
    }
}
impl std::fmt::Display for SeriesValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Float64(vs) => "[f64]",
            Self::Bool(vs) => "[bool]",
            Self::Vacant => "[vacant]",
        };

        write!(f, "{}", s)
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DataSeries {
    pub values: SeriesValues,
}

impl DataSeries {
    pub fn new(values: SeriesValues) -> Self {
        Self { values }
    }

    pub fn merge(&mut self, other: &mut DataSeries) -> DataframeResult<()> {
        match &mut self.values {
            SeriesValues::Float64(vs) => match other.values {
                SeriesValues::Float64(other_vals) => {
                    vs.append(&mut other_vals);
                    Ok(())
                }
                invalid => Err(DataframeError::UnmatchedSeriesTypeError(
                    self.values.to_string(),
                    invalid.to_string(),
                )),
            },
            SeriesValues::Bool(vs) => match other.values {
                SeriesValues::Bool(other_vals) => {
                    vs.append(&mut other_vals);
                    Ok(())
                }
                invalid => Err(DataframeError::UnmatchedSeriesTypeError(
                    self.values.to_string(),
                    invalid.to_string(),
                )),
            },

            SeriesValues::Vacant_ => { /* do nothing */ }
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&FieldValue> {
        match self.values {
            SeriesValues::Float64(vs) => vs.get(index).map(|v| &FieldValue::Float64(*v)),
            SeriesValues::Bool(vs) => vs.get(index).map(|v| &FieldValue::Bool(*v)),
            _ => None,
        }
    }

    pub fn retain(
        &mut self,
        retain_start_index: usize,
        cut_off_suffix_start_idx: usize,
    ) -> DataframeResult<()> {
        match self.values {
            SeriesValues::Float64(vs) => {
                trim_values(&mut vs, retain_start_index, cut_off_suffix_start_idx)?
            }

            SeriesValues::Bool(vs) => {
                trim_values(&mut vs, retain_start_index, cut_off_suffix_start_idx)?
            }

            _ => { /* do nothing */ }
        }

        Ok(())
    }
}

//impl From<DataSeriesRef<'_>> for DataSeries {
//    fn from(ds: DataSeriesRef<'_>) -> DataSeries {
//        DataSeries::new(ds.values.into_iter().map(|e| e.clone()).collect())
//    }
//}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub enum SeriesValuesRef<'a> {
    Float64(&'a [f64]),
    Bool(&'a [bool]),
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct DataSeriesRef<'a> {
    pub values: SeriesValuesRef<'a>,
}

impl<'a> DataSeriesRef<'a> {
    pub fn new(values: SeriesValuesRef<'a>) -> Self {
        Self { values }
    }

    pub fn get(&self, index: usize) -> Option<&FieldValue> {
        match self.values {
            SeriesValuesRef::Float64(vs) => vs.get(index).map(|v| &FieldValue::Float64(*v)),
            SeriesValuesRef::Bool(vs) => vs.get(index).map(|v| &FieldValue::Bool(*v)),
        }
    }
}
