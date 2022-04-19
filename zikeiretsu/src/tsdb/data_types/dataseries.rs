use super::dataframe::{DataframeError, Result as DataframeResult};
use super::dataseries_ref::*;
use super::field::*;
use crate::tsdb::datetime::*;
use crate::tsdb::util::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub enum SeriesValues {
    Vacant(usize),
    TimestampNano(Vec<TimestampNano>),
    TimestampSec(Vec<TimestampSec>),
    String(Vec<String>),
    Float64(Vec<f64>),
    UInt64(Vec<u64>),
    Bool(Vec<bool>),
}

impl SeriesValues {
    pub fn len(&self) -> usize {
        match self {
            Self::Float64(vs) => vs.len(),
            Self::UInt64(vs) => vs.len(),
            Self::Bool(vs) => vs.len(),
            Self::String(vs) => vs.len(),
            Self::TimestampNano(vs) => vs.len(),
            Self::TimestampSec(vs) => vs.len(),
            Self::Vacant(len) => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Float64(vs) => vs.is_empty(),
            Self::UInt64(vs) => vs.is_empty(),
            Self::Bool(vs) => vs.is_empty(),
            Self::String(vs) => vs.is_empty(),
            Self::TimestampNano(vs) => vs.is_empty(),
            Self::TimestampSec(vs) => vs.is_empty(),
            Self::Vacant(len) => *len == 0,
        }
    }
}

impl std::fmt::Display for SeriesValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Float64(_) => "[f64]",
            Self::UInt64(_) => "[u64]",
            Self::Bool(_) => "[bool]",
            Self::String(_) => "[string]",
            Self::TimestampNano(_) => "[timestamp nano]",
            Self::TimestampSec(_) => "[timestamp sec]",
            Self::Vacant(_) => "[vacant]",
        };

        write!(f, "{}", s)
    }
}

macro_rules! retain_series {
    ($enum_value:expr,$vs:expr,$retain_start_index:expr, $cut_off_suffix_start_idx:expr) => {{
        let (droped_prefix, droped_suffix) =
            trim_values($vs, $retain_start_index, $cut_off_suffix_start_idx)?;

        Ok((
            DataSeries::new($enum_value(droped_prefix)),
            DataSeries::new($enum_value(droped_suffix)),
        ))
    }};
}

macro_rules! unmatch_series_error {
    ($self_value:expr,$other_value:expr) => {{
        Err(DataframeError::UnmatchedSeriesTypeError(
            $self_value.to_string(),
            $other_value.to_string(),
        ))
    }};
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DataSeries {
    pub values: SeriesValues,
}

impl DataSeries {
    pub fn new(values: SeriesValues) -> Self {
        Self { values }
    }

    pub fn as_sub_dataseries(&self, start_idx: usize, finish_idx: usize) -> DataSeriesRef {
        let data_range = start_idx..=finish_idx;
        match &self.values {
            SeriesValues::Vacant(_) => {
                DataSeriesRef::new(SeriesValuesRef::Vacant(finish_idx - start_idx))
            }
            SeriesValues::Float64(vs) => {
                DataSeriesRef::new(SeriesValuesRef::Float64(&vs[data_range.clone()]))
            }

            SeriesValues::UInt64(vs) => {
                DataSeriesRef::new(SeriesValuesRef::UInt64(&vs[data_range.clone()]))
            }

            SeriesValues::String(vs) => {
                DataSeriesRef::new(SeriesValuesRef::String(&vs[data_range.clone()]))
            }

            SeriesValues::TimestampNano(vs) => {
                DataSeriesRef::new(SeriesValuesRef::TimestampNano(&vs[data_range.clone()]))
            }

            SeriesValues::TimestampSec(vs) => {
                DataSeriesRef::new(SeriesValuesRef::TimestampSec(&vs[data_range.clone()]))
            }

            SeriesValues::Bool(vs) => {
                DataSeriesRef::new(SeriesValuesRef::Bool(&vs[data_range.clone()]))
            }
        }
    }

    pub fn insert(&mut self, index: usize, other: &FieldValue) -> DataframeResult<()> {
        match &mut self.values {
            SeriesValues::Float64(vs) => match other {
                FieldValue::Float64(other_value) => {
                    vs.insert(index, *other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },
            SeriesValues::UInt64(vs) => match other {
                FieldValue::UInt64(other_value) => {
                    vs.insert(index, *other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::Bool(vs) => match other {
                FieldValue::Bool(other_value) => {
                    vs.insert(index, *other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::String(vs) => match other {
                FieldValue::String(other_value) => {
                    vs.insert(index, other_value.clone());
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::TimestampNano(vs) => match other {
                FieldValue::TimestampNano(other_value) => {
                    vs.insert(index, *other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::TimestampSec(vs) => match other {
                FieldValue::TimestampSec(other_value) => {
                    vs.insert(index, *other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::Vacant(len) => {
                //TODO(tacogips) invalid hwre
                *len = *len + 1;
                Ok(())
            }
        }
    }

    pub fn push(&mut self, other: &FieldValue) -> DataframeResult<()> {
        match &mut self.values {
            SeriesValues::Float64(vs) => match other {
                FieldValue::Float64(other_value) => {
                    vs.push(*other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },
            SeriesValues::UInt64(vs) => match other {
                FieldValue::UInt64(other_value) => {
                    vs.push(*other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::Bool(vs) => match other {
                FieldValue::Bool(other_value) => {
                    vs.push(*other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::String(vs) => match other {
                FieldValue::String(other_value) => {
                    vs.push(other_value.clone());
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::TimestampNano(vs) => match other {
                FieldValue::TimestampNano(other_value) => {
                    vs.push(*other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::TimestampSec(vs) => match other {
                FieldValue::TimestampSec(other_value) => {
                    vs.push(*other_value);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::Vacant(len) => {
                //TODO(tacogips) invalid hwre
                *len = *len + 1;
                Ok(())
            }
        }
    }

    pub fn prepend(&mut self, other: &mut DataSeries) -> DataframeResult<()> {
        match &mut self.values {
            SeriesValues::Float64(vs) => match &mut other.values {
                SeriesValues::Float64(other_vals) => {
                    prepend(vs, other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },
            SeriesValues::UInt64(vs) => match &mut other.values {
                SeriesValues::UInt64(other_vals) => {
                    prepend(vs, other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::Bool(vs) => match &mut other.values {
                SeriesValues::Bool(other_vals) => {
                    prepend(vs, other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::String(vs) => match &mut other.values {
                SeriesValues::String(other_vals) => {
                    prepend(vs, other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::TimestampNano(vs) => match &mut other.values {
                SeriesValues::TimestampNano(other_vals) => {
                    prepend(vs, other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::TimestampSec(vs) => match &mut other.values {
                SeriesValues::TimestampSec(other_vals) => {
                    prepend(vs, other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::Vacant(len) => match &other.values {
                SeriesValues::Vacant(new_len) => {
                    *len = *len + *new_len;
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },
        }
    }

    pub fn append(&mut self, other: &mut DataSeries) -> DataframeResult<()> {
        match &mut self.values {
            SeriesValues::Float64(vs) => match &mut other.values {
                SeriesValues::Float64(other_vals) => {
                    vs.append(other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },
            SeriesValues::UInt64(vs) => match &mut other.values {
                SeriesValues::UInt64(other_vals) => {
                    vs.append(other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::Bool(vs) => match &mut other.values {
                SeriesValues::Bool(other_vals) => {
                    vs.append(other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::String(vs) => match &mut other.values {
                SeriesValues::String(other_vals) => {
                    vs.append(other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::TimestampNano(vs) => match &mut other.values {
                SeriesValues::TimestampNano(other_vals) => {
                    vs.append(other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::TimestampSec(vs) => match &mut other.values {
                SeriesValues::TimestampSec(other_vals) => {
                    vs.append(other_vals);
                    Ok(())
                }
                invalid => unmatch_series_error!(self.values, invalid),
            },

            SeriesValues::Vacant(_) => match &other.values {
                SeriesValues::Vacant(_) => Ok(()),
                invalid => unmatch_series_error!(self.values, invalid),
            },
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<FieldValue> {
        match &self.values {
            SeriesValues::Float64(vs) => vs.get(index).map(|v| FieldValue::Float64(*v)),
            SeriesValues::UInt64(vs) => vs.get(index).map(|v| FieldValue::UInt64(*v)),
            SeriesValues::Bool(vs) => vs.get(index).map(|v| FieldValue::Bool(*v)),
            SeriesValues::String(vs) => vs.get(index).map(|v| FieldValue::String(v.clone())),
            SeriesValues::TimestampNano(vs) => vs.get(index).map(|v| FieldValue::TimestampNano(*v)),
            SeriesValues::TimestampSec(vs) => vs.get(index).map(|v| FieldValue::TimestampSec(*v)),
            SeriesValues::Vacant(len) => {
                if index >= *len {
                    None
                } else {
                    Some(FieldValue::Vacant)
                }
            }
        }
    }

    pub fn retain(
        &mut self,
        retain_start_index: usize,
        cut_off_suffix_start_idx: usize,
    ) -> DataframeResult<(DataSeries, DataSeries)> {
        match &mut self.values {
            SeriesValues::Float64(vs) => retain_series!(
                SeriesValues::Float64,
                vs,
                retain_start_index,
                cut_off_suffix_start_idx
            ),

            SeriesValues::UInt64(vs) => retain_series!(
                SeriesValues::UInt64,
                vs,
                retain_start_index,
                cut_off_suffix_start_idx
            ),

            SeriesValues::Bool(vs) => retain_series!(
                SeriesValues::Bool,
                vs,
                retain_start_index,
                cut_off_suffix_start_idx
            ),

            SeriesValues::TimestampNano(vs) => retain_series!(
                SeriesValues::TimestampNano,
                vs,
                retain_start_index,
                cut_off_suffix_start_idx
            ),

            SeriesValues::TimestampSec(vs) => retain_series!(
                SeriesValues::TimestampSec,
                vs,
                retain_start_index,
                cut_off_suffix_start_idx
            ),

            SeriesValues::String(vs) => retain_series!(
                SeriesValues::String,
                vs,
                retain_start_index,
                cut_off_suffix_start_idx
            ),

            SeriesValues::Vacant(len) => {
                if retain_start_index > cut_off_suffix_start_idx {
                    return Err(DataframeError::VecOpeError(VecOpeError::InvalidRange(
                        retain_start_index,
                        cut_off_suffix_start_idx,
                    )));
                }

                if retain_start_index >= *len {
                    return Err(DataframeError::VecOpeError(VecOpeError::OutOfRange(
                        retain_start_index,
                    )));
                }

                Ok((
                    DataSeries::new(SeriesValues::Vacant(retain_start_index)),
                    DataSeries::new(SeriesValues::Vacant(*len - cut_off_suffix_start_idx - 1)),
                ))
            }
        }
    }

    pub fn as_dataseries_ref<'a>(&'a self) -> DataSeriesRef<'a> {
        let vs = match &self.values {
            SeriesValues::Float64(vs) => SeriesValuesRef::Float64(&vs),
            SeriesValues::UInt64(vs) => SeriesValuesRef::UInt64(&vs),
            SeriesValues::Bool(vs) => SeriesValuesRef::Bool(&vs),
            SeriesValues::String(vs) => SeriesValuesRef::String(&vs),
            SeriesValues::TimestampNano(vs) => SeriesValuesRef::TimestampNano(&vs),
            SeriesValues::TimestampSec(vs) => SeriesValuesRef::TimestampSec(&vs),
            SeriesValues::Vacant(len) => SeriesValuesRef::Vacant(*len),
        };

        DataSeriesRef::new(vs)
    }
}

impl From<SeriesValues> for DataSeries {
    fn from(vs: SeriesValues) -> Self {
        DataSeries::new(vs)
    }
}

impl From<DataSeriesRef<'_>> for DataSeries {
    fn from(ds: DataSeriesRef<'_>) -> DataSeries {
        let vs = match ds.values {
            SeriesValuesRef::Float64(vs) => SeriesValues::Float64(vs.to_vec()),
            SeriesValuesRef::UInt64(vs) => SeriesValues::UInt64(vs.to_vec()),
            SeriesValuesRef::Bool(vs) => SeriesValues::Bool(vs.to_vec()),
            SeriesValuesRef::String(vs) => SeriesValues::String(vs.to_vec()),
            SeriesValuesRef::TimestampNano(vs) => SeriesValues::TimestampNano(vs.to_vec()),
            SeriesValuesRef::TimestampSec(vs) => SeriesValues::TimestampSec(vs.to_vec()),
            SeriesValuesRef::Vacant(len) => SeriesValues::Vacant(len),
        };

        DataSeries::new(vs)
    }
}
