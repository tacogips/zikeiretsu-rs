use super::arrow_dataframe::*;
use super::dataseries::*;
use super::dataseries_ref::*;
use crate::tsdb::util::VecOpeError;

use serde::{Deserialize, Serialize};
use thiserror::*;

#[derive(Error, Debug)]
pub enum DataframeError {
    #[error("data series index out of bound data seriese index:{0}, data index:{1}")]
    DataSeriesIndexOutOfBound(usize, usize),

    #[error("unsorted dataframe. {0}")]
    UnsortedDataframe(String),

    #[error("vec ope error. {0}")]
    VecOpeError(#[from] VecOpeError),

    #[error("attempt to merge unmatched series type error. {0}, {1}")]
    UnmatchedSeriesTypeError(String, String),

    #[error("unmatched field number. This might be a by bug. {0}, {1}")]
    UnmatchedFieldNumError(usize, usize),
}

pub type Result<T> = std::result::Result<T, DataframeError>;

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DataFrame {
    pub data_serieses: Vec<DataSeries>,
    pub column_names: Option<Vec<String>>,
}

impl DataFrame {
    pub fn new(data_serieses: Vec<DataSeries>, column_names: Option<Vec<String>>) -> Self {
        Self {
            data_serieses,
            column_names,
        }
    }

    pub fn merge(&mut self, other: &mut Self) -> Result<()> {
        for (idx, data_series) in self.data_serieses.iter_mut().enumerate() {
            match other.get_series_mut(idx) {
                Some(other_series) => data_series.append(other_series)?,
                None => return Err(DataframeError::DataSeriesIndexOutOfBound(idx, 0)),
            }
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        if self.fields_len() == 0 {
            0
        } else {
            self.data_serieses[0].len()
        }
    }

    pub fn fields_len(&self) -> usize {
        self.data_serieses.len()
    }

    pub fn get_series(&self, field_idx: usize) -> Option<&DataSeries> {
        self.data_serieses.get(field_idx)
    }

    pub fn get_series_mut(&mut self, field_idx: usize) -> Option<&mut DataSeries> {
        self.data_serieses.get_mut(field_idx)
    }
}
impl Default for DataFrame {
    fn default() -> Self {
        Self::new(vec![], None)
    }
}

impl ArrowConvatibleDataFrame for DataFrame {
    fn as_data_serieses_ref_vec(&self) -> Vec<DataSeriesRef<'_>> {
        self.data_serieses
            .iter()
            .map(|ds| ds.as_dataseries_ref())
            .collect()
    }

    fn column_names(&self) -> Option<&Vec<String>> {
        self.column_names.as_ref()
    }
}
