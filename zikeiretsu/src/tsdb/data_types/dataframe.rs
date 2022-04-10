use super::dataseries::*;
use super::dataseries_ref::*;
use super::field::*;
use super::{datapoint::DataPoint, DatapointSearchCondition};
use crate::tsdb::datetime::*;
use crate::tsdb::util::{trim_values, VecOpeError};
use polars::prelude::PolarsError;

use std::cmp::Ordering;

use crate::tsdb::search::*;
use serde::{Deserialize, Serialize};
use thiserror::*;

#[derive(Error, Debug)]
pub enum DataframeError {
    #[error(" data series index out of bound data seriese index:{0}, data index:{1}")]
    DataSeriesIndexOutOfBound(usize, usize),

    #[error("unsorted dataframe. {0}")]
    UnsortedDataframe(String),

    #[error("vec ope error. {0}")]
    VecOpeError(#[from] VecOpeError),

    #[error("attempt to merge unmatched series type error. {0}, {1}")]
    UnmatchedSeriesTypeError(String, String),
    #[error("polars error. {0}")]
    PolarsError(#[from] PolarsError),
}

pub type Result<T> = std::result::Result<T, DataframeError>;

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DataFrame {
    pub data_serieses: Vec<DataSeries>,
}

impl DataFrame {
    pub fn new(data_serieses: Vec<DataSeries>) -> Self {
        Self { data_serieses }
    }

    pub fn merge(&mut self, other: &mut Self) -> Result<()> {
        for (idx, data_series) in self.data_serieses.iter_mut().enumerate() {
            match other.get_series_mut(idx) {
                Some(other_series) => data_series.merge(other_series)?,
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
