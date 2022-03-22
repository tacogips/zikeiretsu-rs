use super::field::*;
use super::timestamp_nano::*;
use super::timestamp_sec::*;
use super::DatapointSearchCondition;
use std::cmp::Ordering;
use std::convert::TryFrom;

use crate::tsdb::search::*;
use serde::{Deserialize, Serialize};
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DataFrame {
    pub timestamp_nanos: Vec<TimestampNano>,
    pub data_serieses: Vec<DataSeries>,
}

impl DataFrame {
    pub fn new(timestamp_nanos: Vec<TimestampNano>, data_serieses: Vec<DataSeries>) -> Self {
        Self {
            timestamp_nanos,
            data_serieses,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.timestamp_nanos.len()
    }

    pub fn get_series(&self, field_idx: usize) -> Option<&DataSeries> {
        self.data_serieses.get(field_idx)
    }

    pub async fn search<'a>(&'a self, cond: &DatapointSearchCondition) -> Option<DataFrameRef<'a>> {
        self.search_with_indices(cond)
            .await
            .map(|(datapoints, _indices)| datapoints)
    }

    pub async fn search_with_indices<'a>(
        &'a self,
        cond: &DatapointSearchCondition,
    ) -> Option<(DataFrameRef<'a>, (usize, usize))> {
        let since_cond = cond
            .inner_since
            .map(|since| move |ts: &TimestampNano| ts.cmp(&since));

        let until_cond = cond
            .inner_until
            .map(|until| move |ts: &TimestampNano| ts.cmp(&until));

        match binary_search_range_with_idx_by(&self.timestamp_nanos, since_cond, until_cond) {
            None => None,
            Some((tss, (start_idx, finish_idx))) => {
                let selected_series = DataFrameRef::new(
                    tss,
                    self.data_serieses
                        .iter()
                        .map(|series| {
                            DataSeriesRef::new(&series.values.as_slice()[start_idx..finish_idx])
                        })
                        .collect(),
                );
                Some((selected_series, (start_idx, finish_idx)))
            }
        }
    }

    pub(crate) fn check_dataframe_is_sorted(dataframe: &DataFrame) -> Result<(), String> {
        if dataframe.is_empty() {
            Ok(())
        } else {
            let mut prev = unsafe { dataframe.timestamp_nanos.get_unchecked(0) };
            for each in dataframe.timestamp_nanos[1..].iter() {
                if each.cmp(&prev) == Ordering::Less {
                    return Err(format!("{:?}, {:?}", each, prev));
                }
                prev = each
            }

            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct DataFrameRef<'a> {
    timestamp_nanos: &'a [TimestampNano],
    data_serieses: Vec<DataSeriesRef<'a>>,
}

impl<'a> DataFrameRef<'a> {
    pub fn new(
        timestamp_nanos: &'a [TimestampNano],
        data_serieses: Vec<DataSeriesRef<'a>>,
    ) -> Self {
        Self {
            timestamp_nanos,
            data_serieses,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DataSeries {
    pub values: Vec<FieldValue>,
}

impl DataSeries {
    pub fn new(values: Vec<FieldValue>) -> Self {
        Self { values }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct DataSeriesRef<'a> {
    pub values: &'a [FieldValue],
}

impl<'a> DataSeriesRef<'a> {
    pub fn new(values: &'a [FieldValue]) -> Self {
        Self { values }
    }
}
