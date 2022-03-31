use super::dataseries::*;
use super::field::*;
use super::{datapoint::DataPoint, polars::zdata_frame_to_dataframe, DatapointSearchCondition};
use crate::tsdb::datetime::*;
use crate::tsdb::util::{trim_values, VecOpeError};
use chrono::FixedOffset;
use polars::prelude::{DataFrame as PDataFrame, PolarsError};

use std::cmp::Ordering;

use crate::tsdb::search::*;
use serde::{Deserialize, Serialize};
use thiserror::*;
use tokio::task::JoinError;

pub type Result<T> = std::result::Result<T, DataframeError>;

#[derive(Error, Debug)]
pub enum DataframeError {
    #[error(" data series index out of bound data seriese index:{0}, data index:{1}")]
    DataSeriesIndexOutOfBound(usize, usize),

    #[error("unsorted dataframe. {0}")]
    UnsortedDataframe(String),

    #[error("unmatched number of column names . field of df:{0}, columns:{1}")]
    UnmatchedColumnNameNumber(usize, usize),

    #[error("vec ope error. {0}")]
    VecOpeError(#[from] VecOpeError),

    #[error("join error. {0}")]
    JoinError(#[from] JoinError),

    #[error("attempt to merge unmatched series type error. {0}, {1}")]
    UnmatchedSeriesTypeError(String, String),

    #[error("polars error. {0}")]
    PolarsError(#[from] PolarsError),
}

pub trait DataSeriesSeq {
    fn data_serieses(&self) -> &[DataSeries];
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DataFrame {
    pub timestamp_nanos: Vec<TimestampNano>,
    pub data_serieses: Vec<DataSeries>,
}

//impl DataSeriesSeq for DataFrame {
//    fn data_serieses(self) -> &[DataSeries] {
//        DataSeries
//        SeriesValues ::TimestampNano::
//        self.data_serieses.as_slice()
//    }
//}

impl DataFrame {
    pub fn new(timestamp_nanos: Vec<TimestampNano>, data_serieses: Vec<DataSeries>) -> Self {
        Self {
            timestamp_nanos,
            data_serieses,
        }
    }

    pub async fn as_polars_dataframe(
        &self,
        column_names: Option<&[&str]>,
        timezone: &FixedOffset,
    ) -> Result<PDataFrame> {
        zdata_frame_to_dataframe(&self, column_names, timezone).await
    }

    pub fn merge(&mut self, other: &mut DataFrame) -> Result<()> {
        self.timestamp_nanos.append(&mut other.timestamp_nanos);
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
        self.timestamp_nanos.len()
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

    pub async fn search<'a>(&'a self, cond: &DatapointSearchCondition) -> Option<DataFrameRef<'a>> {
        self.search_with_indices(cond)
            .await
            .map(|(dataframes, _indices)| dataframes)
    }

    pub async fn retain<'a>(&mut self, cond: &DatapointSearchCondition) -> Result<()> {
        match self.search_with_indices(cond).await {
            None => {
                self.timestamp_nanos.clear();
                self.data_serieses.clear();
                Ok(())
            }
            Some((_, indices)) => {
                let (start, end) = indices;

                trim_values(&mut self.timestamp_nanos, start, end + 1)?;

                for each_series in self.data_serieses.iter_mut() {
                    each_series.retain(start, end + 1)?;
                }
                Ok(())
            }
        }
    }

    pub fn into_datapoints(self) -> Result<Vec<DataPoint>> {
        let mut result = Vec::<DataPoint>::new();
        for (idx, ts) in self.timestamp_nanos.into_iter().enumerate() {
            let mut field_values = Vec::<FieldValue>::new();
            for (ds_idx, each_dataseries) in self.data_serieses.iter().enumerate() {
                match each_dataseries.get(idx) {
                    Some(data_series_value) => field_values.push(data_series_value.clone()),
                    None => return Err(DataframeError::DataSeriesIndexOutOfBound(ds_idx, idx)),
                }
            }

            result.push(DataPoint::new(ts, field_values))
        }

        Ok(result)
    }

    pub async fn search_with_indices<'a>(
        &'a self,
        cond: &DatapointSearchCondition,
    ) -> Option<(DataFrameRef<'a>, (usize, usize))> {
        let since_eq_cond = cond
            .inner_since_eq
            .map(|since| move |ts: &TimestampNano| ts.cmp(&since));

        let until_neq_cond = cond
            .inner_until_neq
            .map(|until| move |ts: &TimestampNano| ts.cmp(&until));

        match binary_search_range_with_idx_by(&self.timestamp_nanos, since_eq_cond, until_neq_cond)
        {
            None => None,
            Some((tss, (start_idx, finish_idx))) => {
                let selected_series = DataFrameRef::new(
                    tss,
                    self.data_serieses
                        .iter()
                        .map(|series| match &series.values {
                            SeriesValues::Vacant(_) => {
                                DataSeriesRef::new(SeriesValuesRef::Vacant(finish_idx - start_idx))
                            }
                            SeriesValues::Float64(vs) => DataSeriesRef::new(
                                SeriesValuesRef::Float64(&vs[start_idx..finish_idx + 1]),
                            ),

                            SeriesValues::String(vs) => DataSeriesRef::new(
                                SeriesValuesRef::String(&vs[start_idx..finish_idx + 1]),
                            ),

                            SeriesValues::TimestampNano(vs) => DataSeriesRef::new(
                                SeriesValuesRef::TimestampNano(&vs[start_idx..finish_idx + 1]),
                            ),

                            SeriesValues::Bool(vs) => DataSeriesRef::new(SeriesValuesRef::Bool(
                                &vs[start_idx..finish_idx + 1],
                            )),
                        })
                        .collect(),
                );
                Some((selected_series, (start_idx, finish_idx)))
            }
        }
    }

    pub(crate) fn check_dataframe_is_sorted(dataframe: &DataFrame) -> Result<()> {
        if dataframe.is_empty() {
            Ok(())
        } else {
            let mut prev = unsafe { dataframe.timestamp_nanos.get_unchecked(0) };
            for each in dataframe.timestamp_nanos[1..].iter() {
                if each.cmp(&prev) == Ordering::Less {
                    return Err(DataframeError::UnsortedDataframe(format!(
                        "{:?}, {:?}",
                        each, prev
                    )));
                }
                prev = each
            }

            Ok(())
        }
    }
}

impl From<DataFrameRef<'_>> for DataFrame {
    fn from(df: DataFrameRef<'_>) -> DataFrame {
        DataFrame::new(
            df.timestamp_nanos.to_vec(),
            df.data_serieses.into_iter().map(|e| e.into()).collect(),
        )
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

    pub fn len(&self) -> usize {
        self.timestamp_nanos.len()
    }

    pub fn into_datapoints(self) -> Result<Vec<DataPoint>> {
        let mut result = Vec::<DataPoint>::new();
        for (idx, ts) in self.timestamp_nanos.into_iter().enumerate() {
            let mut field_values = Vec::<FieldValue>::new();
            for (ds_idx, each_dataseries) in self.data_serieses.iter().enumerate() {
                match each_dataseries.get(idx) {
                    Some(data_series_value) => field_values.push(data_series_value.clone()),
                    None => return Err(DataframeError::DataSeriesIndexOutOfBound(ds_idx, idx)),
                }
            }

            result.push(DataPoint::new(*ts, field_values))
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    macro_rules! dataframe {
        ($ts:expr) => {{
            let mut timestamp_nanos = Vec::<TimestampNano>::new();
            let mut values = Vec::<f64>::new();
            for (ts, val) in $ts {
                timestamp_nanos.push(TimestampNano::new(ts));
                values.push(val as f64);
            }

            DataFrame::new(
                timestamp_nanos,
                vec![DataSeries::new(SeriesValues::Float64(values))],
            )
        }};
    }

    macro_rules! ts {
        ($v:expr) => {
            TimestampNano::new($v)
        };
    }

    #[tokio::test]
    async fn dataframe_binsearch_test_1() {
        let mut df = dataframe!([
            (9, 1),
            (10, 2),
            (19, 3),
            (20, 4),
            (20, 5),
            (20, 6),
            (30, 7),
            (40, 8),
            (50, 9),
            (50, 10),
            (51, 11)
        ]);
        let condition = DatapointSearchCondition::since(ts!(20)).with_until(ts!(50));
        let result = df.search(&condition).await;
        assert!(result.is_some());
        let result = result.unwrap();

        let result: DataFrame = result.into();
        assert_eq!(
            result,
            dataframe!([(20, 4), (20, 5), (20, 6), (30, 7), (40, 8),])
        );

        assert!(df.retain(&condition).await.is_ok());
        assert_eq!(
            df,
            dataframe!([(20, 4), (20, 5), (20, 6), (30, 7), (40, 8),])
        );
    }

    #[tokio::test]
    async fn dataframe_binsearch_test_2() {
        let mut df = dataframe!([
            (9, 1),
            (10, 2),
            (19, 3),
            (20, 4),
            (20, 5),
            (20, 6),
            (30, 7),
            (40, 8),
            (50, 9),
            (50, 10),
            (51, 11)
        ]);

        let condition = DatapointSearchCondition::since(ts!(20));
        let result = df.search(&condition).await;
        assert!(result.is_some());
        let result = result.unwrap();

        let result: DataFrame = result.into();
        assert_eq!(
            result,
            dataframe!([
                (20, 4),
                (20, 5),
                (20, 6),
                (30, 7),
                (40, 8),
                (50, 9),
                (50, 10),
                (51, 11)
            ])
        );

        assert!(df.retain(&condition).await.is_ok());
        assert_eq!(
            df,
            dataframe!([
                (20, 4),
                (20, 5),
                (20, 6),
                (30, 7),
                (40, 8),
                (50, 9),
                (50, 10),
                (51, 11)
            ])
        );
    }

    #[tokio::test]
    async fn dataframe_binsearch_test_3() {
        let mut df = dataframe!([
            (9, 1),
            (10, 2),
            (19, 3),
            (20, 4),
            (20, 5),
            (20, 6),
            (30, 7),
            (40, 8),
            (50, 9),
            (50, 10),
            (51, 11)
        ]);

        let condition = DatapointSearchCondition::until(ts!(40));
        let result = df.search(&condition).await;
        assert!(result.is_some());
        let result = result.unwrap();

        let result: DataFrame = result.into();
        assert_eq!(
            result,
            dataframe!([(9, 1), (10, 2), (19, 3), (20, 4), (20, 5), (20, 6), (30, 7),])
        );

        assert!(df.retain(&condition).await.is_ok());
        assert_eq!(
            df,
            dataframe!([(9, 1), (10, 2), (19, 3), (20, 4), (20, 5), (20, 6), (30, 7),])
        );
    }
}
