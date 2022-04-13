use super::dataframe::{DataframeError, Result};
use super::dataseries::*;
use super::dataseries_ref::*;
use super::field::*;
use super::{datapoint::DataPoint, DatapointSearchCondition};
use crate::tsdb::datetime::*;
use crate::tsdb::util::{prepend, trim_values};

use std::cmp::Ordering;

use crate::tsdb::search::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct TimeSeriesDataFrame {
    pub timestamp_nanos: Vec<TimestampNano>,
    pub columns: Vec<DataSeries>,
}

impl TimeSeriesDataFrame {
    pub fn new(timestamp_nanos: Vec<TimestampNano>, data_serieses: Vec<DataSeries>) -> Self {
        Self {
            timestamp_nanos,
            columns: data_serieses,
        }
    }

    pub fn empty() -> Self {
        Self {
            timestamp_nanos: vec![],
            columns: vec![],
        }
    }

    pub fn insert(
        &mut self,
        timestamp_nano_to_insert: TimestampNano,
        row_to_insert: Vec<FieldValue>,
    ) -> Result<()> {
        let compare_timestamps = |each_ts: &TimestampNano| each_ts.cmp(&timestamp_nano_to_insert);

        let column_len = self.column_len();
        let new_row_column_len = row_to_insert.len();
        match binary_search_by(
            &self.timestamp_nanos,
            compare_timestamps,
            BinaryRangeSearchType::AtLeastEq,
        ) {
            Some(target_index) => {
                self.timestamp_nanos
                    .insert(target_index, timestamp_nano_to_insert);
                for (column_idx, each_column_series) in self.columns.iter_mut().enumerate() {
                    match row_to_insert.get(column_idx) {
                        Some(field_to_insert) => {
                            each_column_series.insert(target_index, &field_to_insert)?
                        }
                        None => {
                            return Err(DataframeError::UnmatchedFieldNumError(
                                column_len,
                                new_row_column_len,
                            ))
                        }
                    }
                }
            }

            None => {
                // push to the tail
                self.timestamp_nanos.push(timestamp_nano_to_insert);
                for (column_idx, each_column_series) in self.columns.iter_mut().enumerate() {
                    match row_to_insert.get(column_idx) {
                        Some(field_to_insert) => each_column_series.push(&field_to_insert)?,
                        None => {
                            return Err(DataframeError::UnmatchedFieldNumError(
                                column_len,
                                new_row_column_len,
                            ))
                        }
                    }
                }

                //TODO()
            }
        };

        Ok(())
    }

    pub fn prepend(&mut self, other: &mut TimeSeriesDataFrame) -> Result<()> {
        prepend(&mut self.timestamp_nanos, &mut other.timestamp_nanos);
        for (idx, data_series) in self.columns.iter_mut().enumerate() {
            match other.get_series_mut(idx) {
                Some(other_series) => data_series.prepend(other_series)?,
                None => return Err(DataframeError::DataSeriesIndexOutOfBound(idx, 0)),
            }
        }
        Ok(())
    }

    pub fn append(&mut self, other: &mut TimeSeriesDataFrame) -> Result<()> {
        self.timestamp_nanos.append(&mut other.timestamp_nanos);
        for (idx, data_series) in self.columns.iter_mut().enumerate() {
            match other.get_series_mut(idx) {
                Some(other_series) => data_series.append(other_series)?,
                None => return Err(DataframeError::DataSeriesIndexOutOfBound(idx, 0)),
            }
        }
        Ok(())
    }

    pub async fn merge(&mut self, other: &mut TimeSeriesDataFrame) -> Result<()> {
        if self.is_empty() {
            return self.append(other);
        }

        let fist_timestamp = self.timestamp_nanos.first().unwrap();
        let last_timestamp = self.timestamp_nanos.last().unwrap();
        let self_time_range = DatapointSearchCondition::new(
            Some(fist_timestamp.clone()),
            Some(last_timestamp.clone()),
        );
        let (mut prefix_data_frames, mut suffix_data_frames) =
            other.retain_matches(&self_time_range).await?;

        //  Insert rows into middle of self dataframes
        //  This may be inefficient process
        if !other.is_empty() {
            for row_idx_of_other in 0..other.len() {
                let (ts, field_values) = other.get_row(row_idx_of_other).unwrap();
                self.insert(ts.clone(), field_values)?;
            }
        }
        drop(other);
        if !suffix_data_frames.is_empty() {
            self.append(&mut suffix_data_frames)?;
        }

        if !prefix_data_frames.is_empty() {
            self.prepend(&mut prefix_data_frames)?;
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.timestamp_nanos.len()
    }

    pub fn column_len(&self) -> usize {
        self.columns.len()
    }

    pub fn get_series(&self, field_idx: usize) -> Option<&DataSeries> {
        self.columns.get(field_idx)
    }

    pub fn get_series_mut(&mut self, field_idx: usize) -> Option<&mut DataSeries> {
        self.columns.get_mut(field_idx)
    }

    pub fn get_row(&mut self, row_idx: usize) -> Option<(&TimestampNano, Vec<FieldValue>)> {
        match self.timestamp_nanos.get(row_idx) {
            None => None,
            Some(ts) => {
                let mut field_values = Vec::new();

                for (field_idx, each_field) in self.columns.iter().enumerate() {
                    match each_field.get(row_idx){
                        Some(field_value)=>field_values.push(field_value) ,
                        None=>panic!("TimeSeriesDataFrame has invalid status. Mismatch between timestamp num and datafield:{field_idx} num. ts_num:{ts_num} vs field_num:{field_num}",
                            field_idx = field_idx,
                            ts_num = self.timestamp_nanos.len(),
                            field_num =  each_field.len()),

                    }
                }
                Some((ts, field_values))
            }
        }
    }

    pub async fn search<'a>(
        &'a self,
        cond: &DatapointSearchCondition,
    ) -> Option<TimeSeriesDataFrameRef<'a>> {
        self.search_with_indices(cond)
            .await
            .map(|(dataframes, _indices)| dataframes)
    }

    pub async fn retain_matches<'a>(
        &mut self,
        cond: &DatapointSearchCondition,
    ) -> Result<(TimeSeriesDataFrame, TimeSeriesDataFrame)> {
        match self.search_with_indices(cond).await {
            None => {
                self.timestamp_nanos.clear();
                self.columns.clear();
                Ok((TimeSeriesDataFrame::empty(), TimeSeriesDataFrame::empty()))
            }
            Some((_, indices)) => {
                let (start, end) = indices;

                let (timestamps_prefix, timestamps_suffix) =
                    trim_values(&mut self.timestamp_nanos, start, end + 1)?;

                let mut prefix_data_serieses = Vec::<DataSeries>::new();
                let mut sufix_data_serieses = Vec::<DataSeries>::new();

                for each_series in self.columns.iter_mut() {
                    let (each_prefix_data_series, each_suffix_data_series) =
                        each_series.retain(start, end + 1)?;

                    prefix_data_serieses.push(each_prefix_data_series);
                    sufix_data_serieses.push(each_suffix_data_series);
                }
                Ok((
                    TimeSeriesDataFrame::new(timestamps_prefix, prefix_data_serieses),
                    TimeSeriesDataFrame::new(timestamps_suffix, sufix_data_serieses),
                ))
            }
        }
    }

    pub fn into_datapoints(self) -> Result<Vec<DataPoint>> {
        let mut result = Vec::<DataPoint>::new();
        for (idx, ts) in self.timestamp_nanos.into_iter().enumerate() {
            let mut field_values = Vec::<FieldValue>::new();
            for (ds_idx, each_dataseries) in self.columns.iter().enumerate() {
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
    ) -> Option<(TimeSeriesDataFrameRef<'a>, (usize, usize))> {
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
                let selected_series = TimeSeriesDataFrameRef::new(
                    tss,
                    self.columns
                        .iter()
                        .map(|series| match &series.values {
                            SeriesValues::Vacant(_) => {
                                DataSeriesRef::new(SeriesValuesRef::Vacant(finish_idx - start_idx))
                            }
                            SeriesValues::Float64(vs) => DataSeriesRef::new(
                                SeriesValuesRef::Float64(&vs[start_idx..finish_idx + 1]),
                            ),

                            SeriesValues::UInt64(vs) => DataSeriesRef::new(
                                SeriesValuesRef::UInt64(&vs[start_idx..finish_idx + 1]),
                            ),

                            SeriesValues::String(vs) => DataSeriesRef::new(
                                SeriesValuesRef::String(&vs[start_idx..finish_idx + 1]),
                            ),

                            SeriesValues::TimestampNano(vs) => DataSeriesRef::new(
                                SeriesValuesRef::TimestampNano(&vs[start_idx..finish_idx + 1]),
                            ),

                            SeriesValues::TimestampSec(vs) => DataSeriesRef::new(
                                SeriesValuesRef::TimestampSec(&vs[start_idx..finish_idx + 1]),
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

    pub(crate) fn check_dataframe_is_sorted(dataframe: &TimeSeriesDataFrame) -> Result<()> {
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

impl From<TimeSeriesDataFrameRef<'_>> for TimeSeriesDataFrame {
    fn from(df: TimeSeriesDataFrameRef<'_>) -> TimeSeriesDataFrame {
        TimeSeriesDataFrame::new(
            df.timestamp_nanos.to_vec(),
            df.data_serieses.into_iter().map(|e| e.into()).collect(),
        )
    }
}

impl DataSeriesRefs for TimeSeriesDataFrame {
    fn as_data_serieses_ref_vec<'a>(&'a self) -> Vec<DataSeriesRef<'a>> {
        let mut vs: Vec<DataSeriesRef<'_>> = self
            .columns
            .iter()
            .map(|ds| ds.as_dataseries_ref())
            .collect();

        let ts = DataSeriesRef::new(SeriesValuesRef::TimestampNano(&self.timestamp_nanos));
        vs.insert(0, ts);

        vs
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct TimeSeriesDataFrameRef<'a> {
    timestamp_nanos: &'a [TimestampNano],
    data_serieses: Vec<DataSeriesRef<'a>>,
}

impl<'a> TimeSeriesDataFrameRef<'a> {
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

            TimeSeriesDataFrame::new(
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

        let result: TimeSeriesDataFrame = result.into();
        assert_eq!(
            result,
            dataframe!([(20, 4), (20, 5), (20, 6), (30, 7), (40, 8),])
        );

        assert!(df.retain_matches(&condition).await.is_ok());
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

        let result: TimeSeriesDataFrame = result.into();
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

        assert!(df.retain_matches(&condition).await.is_ok());
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

        let result: TimeSeriesDataFrame = result.into();
        assert_eq!(
            result,
            dataframe!([(9, 1), (10, 2), (19, 3), (20, 4), (20, 5), (20, 6), (30, 7),])
        );

        assert!(df.retain_matches(&condition).await.is_ok());
        assert_eq!(
            df,
            dataframe!([(9, 1), (10, 2), (19, 3), (20, 4), (20, 5), (20, 6), (30, 7),])
        );
    }
}
