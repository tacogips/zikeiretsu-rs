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
    #[serde(rename = "ts")]
    pub timestamp_nanos: Vec<TimestampNano>,

    #[serde(rename = "columns")]
    pub columns: Vec<DataSeries>,

    #[serde(rename = "column_names")]
    column_names: Option<Vec<String>>,
}

impl TimeSeriesDataFrame {
    pub fn new(
        timestamp_nanos: Vec<TimestampNano>,
        data_serieses: Vec<DataSeries>,
        column_names: Option<Vec<String>>,
    ) -> Self {
        Self {
            timestamp_nanos,
            columns: data_serieses,
            column_names,
        }
    }

    pub fn set_column_names(&mut self, column_names: Option<Vec<String>>) {
        self.column_names = column_names;
    }

    pub fn empty() -> Self {
        Self {
            timestamp_nanos: vec![],
            columns: vec![],
            column_names: None,
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
            BinaryRangeSearchType::AtLeastInclusive,
        ) {
            Some(target_index) => {
                self.timestamp_nanos
                    .insert(target_index, timestamp_nano_to_insert);
                for (column_idx, each_column_series) in self.columns.iter_mut().enumerate() {
                    match row_to_insert.get(column_idx) {
                        Some(field_to_insert) => {
                            each_column_series.insert(target_index, field_to_insert)?
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
                        Some(column_to_insert) => each_column_series.push(column_to_insert)?,
                        None => {
                            return Err(DataframeError::UnmatchedFieldNumError(
                                column_len,
                                new_row_column_len,
                            ))
                        }
                    }
                }
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

        let first_timestamp = self.timestamp_nanos.first().unwrap();
        let last_timestamp = self.timestamp_nanos.last().unwrap();
        let self_time_range =
            DatapointSearchCondition::new(Some(*first_timestamp), Some(*last_timestamp));

        let (mut prefix_data_frames, mut suffix_data_frames) =
            other.retain_matches(&self_time_range).await?;

        //  Insert rows into middle of self dataframes
        //  This may be inefficient process
        if !other.is_empty() {
            for row_idx_of_other in 0..other.len() {
                let (ts, field_values) = other.get_row(row_idx_of_other).unwrap();
                self.insert(*ts, field_values)?;
            }
        }
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
        let (match_start_idx, match_end_index, retain_data) = match self
            .search_with_indices(cond)
            .await
        {
            None => {
                match cond.as_ref() {
                    (None, None) => {
                        //self  must be empty here
                        return Ok((TimeSeriesDataFrame::empty(), TimeSeriesDataFrame::empty()));
                    }

                    (None, Some(_)) => {
                        // all data toe suffix
                        (0, 0, false)
                    }

                    (Some(_), None) => {
                        // all data tobe prefix
                        (self.len(), self.len(), false)
                    }

                    (Some(cond_since), Some(_)) => {
                        let last_timestamp = self.timestamp_nanos.last().unwrap();
                        if cond_since > last_timestamp {
                            // all data tobe prefix
                            (self.len(), self.len(), false)
                        } else {
                            // all data tobe suffix
                            (0, 0, false)
                        }
                    }
                }
            }
            Some((_, indices)) => {
                let (start, end) = indices;

                (start, end, true)
            }
        };

        let (retain_start_index, cutoff_start_index) = if retain_data {
            (match_start_idx, match_end_index + 1)
        } else {
            (match_start_idx, match_end_index)
        };

        let (timestamps_prefix, timestamps_suffix) = trim_values(
            &mut self.timestamp_nanos,
            retain_start_index,
            cutoff_start_index,
        )?;

        let mut prefix_data_serieses = Vec::<DataSeries>::new();
        let mut suffix_data_serieses = Vec::<DataSeries>::new();

        for each_series in self.columns.iter_mut() {
            let (each_prefix_data_series, each_suffix_data_series) =
                each_series.retain(retain_start_index, cutoff_start_index)?;

            prefix_data_serieses.push(each_prefix_data_series);
            suffix_data_serieses.push(each_suffix_data_series);
        }
        Ok((
            TimeSeriesDataFrame::new(
                timestamps_prefix,
                prefix_data_serieses,
                self.column_names.clone(),
            ),
            TimeSeriesDataFrame::new(
                timestamps_suffix,
                suffix_data_serieses,
                self.column_names.clone(),
            ),
        ))
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
        let since_inclusive_cond = cond
            .inner_since_inclusive
            .map(|since| move |ts: &TimestampNano| ts.cmp(&since));

        let until_exclusive_cond = cond
            .inner_until_exclusive
            .map(|until| move |ts: &TimestampNano| ts.cmp(&until));

        match binary_search_range_with_idx_by(
            &self.timestamp_nanos,
            since_inclusive_cond,
            until_exclusive_cond,
        ) {
            None => None,
            Some((tss, (start_idx, finish_idx))) => {
                let selected_series = TimeSeriesDataFrameRef::new(
                    tss,
                    self.columns
                        .iter()
                        .map(|series| series.as_sub_dataseries(start_idx, finish_idx))
                        .collect(),
                    self.column_names
                        .as_ref()
                        .map(|column_names| column_names.as_slice()),
                );
                Some((selected_series, (start_idx, finish_idx)))
            }
        }
    }

    #[allow(dead_code)]
    pub fn check_dataframe_is_sorted(dataframe: &TimeSeriesDataFrame) -> Result<()> {
        if dataframe.is_empty() {
            Ok(())
        } else {
            let mut prev = unsafe { dataframe.timestamp_nanos.get_unchecked(0) };
            for each in dataframe.timestamp_nanos[1..].iter() {
                if each.cmp(prev) == Ordering::Less {
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
            df.column_names
                .map(|e| e.into_iter().map(|s| s.to_string()).collect()),
        )
    }
}

impl DataSeriesRefs for TimeSeriesDataFrame {
    fn column_names(&self) -> Option<&Vec<String>> {
        self.column_names.as_ref()
    }

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
    column_names: Option<&'a [String]>,
}

impl<'a> TimeSeriesDataFrameRef<'a> {
    pub fn new(
        timestamp_nanos: &'a [TimestampNano],
        data_serieses: Vec<DataSeriesRef<'a>>,
        column_names: Option<&'a [String]>,
    ) -> Self {
        Self {
            timestamp_nanos,
            data_serieses,
            column_names,
        }
    }

    pub fn len(&self) -> usize {
        self.timestamp_nanos.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_datapoints(self) -> Result<Vec<DataPoint>> {
        let mut result = Vec::<DataPoint>::new();
        for (idx, ts) in self.timestamp_nanos.iter().enumerate() {
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

    macro_rules! ts {
        ($v:expr) => {
            TimestampNano::new($v)
        };
    }
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
                None,
            )
        }};
    }

    macro_rules! multi_dataframe {
        ($ts:expr) => {{
            let mut timestamp_nanos = Vec::<TimestampNano>::new();
            let mut values1 = Vec::<f64>::new();
            let mut values2 = Vec::<bool>::new();
            for (ts, val1, val2) in $ts {
                timestamp_nanos.push(TimestampNano::new(ts));
                values1.push(val1 as f64);
                values2.push(val2 as bool);
            }

            TimeSeriesDataFrame::new(
                timestamp_nanos,
                vec![
                    DataSeries::new(SeriesValues::Float64(values1)),
                    DataSeries::new(SeriesValues::Bool(values2)),
                ],
                None,
            )
        }};
    }

    macro_rules! some_ts {
        ($v:expr) => {
            Some(TimestampNano::new($v))
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

    #[tokio::test]
    async fn dataframe_binsearch_test_4() {
        let df = dataframe!([
            (2, 22),
            (3, 33),
            (4, 44),
            (5, 55),
            (6, 66),
            (7, 77),
            (8, 88),
            (10, 1010)
        ]);
        let condition = DatapointSearchCondition::new(some_ts!(0), some_ts!(3));

        let result = df.search(&condition).await;
        assert!(result.is_some());
        let result = result.unwrap();

        let result: TimeSeriesDataFrame = result.into();
        assert_eq!(result, dataframe!([(2, 22)]));
    }

    #[tokio::test]
    async fn dataframe_merge_1() {
        let df_1 = dataframe!([(9, 1), (10, 2), (19, 3), (20, 4), (50, 10), (51, 11)]);

        let df_2 = dataframe!([(8, 1), (10, 2222), (11, 3), (52, 4), (53, 5)]);

        {
            let mut df_1_clone = df_1.clone();
            let mut df_2_clone = df_2.clone();
            let result = df_1_clone.merge(&mut df_2_clone).await;
            assert!(result.is_ok());

            let expected = dataframe!([
                (8, 1),
                (9, 1),
                (10, 2222),
                (10, 2),
                (11, 3),
                (19, 3),
                (20, 4),
                (50, 10),
                (51, 11),
                (52, 4),
                (53, 5)
            ]);

            assert_eq!(df_1_clone, expected);
        }

        {
            let mut df_1_clone = df_1.clone();
            let mut df_2_clone = df_2.clone();

            let result = df_2_clone.merge(&mut df_1_clone).await;
            assert!(result.is_ok());

            let expected = dataframe!([
                (8, 1),
                (9, 1),
                (10, 2),
                (10, 2222),
                (11, 3),
                (19, 3),
                (20, 4),
                (50, 10),
                (51, 11),
                (52, 4),
                (53, 5)
            ]);

            assert_eq!(df_2_clone, expected);
        }
    }

    #[tokio::test]
    async fn dataframe_merge_2() {
        let mut df_1 = dataframe!([(2, 22), (4, 44), (5, 55), (6, 66), (8, 88), (10, 1010)]);
        let mut df_2 = dataframe!([(9, 99)]);

        let result = df_1.merge(&mut df_2).await;
        assert!(result.is_ok());

        let expected = dataframe!([
            (2, 22),
            (4, 44),
            (5, 55),
            (6, 66),
            (8, 88),
            (9, 99),
            (10, 1010)
        ]);

        assert_eq!(df_1, expected);
    }

    #[tokio::test]
    async fn dataframe_merge_3() {
        let mut df_1 = dataframe!([(2, 22), (4, 44), (5, 55), (6, 66), (8, 88), (10, 1010)]);
        let mut df_2 = dataframe!([(1, 11), (11, 1111)]);

        let result = df_1.merge(&mut df_2).await;
        assert!(result.is_ok());

        let expected = dataframe!([
            (1, 11),
            (2, 22),
            (4, 44),
            (5, 55),
            (6, 66),
            (8, 88),
            (10, 1010),
            (11, 1111)
        ]);

        assert_eq!(df_1, expected);
    }

    #[tokio::test]
    async fn dataframe_merge_4() {
        let mut df_1 = dataframe!([(2, 22), (4, 44), (5, 55), (6, 66), (8, 88), (10, 1010)]);
        let mut df_2 = dataframe!([(12, 1212), (13, 1313)]);

        let result = df_1.merge(&mut df_2).await;
        assert!(result.is_ok());

        let expected = dataframe!([
            (2, 22),
            (4, 44),
            (5, 55),
            (6, 66),
            (8, 88),
            (10, 1010),
            (12, 1212),
            (13, 1313)
        ]);

        assert_eq!(df_1, expected);
    }

    #[tokio::test]
    async fn dataframe_merge_5() {
        let mut df_1 = dataframe!([(4, 44), (5, 55), (6, 66), (8, 88), (10, 1010)]);
        let mut df_2 = dataframe!([(2, 22), (12, 1212), (13, 1313)]);

        let result = df_1.merge(&mut df_2).await;
        assert!(result.is_ok());

        let expected = dataframe!([
            (2, 22),
            (4, 44),
            (5, 55),
            (6, 66),
            (8, 88),
            (10, 1010),
            (12, 1212),
            (13, 1313)
        ]);

        assert_eq!(df_1, expected);
    }

    #[tokio::test]
    async fn dataframe_merge_6_inefficient_though() {
        let mut df_1 = dataframe!([(4, 44), (5, 55), (6, 66), (8, 88), (10, 1010)]);
        let mut df_2 = dataframe!([(2, 22), (3, 33)]);

        let result = df_1.merge(&mut df_2).await;
        assert!(result.is_ok());

        let expected = dataframe!([
            (2, 22),
            (3, 33),
            (4, 44),
            (5, 55),
            (6, 66),
            (8, 88),
            (10, 1010),
        ]);

        assert_eq!(df_1, expected);
    }

    #[tokio::test]
    async fn dataframe_merge_7_inefficient_though_use_append_instead() {
        let mut df_1 = dataframe!([(4, 44), (5, 55), (6, 66), (8, 88), (10, 1010)]);
        let mut df_2 = dataframe!([(11, 1111), (12, 1212)]);

        let result = df_1.merge(&mut df_2).await;
        assert!(result.is_ok());

        let expected = dataframe!([
            (4, 44),
            (5, 55),
            (6, 66),
            (8, 88),
            (10, 1010),
            (11, 1111),
            (12, 1212),
        ]);

        assert_eq!(df_1, expected);
    }

    #[tokio::test]
    async fn dataframe_merge_multiple_columns_1() {
        let mut df_1 = multi_dataframe!([
            (4, 44, true),
            (5, 55, false),
            (6, 66, true),
            (8, 88, true),
            (10, 1010, true)
        ]);
        let mut df_2 = multi_dataframe!([(2, 22, true), (3, 33, false)]);

        let result = df_1.merge(&mut df_2).await;
        assert!(result.is_ok());

        let expected = multi_dataframe!([
            (2, 22, true),
            (3, 33, false),
            (4, 44, true),
            (5, 55, false),
            (6, 66, true),
            (8, 88, true),
            (10, 1010, true),
        ]);

        assert_eq!(df_1, expected);
    }

    #[tokio::test]
    async fn retain_matches_1() {
        let mut df = dataframe!([
            (2, 22),
            (3, 33),
            (4, 44),
            (5, 55),
            (6, 66),
            (7, 77),
            (8, 88),
            (10, 1010)
        ]);
        let cond = DatapointSearchCondition::new(some_ts!(4), some_ts!(8));
        let (prefix, suffix) = df.retain_matches(&cond).await.unwrap();

        assert_eq!(prefix, dataframe!([(2, 22), (3, 33)]));

        assert_eq!(df, dataframe!([(4, 44), (5, 55), (6, 66), (7, 77)]));

        assert_eq!(suffix, dataframe!([(8, 88), (10, 1010)]));
    }

    #[tokio::test]
    async fn retain_matches_2() {
        let mut df = dataframe!([
            (2, 22),
            (3, 33),
            (4, 44),
            (5, 55),
            (6, 66),
            (7, 77),
            (8, 88),
            (10, 1010)
        ]);

        let cond = DatapointSearchCondition::new(some_ts!(0), some_ts!(3));
        let (prefix, suffix) = df.retain_matches(&cond).await.unwrap();

        assert_eq!(
            prefix,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );

        assert_eq!(df, dataframe!([(2, 22)]));

        assert_eq!(
            suffix,
            dataframe!([
                (3, 33),
                (4, 44),
                (5, 55),
                (6, 66),
                (7, 77),
                (8, 88),
                (10, 1010)
            ])
        );
    }

    #[tokio::test]
    async fn retain_matches_3() {
        let mut df = dataframe!([
            (2, 22),
            (3, 33),
            (4, 44),
            (5, 55),
            (6, 66),
            (7, 77),
            (8, 88),
            (10, 1010)
        ]);
        let cond = DatapointSearchCondition::new(some_ts!(8), some_ts!(13));
        let (prefix, suffix) = df.retain_matches(&cond).await.unwrap();

        assert_eq!(
            prefix,
            dataframe!([(2, 22), (3, 33), (4, 44), (5, 55), (6, 66), (7, 77)])
        );

        assert_eq!(df, dataframe!([(8, 88), (10, 1010)]));

        assert_eq!(
            suffix,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );
    }

    #[tokio::test]
    async fn retain_matches_4() {
        let mut df = dataframe!([
            (2, 22),
            (3, 33),
            (4, 44),
            (5, 55),
            (6, 66),
            (7, 77),
            (8, 88),
            (10, 1010)
        ]);
        let cond = DatapointSearchCondition::new(some_ts!(11), None);
        let (prefix, suffix) = df.retain_matches(&cond).await.unwrap();

        assert_eq!(
            prefix,
            dataframe!([
                (2, 22),
                (3, 33),
                (4, 44),
                (5, 55),
                (6, 66),
                (7, 77),
                (8, 88),
                (10, 1010)
            ])
        );

        assert_eq!(
            df,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );

        assert_eq!(
            suffix,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );
    }

    #[tokio::test]
    async fn retain_matches_5() {
        let mut df = dataframe!([
            (2, 22),
            (3, 33),
            (4, 44),
            (5, 55),
            (6, 66),
            (7, 77),
            (8, 88),
            (10, 1010)
        ]);
        let cond = DatapointSearchCondition::new(None, some_ts!(2));
        let (prefix, suffix) = df.retain_matches(&cond).await.unwrap();

        assert_eq!(
            prefix,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );
        assert_eq!(
            suffix,
            dataframe!([
                (2, 22),
                (3, 33),
                (4, 44),
                (5, 55),
                (6, 66),
                (7, 77),
                (8, 88),
                (10, 1010)
            ])
        );

        assert_eq!(
            df,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );
    }

    #[tokio::test]
    async fn retain_matches_6() {
        let mut df = dataframe!([
            (2, 22),
            (3, 33),
            (4, 44),
            (5, 55),
            (6, 66),
            (7, 77),
            (8, 88),
            (10, 1010)
        ]);
        let cond = DatapointSearchCondition::new(some_ts!(1), some_ts!(2));
        let (prefix, suffix) = df.retain_matches(&cond).await.unwrap();

        assert_eq!(
            prefix,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );
        assert_eq!(
            suffix,
            dataframe!([
                (2, 22),
                (3, 33),
                (4, 44),
                (5, 55),
                (6, 66),
                (7, 77),
                (8, 88),
                (10, 1010)
            ])
        );

        assert_eq!(
            df,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );
    }

    #[tokio::test]
    async fn retain_matches_7() {
        let mut df = dataframe!([
            (2, 22),
            (3, 33),
            (4, 44),
            (5, 55),
            (6, 66),
            (7, 77),
            (8, 88),
            (10, 1010)
        ]);
        let cond = DatapointSearchCondition::new(some_ts!(11), some_ts!(12));
        let (prefix, suffix) = df.retain_matches(&cond).await.unwrap();

        assert_eq!(
            prefix,
            dataframe!([
                (2, 22),
                (3, 33),
                (4, 44),
                (5, 55),
                (6, 66),
                (7, 77),
                (8, 88),
                (10, 1010)
            ])
        );

        assert_eq!(
            df,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );

        assert_eq!(
            suffix,
            TimeSeriesDataFrame::new(
                vec![],
                vec![DataSeries::new(SeriesValues::Float64(vec![]))],
                None
            )
        );
    }

    #[tokio::test]
    async fn dataframe_serde_1() {
        use serde_json;
        let df = multi_dataframe!([
            (2, 22, true),
            (3, 33, false),
            (4, 44, true),
            (5, 55, false),
            (6, 66, true),
            (8, 88, true),
            (10, 1010, true),
        ]);
        let serilized = serde_json::to_string(&df).unwrap();
        println!("{serilized}");
        let serialized_df: TimeSeriesDataFrame = serde_json::from_str(&serilized).unwrap();

        assert_eq!(df, serialized_df);
    }
}
