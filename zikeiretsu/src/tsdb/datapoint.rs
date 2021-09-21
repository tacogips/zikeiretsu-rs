use super::field::*;
use super::timestamp_nano::*;
use super::timestamp_sec::*;
use std::cmp::Ordering;

use crate::tsdb::search::*;

#[derive(Debug, PartialEq, Clone)]
pub struct DataPoint {
    pub timestamp_nano: TimestampNano,
    pub field_values: Vec<FieldValue>,
}

impl DataPoint {
    pub fn new(timestamp_nano: TimestampNano, field_values: Vec<FieldValue>) -> Self {
        Self {
            timestamp_nano,
            field_values,
        }
    }

    pub fn filed_num(&self) -> usize {
        self.field_values.len()
    }

    pub async fn search<'a>(
        datapoints: &'a [DataPoint],
        cond: &DatapointSearchCondition,
    ) -> Option<&'a [DataPoint]> {
        Self::search_with_indices(datapoints, cond)
            .await
            .map(|(datapoints, _indices)| datapoints)
    }

    pub async fn search_with_indices<'a>(
        datapoints: &'a [DataPoint],
        cond: &DatapointSearchCondition,
    ) -> Option<(&'a [DataPoint], (usize, usize))> {
        let since_cond = cond
            .inner_since
            .map(|since| move |datapoint: &DataPoint| datapoint.timestamp_nano.cmp(&since));

        let until_cond = cond
            .inner_until
            .map(|until| move |datapoint: &DataPoint| datapoint.timestamp_nano.cmp(&until));

        binary_search_range_with_idx_by(&datapoints, since_cond, until_cond)
    }

    pub(crate) fn check_datapoints_is_sorted(datapoints: &[DataPoint]) -> Result<(), String> {
        if datapoints.is_empty() {
            Ok(())
        } else {
            let mut prev = unsafe { datapoints.get_unchecked(0) };
            for each in datapoints[1..].iter() {
                if each.timestamp_nano.cmp(&prev.timestamp_nano) == Ordering::Less {
                    return Err(format!(
                        "{:?}, {:?}",
                        each.timestamp_nano, prev.timestamp_nano
                    ));
                }
                prev = each
            }

            Ok(())
        }
    }
}

#[derive(Clone)]
pub struct DatapointSearchCondition {
    pub inner_since: Option<TimestampNano>,
    pub inner_until: Option<TimestampNano>,
}

impl DatapointSearchCondition {
    pub fn new(inner_since: Option<TimestampNano>, inner_until: Option<TimestampNano>) -> Self {
        Self {
            inner_since,
            inner_until,
        }
    }

    pub fn as_secs(&self) -> (Option<TimestampSec>, Option<TimestampSec>) {
        (
            self.inner_since.map(|i| i.as_timestamp_sec()),
            self.inner_until.map(|i| i.as_timestamp_sec()),
        )
    }

    pub fn since(since: TimestampNano) -> Self {
        Self {
            inner_since: Some(since),
            inner_until: None,
        }
    }

    pub fn until(until: TimestampNano) -> Self {
        Self {
            inner_since: None,
            inner_until: Some(until),
        }
    }

    pub fn with_since(mut self, since: TimestampNano) -> Self {
        self.inner_since = Some(since);
        self
    }

    pub fn with_until(mut self, until: TimestampNano) -> Self {
        self.inner_until = Some(until);
        self
    }
}
