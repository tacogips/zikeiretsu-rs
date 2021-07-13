use super::field::*;
use super::timestamp_nano::*;

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

    pub async fn search(
        datapoints: &[DataPoint],
        cond: DatapointSearchCondition,
    ) -> Option<&[DataPoint]> {
        let since_cond = cond
            .inner_since
            .map(|since| move |datapoint: &DataPoint| datapoint.timestamp_nano.cmp(&since));

        let until_cond = cond
            .inner_until
            .map(|until| move |datapoint: &DataPoint| datapoint.timestamp_nano.cmp(&until));

        binary_search_range_by(&datapoints, since_cond, until_cond)
    }
}

pub struct DatapointSearchCondition {
    pub inner_since: Option<TimestampNano>,
    pub inner_until: Option<TimestampNano>,
}

impl DatapointSearchCondition {
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
