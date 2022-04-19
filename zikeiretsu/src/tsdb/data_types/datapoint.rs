use super::field::*;
use crate::tsdb::datetime::*;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;

use crate::tsdb::search::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
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

    pub fn get_field(&self, field_idx: usize) -> Option<&FieldValue> {
        self.field_values.get(field_idx)
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
            .inner_since_inclusive
            .map(|since| move |datapoint: &DataPoint| datapoint.timestamp_nano.cmp(&since));

        let until_cond = cond
            .inner_until_exclusive
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

#[derive(Debug, PartialEq, Clone)]
pub struct DatapointSearchCondition {
    pub inner_since_inclusive: Option<TimestampNano>,
    pub inner_until_exclusive: Option<TimestampNano>,
}

impl DatapointSearchCondition {
    pub fn new(
        inner_since_inclusive: Option<TimestampNano>,
        inner_until_exclusive: Option<TimestampNano>,
    ) -> Self {
        Self {
            inner_since_inclusive,
            inner_until_exclusive,
        }
    }

    pub fn all() -> Self {
        Self {
            inner_since_inclusive: None,
            inner_until_exclusive: None,
        }
    }

    pub fn as_ref(&self) -> (Option<&TimestampNano>, Option<&TimestampNano>) {
        (self.inner_since_inclusive.as_ref(), self.inner_until_exclusive.as_ref())
    }

    pub fn as_secs(&self) -> (Option<TimestampSec>, Option<TimestampSec>) {
        (
            self.inner_since_inclusive.map(|i| i.as_timestamp_sec()),
            self.inner_until_exclusive.map(|i| i.as_timestamp_sec()),
        )
    }

    pub fn since(since: TimestampNano) -> Self {
        Self {
            inner_since_inclusive: Some(since),
            inner_until_exclusive: None,
        }
    }

    pub fn until(until: TimestampNano) -> Self {
        Self {
            inner_since_inclusive: None,
            inner_until_exclusive: Some(until),
        }
    }

    pub fn with_since(mut self, since: TimestampNano) -> Self {
        self.inner_since_inclusive = Some(since);
        self
    }

    pub fn with_until(mut self, until: TimestampNano) -> Self {
        self.inner_until_exclusive = Some(until);
        self
    }

    pub fn contains_whole(&self, since: &TimestampNano, until: &TimestampNano) -> bool {
        if let Some(since_inclusive) = self.inner_since_inclusive {
            if since < &since_inclusive {
                return false;
            }
        }

        if let Some(until_exclusive) = self.inner_until_exclusive {
            if until >= &until_exclusive {
                return false;
            }
        }
        true
    }

    pub fn from_str_opts(
        since: Option<&String>,
        until: Option<&String>,
    ) -> Result<Self, chrono::ParseError> {
        let inner_since = match since {
            Some(since) => Some(TimestampNano::try_from(since.as_ref())?),
            None => None,
        };

        let inner_until = match until {
            Some(until) => Some(TimestampNano::try_from(until.as_ref())?),
            None => None,
        };

        Ok(DatapointSearchCondition {
            inner_since_inclusive: inner_since,
            inner_until_exclusive: inner_until,
        })
    }
}

impl fmt::Display for DatapointSearchCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({:?}, {:?})",
            self.inner_since_inclusive.map(|s| s.to_string()),
            self.inner_until_exclusive.map(|s| s.to_string()),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_contain_whole() {
        assert!(DatapointSearchCondition::all()
            .contains_whole(&TimestampNano::new(10), &TimestampNano::new(20)));

        assert!(!DatapointSearchCondition::new(
            Some(TimestampNano::new(10)),
            Some(TimestampNano::new(20))
        )
        .contains_whole(&TimestampNano::new(10), &TimestampNano::new(20)));

        assert!(
            DatapointSearchCondition::new(Some(TimestampNano::new(10)), None)
                .contains_whole(&TimestampNano::new(10), &TimestampNano::new(20))
        );

        assert!(
            DatapointSearchCondition::new(Some(TimestampNano::new(10)), None)
                .contains_whole(&TimestampNano::new(10), &TimestampNano::new(20))
        );

        assert!(DatapointSearchCondition::new(
            Some(TimestampNano::new(10)),
            Some(TimestampNano::new(21))
        )
        .contains_whole(&TimestampNano::new(10), &TimestampNano::new(20)));

        assert!(!DatapointSearchCondition::new(
            Some(TimestampNano::new(10)),
            Some(TimestampNano::new(21))
        )
        .contains_whole(&TimestampNano::new(9), &TimestampNano::new(20)));

        assert!(
            DatapointSearchCondition::new(None, Some(TimestampNano::new(21)))
                .contains_whole(&TimestampNano::new(9), &TimestampNano::new(20))
        );
    }
}
