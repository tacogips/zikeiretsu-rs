use super::*;
use crate::tsdb::datapoint::*;
use std::marker::{Send, Sync};

pub trait DatapointSorter: Clone + Send + Sync {
    fn compare(&mut self, lhs: &DataPoint, rhs: &DataPoint) -> Ordering;
}

#[derive(Clone)]
pub struct DatapointDefaultSorter;

impl DatapointSorter for DatapointDefaultSorter {
    fn compare(&mut self, lhs: &DataPoint, rhs: &DataPoint) -> Ordering {
        lhs.timestamp_nano.cmp(&rhs.timestamp_nano)
    }
}
