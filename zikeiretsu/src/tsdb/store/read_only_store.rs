use super::*;

use crate::tsdb::{datapoint::*, datapoints_searcher::*};

pub struct RefReadonlyStore<'datapoint> {
    datapoints: &'datapoint [DataPoint],
}

impl<'datapoint> RefReadonlyStore<'datapoint> {
    pub fn new(datapoints: &'datapoint [DataPoint], validate: bool) -> Result<Self> {
        if validate {
            if let Err(e) = DataPoint::check_datapoints_is_sorted(&datapoints) {
                return Err(StoreError::UnsortedDatapoints(e));
            }
        }
        Ok(Self { datapoints })
    }

    pub async fn datapoints_searcher<'a>(&'a self) -> DatapointSearcher<'a> {
        DatapointSearcher::new(&self.datapoints)
    }
}

pub struct ReadonlyStore {
    datapoints: Vec<DataPoint>,
}

impl ReadonlyStore {
    pub fn new(datapoints: Vec<DataPoint>, validate: bool) -> Result<Self> {
        if validate {
            if let Err(e) = DataPoint::check_datapoints_is_sorted(&datapoints) {
                return Err(StoreError::UnsortedDatapoints(e));
            }
        }
        Ok(Self { datapoints })
    }

    pub fn all_datapoints(&self) -> &[DataPoint] {
        &self.datapoints
    }

    pub fn searcher<'a>(&'a self) -> DatapointSearcher<'a> {
        DatapointSearcher::new(&self.datapoints)
    }
}
