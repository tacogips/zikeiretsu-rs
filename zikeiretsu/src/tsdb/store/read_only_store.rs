use super::*;

use crate::tsdb::{datapoint::*, datapoints_searcher::*, field::*};

pub struct RefReadonlyStore<'datapoint> {
    datapoints: &'datapoint [DataPoint],
}

impl<'datapoint> RefReadonlyStore<'datapoint> {
    pub fn new(
        field_types: Vec<FieldType>,
        datapoints: &'datapoint [DataPoint],
        validate: bool,
    ) -> Result<Self> {
        if validate {
            if let Err(e) = DataPoint::check_datapoints_is_sorted(&datapoints) {
                return Err(StoreError::UnsortedDatapoints(e));
            }
        }
        Ok(Self { datapoints })
    }

    async fn datapoints_searcher<'a>(&'a self) -> DatapointSearcher<'a> {
        DatapointSearcher::new(&self.datapoints)
    }
}

pub struct ReadonlyStore {
    datapoints: Vec<DataPoint>,
}

impl ReadonlyStore {
    pub fn new(
        field_types: Vec<FieldType>,
        datapoints: Vec<DataPoint>,
        validate: bool,
    ) -> Result<Self> {
        if validate {
            if let Err(e) = DataPoint::check_datapoints_is_sorted(&datapoints) {
                return Err(StoreError::UnsortedDatapoints(e));
            }
        }
        Ok(Self { datapoints })
    }

    async fn datapoints(&self) -> &[DataPoint] {
        &self.datapoints
    }

    async fn datapoints_searcher<'a>(&'a self) -> DatapointSearcher<'a> {
        DatapointSearcher::new(&self.datapoints)
    }
}
