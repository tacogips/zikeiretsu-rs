use super::*;

use crate::tsdb::{dataframe::*, datapoint::*, datapoints_searcher::*};

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
    dataframe: DataFrame,
}

impl ReadonlyStore {
    pub fn new(dataframe: DataFrame, validate: bool) -> Result<Self> {
        if validate {
            if let Err(e) = DataFrame::check_dataframe_is_sorted(&dataframe) {
                return Err(StoreError::UnsortedDataFrame(e.to_string()));
            }
        }
        Ok(Self { dataframe })
    }

    pub fn len(&self) -> usize {
        self.dataframe.len()
    }

    pub fn all_dataframe(&self) -> &DataFrame {
        &self.dataframe
    }
}
