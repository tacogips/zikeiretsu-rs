use super::*;

use crate::tsdb::{datapoint::*, field::*};

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
            if let Err(e) = check_datapoints_is_sorted(&datapoints) {
                return Err(e);
            }
        }
        Ok(Self { datapoints })
    }
}

#[async_trait]
impl DatapointsStore for RefReadonlyStore<'_> {
    async fn datapoints(&mut self) -> Result<&[DataPoint]> {
        Ok(&self.datapoints)
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
            if let Err(e) = check_datapoints_is_sorted(&datapoints) {
                return Err(e);
            }
        }
        Ok(Self { datapoints })
    }
}

#[async_trait]
impl DatapointsStore for ReadonlyStore {
    async fn datapoints(&mut self) -> Result<&[DataPoint]> {
        Ok(&self.datapoints)
    }
}
