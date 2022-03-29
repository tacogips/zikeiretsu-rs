use super::Result as StoreResult;
use super::*;

use crate::tsdb::dataframe::*;

pub struct ReadonlyStore {
    dataframe: DataFrame,
}

impl ReadonlyStore {
    pub fn new(dataframe: DataFrame, validate: bool) -> StoreResult<Self> {
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
