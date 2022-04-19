use super::Result as StoreResult;
use super::*;

use crate::tsdb::time_series_dataframe::*;

pub struct ReadonlyStore {
    time_series_dataframe: TimeSeriesDataFrame,
}

impl ReadonlyStore {
    pub fn new(dataframe: TimeSeriesDataFrame, validate: bool) -> StoreResult<Self> {
        if validate {
            if let Err(e) = TimeSeriesDataFrame::check_dataframe_is_sorted(&dataframe) {
                return Err(StoreError::UnsortedDataFrame(e.to_string()));
            }
        }
        Ok(Self {
            time_series_dataframe: dataframe,
        })
    }

    pub fn len(&self) -> usize {
        self.time_series_dataframe.len()
    }

    pub fn as_dataframe(&self) -> &TimeSeriesDataFrame {
        &self.time_series_dataframe
    }
}
