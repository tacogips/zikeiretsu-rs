use super::field::*;
use crate::tsdb::util::*;
use serde::{Deserialize, Serialize};
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DataSeries {
    pub values: Vec<FieldValue>,
}

impl DataSeries {
    pub fn new(values: Vec<FieldValue>) -> Self {
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn merge(&mut self, other: &mut DataSeries) {
        self.values.append(&mut other.values);
    }

    pub fn get(&self, index: usize) -> Option<&FieldValue> {
        self.values.get(index)
    }
}

impl From<DataSeriesRef<'_>> for DataSeries {
    fn from(ds: DataSeriesRef<'_>) -> DataSeries {
        DataSeries::new(ds.values.into_iter().map(|e| e.clone()).collect())
    }
}

#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct DataSeriesRef<'a> {
    pub values: &'a [FieldValue],
}

impl<'a> DataSeriesRef<'a> {
    pub fn new(values: &'a [FieldValue]) -> Self {
        Self { values }
    }

    pub fn get(&self, index: usize) -> Option<&FieldValue> {
        self.values.get(index)
    }
}
