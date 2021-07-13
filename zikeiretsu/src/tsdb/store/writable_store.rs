use super::*;
use crate::tsdb::{datapoint::*, field::*};
use std::marker::Send;
use tokio::sync::Mutex;

pub trait DatapointSorter: Clone {
    fn compare(&mut self, lhs: &DataPoint, rhs: &DataPoint) -> Ordering;
}

#[derive(Clone)]
pub struct DatapointDefaultSorter;

impl DatapointSorter for DatapointDefaultSorter {
    fn compare(&mut self, lhs: &DataPoint, rhs: &DataPoint) -> Ordering {
        lhs.timestamp_nano.cmp(&rhs.timestamp_nano)
    }
}

pub struct WritableStoreBuilder<S: DatapointSorter> {
    field_types: Vec<FieldType>,
    convert_duty_to_sorted_on_read: bool,
    sorter: S,
}

impl WritableStoreBuilder<DatapointDefaultSorter> {
    fn default(field_types: Vec<FieldType>) -> Self {
        Self {
            field_types,
            convert_duty_to_sorted_on_read: true,
            sorter: DatapointDefaultSorter,
        }
    }
}

impl<S: DatapointSorter> WritableStoreBuilder<S> {
    pub fn build(self) -> WritableStore<S> {
        WritableStore {
            field_types: self.field_types,
            convert_duty_to_sorted_on_read: self.convert_duty_to_sorted_on_read,
            duty_datapoints: Mutex::new(vec![]),
            sorted_datapoints: vec![],
            sorter: self.sorter,
        }
    }
}

pub struct WritableStore<S: DatapointSorter> {
    field_types: Vec<FieldType>,

    convert_duty_to_sorted_on_read: bool,

    //TODO(tacogips) Consider LEFT-RIGHT pattern instead of locking for performance if need.
    duty_datapoints: Mutex<Vec<DataPoint>>,
    sorted_datapoints: Vec<DataPoint>,

    sorter: S,
}

impl WritableStore<DatapointDefaultSorter> {
    pub fn new_with_default_sorter(field_types: Vec<FieldType>) -> Self {
        Self {
            field_types,
            duty_datapoints: Mutex::new(vec![]),
            convert_duty_to_sorted_on_read: true,
            sorted_datapoints: vec![],
            sorter: DatapointDefaultSorter,
        }
    }
}

impl<S> WritableStore<S>
where
    S: DatapointSorter,
{
    pub async fn push(&mut self, data_point: DataPoint) -> Result<()> {
        #[cfg(feature = "validate")]
        if !same_field_types(&self.field_types, &data_point.field_values) {
            let data_point_fields = data_point
                .field_values
                .iter()
                .map(|e| e.as_type().to_string())
                .collect::<Vec<String>>()
                .join(",");

            return Err(StoreError::DataFieldTypesMismatched(data_point_fields));
        }

        let mut duty_datapoints = self.duty_datapoints.lock().await;

        duty_datapoints.push(data_point);
        Ok(())
    }

    pub async fn apply_dirties(&mut self) -> Result<()> {
        let mut dirty_datapoints = self.duty_datapoints.lock().await;
        if dirty_datapoints.is_empty() {
            return Ok(());
        }

        let mut sorter = self.sorter.clone();
        dirty_datapoints.sort_by(|l, r| sorter.compare(l, r));

        if self.sorted_datapoints.is_empty() {
            self.sorted_datapoints.append(&mut dirty_datapoints);
        } else {
            while let Some(head) = dirty_datapoints.get(0) {
                let last = self.sorted_datapoints.last().unwrap();
                match last.timestamp_nano.cmp(&head.timestamp_nano) {
                    Ordering::Equal | Ordering::Less => {
                        self.sorted_datapoints.append(&mut dirty_datapoints);
                        break;
                    }
                    _ => {
                        let head = dirty_datapoints.remove(0);
                        match binary_search_by(
                            &self.sorted_datapoints,
                            |datapoint| datapoint.timestamp_nano.cmp(&head.timestamp_nano),
                            BinaryRangeSearchType::AtMost,
                        ) {
                            Some(idx) => {
                                self.sorted_datapoints.insert(idx, head);
                            }
                            None => {
                                self.sorted_datapoints.insert(0, head);
                            }
                        }
                    }
                };
            }
        }
        dirty_datapoints.clear();

        Ok(())
    }
}

#[async_trait]
impl<S> DatapointsStore for WritableStore<S>
where
    S: DatapointSorter + Send,
{
    async fn datapoints(&mut self) -> Result<&[DataPoint]> {
        if self.convert_duty_to_sorted_on_read {
            self.apply_dirties().await?;
        }
        Ok(&self.sorted_datapoints)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::tsdb::*;
    use crate::*;
    macro_rules! float_data_points {
        ($({$timestamp:expr,$values:expr}),*) => {
            vec![
            $(DataPoint::new(ts!($timestamp), $values.into_iter().map(|each| FieldValue::Float64(each as f64)).collect())),*
            ]
        };
    }

    macro_rules! ts {
        ($v:expr) => {
            TimestampNano::new($v)
        };
    }

    #[tokio::test]
    async fn writable_store_test1() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715063000, vec![200f64,36f64]}
        );
        let mut store =
            WritableStoreBuilder::default(vec![FieldType::Float64, FieldType::Float64]).build();

        for each in datapoints.into_iter() {
            let result = store.push(each).await;
            assert!(result.is_ok())
        }

        let expected = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]}
        );
        let data_points = store.datapoints().await;

        assert!(data_points.is_ok());
        let data_points = data_points.unwrap();
        assert_eq!(data_points, expected);
    }
}
