use super::*;
use crate::tsdb::{
    datapoint::*, datapoints_searcher::*, field::*, metrics::Metrics, storage::api as storage_api,
};
use std::marker::{Send, Sync};
use std::path::{Path, PathBuf};
use tokio::sync::{Mutex, MutexGuard};

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

pub struct WritableStoreBuilder<S: DatapointSorter> {
    metrics: Metrics,
    field_types: Vec<FieldType>,
    convert_duty_to_sorted_on_read: bool,
    sorter: S,
    persistence: Persistence,
}

impl WritableStoreBuilder<DatapointDefaultSorter> {
    fn default(metrics: Metrics, field_types: Vec<FieldType>) -> Self {
        Self {
            metrics,
            field_types,
            convert_duty_to_sorted_on_read: true,
            sorter: DatapointDefaultSorter,
            persistence: Persistence::default(),
        }
    }
}

#[derive(Clone)]
pub enum Persistence {
    OnMemory,
    Storage(PathBuf, Option<storage_api::CloudSetting>),
}

impl Default for Persistence {
    fn default() -> Self {
        Self::OnMemory
    }
}

impl<S: DatapointSorter + Send> WritableStoreBuilder<S> {
    pub fn build(self) -> WritableStore<S> {
        WritableStore {
            metrics: self.metrics,
            field_types: self.field_types,
            convert_duty_to_sorted_on_read: self.convert_duty_to_sorted_on_read,
            duty_datapoints: Mutex::new(vec![]),
            sorted_datapoints: Mutex::new(vec![]),
            sorter: self.sorter,
            persistence: self.persistence,
        }
    }
}

pub struct WritableStore<S: DatapointSorter> {
    metrics: Metrics,
    field_types: Vec<FieldType>,

    convert_duty_to_sorted_on_read: bool,

    //TODO(tacogips) Consider LEFT-RIGHT pattern instead of locking for performance if need.
    duty_datapoints: Mutex<Vec<DataPoint>>,
    sorted_datapoints: Mutex<Vec<DataPoint>>,
    sorter: S,
    persistence: Persistence,
}

#[derive(Clone)]
pub struct PersistCondition {
    datapoint_search_condition: DatapointSearchCondition,
    clear_after_persisted: bool,
}

impl WritableStore<DatapointDefaultSorter> {
    pub fn new_with_default_sorter(metrics: Metrics, field_types: Vec<FieldType>) -> Self {
        Self {
            metrics,
            field_types,
            duty_datapoints: Mutex::new(vec![]),
            convert_duty_to_sorted_on_read: true,
            sorted_datapoints: Mutex::new(vec![]),
            sorter: DatapointDefaultSorter,
            persistence: Persistence::default(),
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
        //let mut _edit_lock = self.edit_sorted_datapoints_lock.clone().lock_owned().await;
        let mut dirty_datapoints = self.duty_datapoints.lock().await;
        if dirty_datapoints.is_empty() {
            return Ok(());
        }

        let mut sorted_datapoints = self.sorted_datapoints.lock().await;

        let mut sorter = self.sorter.clone();
        dirty_datapoints.sort_by(|l, r| sorter.compare(l, r));

        if sorted_datapoints.is_empty() {
            sorted_datapoints.append(&mut dirty_datapoints);
        } else {
            while let Some(head) = dirty_datapoints.get(0) {
                let last = sorted_datapoints.last().unwrap();
                match last.timestamp_nano.cmp(&head.timestamp_nano) {
                    Ordering::Equal | Ordering::Less => {
                        sorted_datapoints.append(&mut dirty_datapoints);
                        break;
                    }
                    _ => {
                        let head = dirty_datapoints.remove(0);
                        match binary_search_by(
                            &sorted_datapoints,
                            |datapoint| datapoint.timestamp_nano.cmp(&head.timestamp_nano),
                            BinaryRangeSearchType::AtMost,
                        ) {
                            Some(idx) => {
                                sorted_datapoints.insert(idx, head);
                            }
                            None => {
                                sorted_datapoints.insert(0, head);
                            }
                        }
                    }
                };
            }
        }
        dirty_datapoints.clear();

        Ok(())
    }

    pub async fn purge(
        &mut self,
        datapoint_search_condition: DatapointSearchCondition,
    ) -> Result<()> {
        let datapoints = self.datapoints().await?;
        let datapoints_searcher = DatapointSearcher::new(&datapoints);

        if let Some((_, indices)) = datapoints_searcher
            .search_with_indices(datapoint_search_condition)
            .await
        {
            purge_datapoints(datapoints, indices);
            Ok(())
        } else {
            Ok(())
        }
    }

    /// persist on disk and cloud
    pub async fn persist<P: AsRef<Path>>(
        &mut self,
        condition: PersistCondition,
    ) -> Result<Option<()>> {
        if let Persistence::Storage(db_dir, cloud_setting) = self.persistence.clone() {
            let metrics = self.metrics.clone();
            let datapoints = self.datapoints().await?;
            let datapoints_searcher = DatapointSearcher::new(&datapoints);

            if let Some((_datapoints, indices)) = datapoints_searcher
                .search_with_indices(condition.datapoint_search_condition)
                .await
            {
                storage_api::write::write_datas(
                    db_dir,
                    &metrics,
                    &datapoints,
                    cloud_setting.as_ref(),
                )
                .await?;

                if condition.clear_after_persisted {
                    purge_datapoints(datapoints, indices);
                }
                Ok(Some(()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn datapoints(&mut self) -> Result<MutexGuard<'_, Vec<DataPoint>>> {
        if self.convert_duty_to_sorted_on_read {
            self.apply_dirties().await?;
        }
        Ok(self.sorted_datapoints.lock().await)
    }
}

pub fn purge_datapoints(
    mut datapoints: MutexGuard<'_, Vec<DataPoint>>,
    purge_indices: (usize, usize),
) {
    let (start, end) = purge_indices;
    for idx in start..(end + 1) {
        (*datapoints).swap_remove(idx);
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
        let mut store = WritableStoreBuilder::default(
            Metrics::new("default"),
            vec![FieldType::Float64, FieldType::Float64],
        )
        .build();

        for each in datapoints.into_iter() {
            let result = store.push(each).await;
            assert!(result.is_ok())
        }

        let expected = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]}
        );
        ////TODO(tacogips) remove this
        //store.apply_dirties().await.unwrap();
        let data_points = store.datapoints().await;

        assert!(data_points.is_ok());
        let data_points = data_points.unwrap();
        assert_eq!(*data_points, expected);
    }
}
