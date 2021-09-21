mod persistence;
mod sorter;

use super::*;
use crate::tsdb::{
    datapoint::*, datapoints_searcher::*, field::*, metrics::Metrics, storage::api as storage_api,
};

pub use persistence::*;
use sorter::*;
use std::marker::Send;
use std::ptr;
use tokio::sync::{Mutex, MutexGuard};

pub struct WritableStoreBuilder<S: DatapointSorter> {
    metrics: Metrics,
    field_types: Vec<FieldType>,
    convert_dirty_to_sorted_on_read: bool,
    sorter: S,
    persistence: Persistence,
}

impl Default for Persistence {
    fn default() -> Self {
        Self::OnMemory
    }
}

impl WritableStoreBuilder<DatapointDefaultSorter> {
    pub fn default(metrics: Metrics, field_types: Vec<FieldType>) -> Self {
        Self {
            metrics,
            field_types,
            convert_dirty_to_sorted_on_read: true,
            sorter: DatapointDefaultSorter,
            persistence: Persistence::default(),
        }
    }
}

impl<S: DatapointSorter + Send> WritableStoreBuilder<S> {
    pub fn convert_dirty_to_sorted_on_read(
        mut self,
        convert_dirty_to_sorted_on_read: bool,
    ) -> Self {
        self.convert_dirty_to_sorted_on_read = convert_dirty_to_sorted_on_read;
        self
    }

    pub fn sorter<NS: DatapointSorter + Send>(self, sorter: NS) -> WritableStoreBuilder<NS> {
        let WritableStoreBuilder {
            metrics,
            field_types,
            convert_dirty_to_sorted_on_read,
            persistence,
            ..
        } = self;

        WritableStoreBuilder {
            sorter,
            metrics,
            field_types,
            convert_dirty_to_sorted_on_read,
            persistence,
        }
    }

    pub fn persistence(mut self, persistence: Persistence) -> Self {
        self.persistence = persistence;
        self
    }

    pub fn build(self) -> WritableStore<S> {
        WritableStore {
            metrics: self.metrics,
            field_types: self.field_types,
            convert_dirty_to_sorted_on_read: self.convert_dirty_to_sorted_on_read,
            dirty_datapoints: Mutex::new(vec![]),
            sorted_datapoints: Mutex::new(vec![]),
            sorter: self.sorter,
            persistence: self.persistence,
        }
    }
}

pub struct WritableStore<S: DatapointSorter> {
    metrics: Metrics,
    field_types: Vec<FieldType>,

    convert_dirty_to_sorted_on_read: bool,

    //TODO(tacogips) Consider LEFT-RIGHT pattern instead of locking for performance if need.
    dirty_datapoints: Mutex<Vec<DataPoint>>,
    sorted_datapoints: Mutex<Vec<DataPoint>>,
    sorter: S,
    persistence: Persistence,
}

impl WritableStore<DatapointDefaultSorter> {
    pub fn builder<M: Into<Metrics>>(
        metrics: M,
        field_types: Vec<FieldType>,
    ) -> WritableStoreBuilder<DatapointDefaultSorter> {
        WritableStoreBuilder::default(metrics.into(), field_types)
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

        let mut dirty_datapoints = self.dirty_datapoints.lock().await;

        dirty_datapoints.push(data_point);
        Ok(())
    }

    pub async fn push_multi(&mut self, data_points: Vec<DataPoint>) -> Result<()> {
        #[cfg(feature = "validate")]
        for data_point in data_points.iter() {
            if !same_field_types(&self.field_types, &data_point.field_values) {
                let data_point_fields = data_point
                    .field_values
                    .iter()
                    .map(|e| e.as_type().to_string())
                    .collect::<Vec<String>>()
                    .join(",");

                return Err(StoreError::DataFieldTypesMismatched(data_point_fields));
            }
        }

        let mut dirty_datapoints = self.dirty_datapoints.lock().await;

        for each_data_point in data_points {
            dirty_datapoints.push(each_data_point);
        }
        Ok(())
    }

    pub async fn apply_dirties(&mut self) -> Result<()> {
        let mut dirty_datapoints = self.dirty_datapoints.lock().await;
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
        let datapoints = self.datapoints_with_lock().await?;
        let datapoints_searcher = DatapointSearcher::new(&datapoints);

        if let Some((_, indices)) = datapoints_searcher
            .search_with_indices(datapoint_search_condition)
            .await
        {
            remove_range(datapoints, indices);
        }

        Ok(())
    }

    /// persist on disk and cloud
    pub async fn persist(&mut self, condition: PersistCondition) -> Result<Option<()>> {
        if let Persistence::Storage(db_dir, cloud_setting) = self.persistence.clone() {
            let metrics = self.metrics.clone();
            let datapoints = self.datapoints_with_lock().await?;
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
                    remove_range(datapoints, indices);
                }
                Ok(Some(()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn datapoints_with_lock(&mut self) -> Result<MutexGuard<'_, Vec<DataPoint>>> {
        if self.convert_dirty_to_sorted_on_read {
            self.apply_dirties().await?;
        }
        Ok(self.sorted_datapoints.lock().await)
    }
}

pub fn remove_range(mut datapoints: MutexGuard<'_, Vec<DataPoint>>, range: (usize, usize)) {
    let len = datapoints.len();
    let (start, end) = range;
    assert!(
        start <= end,
        "invalid purge index start:{} > end:{}",
        start,
        end
    );

    assert!(
        end < len,
        "invalid purge end index  end:{}, len:{}",
        end,
        len
    );

    let purge_len = end - start + 1;

    unsafe {
        let purge_start_ptr = datapoints.as_mut_ptr().add(start);
        ptr::copy(
            purge_start_ptr.offset(purge_len as isize),
            purge_start_ptr,
            len - start - purge_len,
        );
        datapoints.set_len(len - purge_len);
    }
}
