mod persistence;
mod sorter;

use super::*;
use uuid::Uuid;

use crate::tsdb::{
    datapoint::*, datapoints_searcher::*, field::*, metrics::Metrics, storage::api as storage_api,
};

use crate::tsdb::util;
use chrono::Duration;
pub use persistence::*;
pub use sorter::*;
use std::marker::Send;
pub use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub struct WritableStoreBuilder<S: DatapointSorter + 'static> {
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

impl<S: DatapointSorter + Send + 'static> WritableStoreBuilder<S> {
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

    pub fn build(self) -> Arc<Mutex<WritableStore<S>>> {
        let store = WritableStore {
            store_id: Uuid::new_v4(),
            metrics: self.metrics,
            field_types: self.field_types,
            convert_dirty_to_sorted_on_read: self.convert_dirty_to_sorted_on_read,
            dirty_datapoints: vec![],
            sorted_datapoints: vec![],
            sorter: self.sorter,
            persistence: self.persistence,
        };
        Arc::new(Mutex::new(store))
    }
}

pub struct WritableStore<S: DatapointSorter + 'static> {
    store_id: Uuid,
    metrics: Metrics,
    #[allow(dead_code)]
    field_types: Vec<FieldType>,

    convert_dirty_to_sorted_on_read: bool,

    //TODO(tacogips) Consider LEFT-RIGHT pattern instead of locking to increase performance.
    dirty_datapoints: Vec<DataPoint>,
    sorted_datapoints: Vec<DataPoint>,
    sorter: S,
    persistence: Persistence,
}

impl WritableStore<DatapointDefaultSorter> {
    pub fn builder(
        metrics: Metrics,
        field_types: Vec<FieldType>,
    ) -> WritableStoreBuilder<DatapointDefaultSorter> {
        WritableStoreBuilder::default(metrics, field_types)
    }
}

impl<S> WritableStore<S>
where
    S: DatapointSorter + 'static,
{
    pub async fn push(&mut self, data_point: DataPoint) -> Result<()> {
        #[cfg(feature = "trace-log")]
        log::trace!("push multi data: {data_point:?}");

        #[cfg(feature = "validate")]
        if !same_field_types(&self.field_types, &data_point.field_values) {
            let expected = self
                .field_types
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(",");

            let data_point_fields = data_point
                .field_values
                .iter()
                .map(|e| e.as_type().to_string())
                .collect::<Vec<String>>()
                .join(",");

            return Err(StoreError::DataFieldTypesMismatched(
                expected,
                data_point_fields,
            ));
        }

        self.dirty_datapoints.push(data_point);
        Ok(())
    }

    pub async fn push_multi(&mut self, data_points: Vec<DataPoint>) -> Result<()> {
        #[cfg(feature = "trace-log")]
        log::trace!("push multi data: {data_points:?}");

        #[cfg(feature = "validate")]
        for data_point in data_points.iter() {
            if !same_field_types(&self.field_types, &data_point.field_values) {
                let expected = self
                    .field_types
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(",");

                let data_point_fields = data_point
                    .field_values
                    .iter()
                    .map(|e| e.as_type().to_string())
                    .collect::<Vec<String>>()
                    .join(",");

                return Err(StoreError::DataFieldTypesMismatched(
                    expected,
                    data_point_fields,
                ));
            }
        }

        for each_data_point in data_points {
            self.dirty_datapoints.push(each_data_point);
        }
        Ok(())
    }

    pub async fn apply_dirties(&mut self) -> Result<()> {
        #[cfg(feature = "trace-log")]
        log::trace!("applying dirties. len : {}", data_points.len());
        if self.dirty_datapoints.is_empty() {
            return Ok(());
        }

        let mut sorter = self.sorter.clone();
        self.dirty_datapoints.sort_by(|l, r| sorter.compare(l, r));

        if self.sorted_datapoints.is_empty() {
            self.sorted_datapoints.append(&mut self.dirty_datapoints);
        } else {
            while let Some(head) = self.dirty_datapoints.get(0) {
                let last = self.sorted_datapoints.last().unwrap();
                match last.timestamp_nano.cmp(&head.timestamp_nano) {
                    Ordering::Equal | Ordering::Less => {
                        self.sorted_datapoints.append(&mut self.dirty_datapoints);
                        break;
                    }
                    _ => {
                        let head = self.dirty_datapoints.remove(0);
                        match binary_search_by(
                            &self.sorted_datapoints,
                            |datapoint| datapoint.timestamp_nano.cmp(&head.timestamp_nano),
                            BinaryRangeSearchType::AtMostInclusive,
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
        self.dirty_datapoints.clear();

        Ok(())
    }

    pub fn shrink_to_fit_vec(&mut self) {
        self.dirty_datapoints.shrink_to_fit();
        self.sorted_datapoints.shrink_to_fit();
    }

    pub async fn purge(&mut self, datapoint_search_condition: DatapointsRange) -> Result<()> {
        let datapoints = self.datapoints_mut().await?;
        let datapoints_searcher = DatapointSearcher::new(datapoints);

        if let Some((_, indices)) = datapoints_searcher
            .search_with_indices(datapoint_search_condition)
            .await
        {
            remove_range(datapoints, indices)?;
        }

        Ok(())
    }

    /// persist on disk and to cloud
    pub async fn persist(&mut self, condition: PersistCondition) -> Result<Option<()>> {
        if let Persistence::Storage(db_dir, cloud_storage_and_setting) = self.persistence.clone() {
            let metrics = self.metrics.clone();
            let writer_id = self.store_id;
            let all_datapoints = self.datapoints_mut().await?;
            let datapoints_searcher = DatapointSearcher::new(all_datapoints);

            if let Some((datapoints, indices)) = datapoints_searcher
                .search_with_indices(condition.datapoint_search_condition)
                .await
            {
                storage_api::write::write_datas(
                    db_dir,
                    &writer_id,
                    &metrics,
                    datapoints,
                    cloud_storage_and_setting
                        .as_ref()
                        .map(|(cloud_strorage, cloud_setting)| (cloud_strorage, cloud_setting)),
                )
                .await?;

                if condition.remove_from_store_after_persisted {
                    log::debug!(
                        "clear writable store after persistence. indices:{indices:?}, datapoint len: {data_len}",
                        data_len = datapoints.len(),
                    );
                    remove_range(all_datapoints, indices)?;
                    self.shrink_to_fit_vec();

                    log::debug!(
                        "after clear writable store, sorted datapoint len: {sorted_datapoint_len }, dirty datapoint len: {dirty_datapoint_len}",
                        sorted_datapoint_len = self.sorted_datapoints.len(),
                        dirty_datapoint_len = self.dirty_datapoints.len(),
                    );
                }
                Ok(Some(()))
            } else {
                Ok(None)
            }
        } else {
            Err(StoreError::NoPersistenceSettingError)
        }
    }

    pub async fn datapoints_mut(&mut self) -> Result<&mut Vec<DataPoint>> {
        if self.convert_dirty_to_sorted_on_read {
            self.apply_dirties().await?;
        }
        Ok(&mut self.sorted_datapoints)
    }

    pub async fn datapoints(&mut self) -> Result<&Vec<DataPoint>> {
        if self.convert_dirty_to_sorted_on_read {
            self.apply_dirties().await?;
        }
        Ok(&self.sorted_datapoints)
    }

    pub async fn datapoints_tail_limit(&mut self, limit: usize) -> Result<&[DataPoint]> {
        let datapoints = self.datapoints().await?.as_slice();
        let found_index = linear_search_grouped_n_datas_with_func(
            datapoints,
            limit,
            |prev, current| prev.timestamp_nano.cmp(&current.timestamp_nano),
            LinearSearchDirection::Desc,
        );

        Ok(&datapoints[found_index..])
    }

    pub fn create_sink_channel(
        store: Arc<Mutex<WritableStore<S>>>,
    ) -> mpsc::UnboundedSender<Vec<DataPoint>> {
        let (datapoints_tx, mut datapoints_rx) = mpsc::unbounded_channel::<Vec<DataPoint>>();
        task::spawn(async move {
            while let Some(datapoints) = datapoints_rx.recv().await {
                log::trace!("datapoints to push multi {:?}", datapoints);
                let mut locked_store = store.lock().await;
                if let Err(e) = locked_store.push_multi(datapoints).await {
                    log::error!("error on push multiple datapoints :{:?}", e,);
                }
            }
        });
        datapoints_tx
    }

    pub fn start_repetedly_persist(
        store: Arc<Mutex<WritableStore<S>>>,
        persist_interval_duration: Duration,
        clear_after_persisted: bool,
    ) -> PeriodicallyPeristenceShutdown {
        start_periodically_persistence(store, persist_interval_duration, clear_after_persisted)
    }

    pub async fn scavange_on_shutdown(&self) -> Result<()> {
        if let Persistence::Storage(db_dir, cloud_storage_and_setting) = self.persistence.clone() {
            storage_api::write::remove_local_lock_file_if_same_writer(
                db_dir,
                &self.store_id,
                &self.metrics,
            )
            .await?;

            storage_api::write::remove_cloud_lock_file_if_same_writer(
                &self.store_id,
                &self.metrics,
                cloud_storage_and_setting
                    .as_ref()
                    .map(|(cloud_strorage, cloud_setting)| (cloud_strorage, cloud_setting)),
            )
            .await?;
        }
        Ok(())
    }
}

fn remove_range(datapoints: &mut Vec<DataPoint>, range: (usize, usize)) -> Result<()> {
    util::remove_range(datapoints, range)?;
    Ok(())
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::tsdb::*;
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

    #[test]
    fn test_remove_range() {
        let mut datapoints = float_data_points!(
            {1629745451_715062000, vec![200f64,12f64]},
            {1629745451_715063000, vec![300f64,36f64]},
            {1629745451_715064000, vec![400f64,36f64]},
            {1629745451_715065000, vec![500f64,36f64]},
            {1629745451_715066000, vec![600f64,36f64]}
        );

        remove_range(&mut datapoints, (1, 3)).unwrap();

        let expected = float_data_points!(
            {1629745451_715062000, vec![200f64,12f64]},
            {1629745451_715066000, vec![600f64,36f64]}
        );

        assert_eq!(expected, datapoints);
    }
}
