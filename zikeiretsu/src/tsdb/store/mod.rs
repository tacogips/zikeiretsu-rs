pub mod read_only_store;
pub mod writable_store;

use crate::tsdb::{search::*, storage::api as storage_api};
use chrono::{DateTime, Utc};
pub use read_only_store::*;
use std::cmp::Ordering;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task;
pub use writable_store::*;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("append error. {0}")]
    AppendError(String),

    #[error("unsorted datapoints. {0}")]
    UnsortedDatapoints(String),

    #[error("data field types mismatched. {0}")]
    DataFieldTypesMismatched(String),

    #[error("search error. {0}")]
    SearchError(String),

    #[error("storage api error. {0}")]
    StorageErorr(#[from] storage_api::StorageApiError),

    #[error("datetime channel Sender Error. {0}")]
    DatetimeChannelSenderError(#[from] mpsc::error::SendError<DateTime<Utc>>),

    #[error("datetime channel Sender Error. {0}")]
    JoinError(#[from] task::JoinError),
}

type Result<T> = std::result::Result<T, StoreError>;

#[cfg(test)]
mod test {

    use crate::tsdb::*;
    use std::path::PathBuf;
    use tempdir::TempDir;
    use tokio::sync::Mutex;

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
        let data_points = store.datapoints_with_lock().await;

        assert!(data_points.is_ok());
        let data_points = data_points.unwrap();
        assert_eq!(*data_points, expected);
    }

    #[tokio::test]
    async fn writable_store_test_2_purge() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
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
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );

        ////TODO(tacogips) remove this
        //store.apply_dirties().await.unwrap();
        {
            //getting datapoint first
            let data_points = store.datapoints_with_lock().await;

            assert!(data_points.is_ok());
            let data_points = data_points.unwrap();
            assert_eq!(*data_points, expected);
        }

        let condition = DatapointSearchCondition::since(TimestampNano::new(1629745451_715063000))
            .with_until(TimestampNano::new(1629745451_715065000));
        let purge_result = store.purge(condition).await;
        assert!(purge_result.is_ok());

        {
            let expected = float_data_points!(
                {1629745451_715062000, vec![100f64,12f64]},
                {1629745451_715066000, vec![300f64,36f64]}
            );
            let data_points = store.datapoints_with_lock().await;

            assert!(data_points.is_ok());
            let data_points = data_points.unwrap();
            assert_eq!(*data_points, expected);
        }
    }

    #[tokio::test]
    async fn remove_range_1() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );
        let datapoints = Mutex::new(datapoints);
        let lock_datapoints = datapoints.lock().await;

        remove_range(lock_datapoints, (2, 3));

        let expected_datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );

        let lock_datapoints = datapoints.lock().await;
        assert_eq!(*lock_datapoints, expected_datapoints);
    }

    #[tokio::test]
    async fn remove_range_2() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );
        let datapoints = Mutex::new(datapoints);
        let lock_datapoints = datapoints.lock().await;

        remove_range(lock_datapoints, (0, 4));

        let expected_datapoints = float_data_points!();

        let lock_datapoints = datapoints.lock().await;
        assert_eq!(*lock_datapoints, expected_datapoints);
    }

    #[tokio::test]
    async fn remove_range_3() {
        let datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );
        let datapoints = Mutex::new(datapoints);
        let lock_datapoints = datapoints.lock().await;

        remove_range(lock_datapoints, (4, 4));

        let expected_datapoints = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]}
        );

        let lock_datapoints = datapoints.lock().await;
        assert_eq!(*lock_datapoints, expected_datapoints);
    }

    #[tokio::test]
    async fn persistence_test_1() {
        let temp_db_dir = TempDir::new("persistence_test_1").unwrap();

        let field_types = vec![FieldType::Float64, FieldType::Float64];
        let metrics: Metrics = "test_metrics".into();

        let persistence = Persistence::Storage(PathBuf::from(temp_db_dir.path()), None);
        let mut store = WritableStore::builder(metrics.clone(), field_types)
            .persistence(persistence)
            .build();

        {
            let input_datapoints = float_data_points!(
                {1629745451_715062000, vec![100f64,12f64]},
                {1629745451_715066000, vec![300f64,36f64]},
                {1629745451_715063000, vec![200f64,36f64]},
                {1629745451_715065000, vec![300f64,36f64]},
                {1629745451_715064000, vec![200f64,37f64]}
            );

            let result = store.push_multi(input_datapoints.clone()).await;
            assert!(result.is_ok());

            let expected_datapoints = float_data_points!(
                {1629745451_715062000, vec![100f64,12f64]},
                {1629745451_715063000, vec![200f64,36f64]},
                {1629745451_715064000, vec![200f64,37f64]},
                {1629745451_715065000, vec![300f64,36f64]},
                {1629745451_715066000, vec![300f64,36f64]}
            );

            let stored_datapoints = store.datapoints_with_lock().await.unwrap();
            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let input_datapoints = float_data_points!(
                {1629745451_715061000, vec![100f64,4000f64]},
                {1629745451_715067000, vec![700f64,100f64]}
            );

            let result = store.push_multi(input_datapoints.clone()).await;
            assert!(result.is_ok());

            let expected_datapoints = float_data_points!(
                {1629745451_715061000, vec![100f64,4000f64]},
                {1629745451_715062000, vec![100f64,12f64]},
                {1629745451_715063000, vec![200f64,36f64]},
                {1629745451_715064000, vec![200f64,37f64]},
                {1629745451_715065000, vec![300f64,36f64]},
                {1629745451_715066000, vec![300f64,36f64]},
                {1629745451_715067000, vec![700f64,100f64]}
            );

            let stored_datapoints = store.datapoints_with_lock().await.unwrap();
            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let condition = PersistCondition {
                datapoint_search_condition: DatapointSearchCondition::new(
                    Some(TimestampNano::new(1629745451_715061000)),
                    Some(TimestampNano::new(1629745451_715066000)),
                ),
                clear_after_persisted: true,
            };

            let result = store.persist(condition).await;

            assert!(result.is_ok());
        }

        {
            // retaining datapoints
            let expected_datapoints = float_data_points!(
                {1629745451_715067000, vec![700f64,100f64]}
            );

            let stored_datapoints = store.datapoints_with_lock().await.unwrap();
            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let condition = DatapointSearchCondition::new(
                Some(TimestampNano::new(1629745451_715062000)),
                Some(TimestampNano::new(1629745451_715066000)),
            );

            let cache_setting = api::CacheSetting::none();

            let datapoints = api::read::search_datas(
                temp_db_dir.path(),
                &metrics,
                &condition,
                &cache_setting,
                None,
            )
            .await;

            assert!(datapoints.is_ok());
            let datapoints = datapoints.unwrap();

            let store = ReadonlyStore::new(datapoints, false).unwrap();
            let searcher = store.searcher().await;

            {
                let result = searcher.search(&condition).await;
                assert!(result.is_some());
                assert_eq!(
                    result.unwrap(),
                    float_data_points!(
                        {1629745451_715062000, vec![100f64,12f64]},
                        {1629745451_715063000, vec![200f64,36f64]},
                        {1629745451_715064000, vec![200f64,37f64]},
                        {1629745451_715065000, vec![300f64,36f64]},
                        {1629745451_715066000, vec![300f64,36f64]}
                    )
                );
            }

            {
                let another_condition = DatapointSearchCondition::new(
                    Some(TimestampNano::new(1629745451_715063000)),
                    Some(TimestampNano::new(1629745451_715065000)),
                );
                let result = searcher.search(&another_condition).await;
                assert!(result.is_some());
                assert_eq!(
                    result.unwrap(),
                    float_data_points!(
                        {1629745451_715063000, vec![200f64,36f64]},
                        {1629745451_715064000, vec![200f64,37f64]},
                        {1629745451_715065000, vec![300f64,36f64]}
                    )
                );
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn persistence_test_2() {
        let temp_db_dir = TempDir::new("persistence_test_2").unwrap();

        let field_types = vec![FieldType::Float64, FieldType::Float64];
        let metrics: Metrics = "test_metrics".into();

        let persistence = Persistence::Storage(PathBuf::from(temp_db_dir.path()), None);
        let mut store = WritableStore::builder(metrics.clone(), field_types)
            .persistence(persistence)
            .build();

        {
            let input_datapoints = float_data_points!(
                {1629745451_715062000, vec![100f64,12f64]},
                {1629745451_715066000, vec![300f64,36f64]},
                {1629745451_715063000, vec![200f64,36f64]},
                {1629745451_715065000, vec![300f64,36f64]},
                {1629745451_715064000, vec![200f64,37f64]},
                {1639745451_715061000, vec![1300f64,36f64]},
                {1639745451_715062000, vec![1200f64,37f64]}
            );

            let result = store.push_multi(input_datapoints.clone()).await;
            assert!(result.is_ok());

            let expected_datapoints = float_data_points!(
                {1629745451_715062000, vec![100f64,12f64]},
                {1629745451_715063000, vec![200f64,36f64]},
                {1629745451_715064000, vec![200f64,37f64]},
                {1629745451_715065000, vec![300f64,36f64]},
                {1629745451_715066000, vec![300f64,36f64]},
                {1639745451_715061000, vec![1300f64,36f64]},
                {1639745451_715062000, vec![1200f64,37f64]}
            );

            let stored_datapoints = store.datapoints_with_lock().await.unwrap();
            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let condition = PersistCondition {
                datapoint_search_condition: DatapointSearchCondition::new(None, None),
                clear_after_persisted: true,
            };

            let result = store.persist(condition).await;

            assert!(result.is_ok());
        }

        {
            // retaining datapoints
            let expected_datapoints = float_data_points!();

            let stored_datapoints = store.datapoints_with_lock().await.unwrap();
            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let condition = DatapointSearchCondition::new(None, None);

            let cache_setting = api::CacheSetting::none();

            let datapoints = api::read::search_datas(
                temp_db_dir.path(),
                &metrics,
                &condition,
                &cache_setting,
                None,
            )
            .await;

            assert!(datapoints.is_ok());
            let datapoints = datapoints.unwrap();

            let store = ReadonlyStore::new(datapoints, false).unwrap();
            let searcher = store.searcher().await;

            {
                let expected = float_data_points!(
                    {1629745451_715062000, vec![100f64,12f64]},
                    {1629745451_715063000, vec![200f64,36f64]},
                    {1629745451_715064000, vec![200f64,37f64]},
                    {1629745451_715065000, vec![300f64,36f64]},
                    {1629745451_715066000, vec![300f64,36f64]},
                    {1639745451_715061000, vec![1300f64,36f64]},
                    {1639745451_715062000, vec![1200f64,37f64]}
                );

                let result = searcher.search(&condition).await;
                assert!(result.is_some());

                assert_eq!(result.unwrap().len(), expected.len());
                for (i, each) in result.unwrap().into_iter().enumerate() {
                    assert_eq!(each, expected.get(i).unwrap());
                }
            }

            {
                let another_condition = DatapointSearchCondition::new(
                    Some(TimestampNano::new(1629745451_715063000)),
                    Some(TimestampNano::new(1629745451_715065000)),
                );
                let result = searcher.search(&another_condition).await;
                assert!(result.is_some());
                assert_eq!(
                    result.unwrap(),
                    float_data_points!(
                        {1629745451_715063000, vec![200f64,36f64]},
                        {1629745451_715064000, vec![200f64,37f64]},
                        {1629745451_715065000, vec![300f64,36f64]}
                    )
                );
            }
        }
    }
}
