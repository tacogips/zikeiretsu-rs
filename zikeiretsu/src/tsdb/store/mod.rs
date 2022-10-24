pub mod writable_store;

use crate::tsdb::storage::api as storage_api;
use crate::tsdb::storage::wal::WalError;
use crate::tsdb::util;
use chrono::{DateTime, Utc};
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

    #[error("unsorted dataframe. {0}")]
    UnsortedDataFrame(String),

    #[error("data field types mismatched. expected fields {0}, acutual:{1}")]
    DataFieldTypesMismatched(String, String),

    #[error("search error. {0}")]
    SearchError(String),

    #[error("storage api error. {0}")]
    StorageErorr(#[from] storage_api::StorageApiError),

    #[error("no persistence setting. ")]
    NoPersistenceSettingError,

    #[error("datetime channel Sender Error. {0}")]
    DatetimeChannelSenderError(#[from] mpsc::error::SendError<DateTime<Utc>>),

    #[error("datetime channel Sender Error. {0}")]
    JoinError(#[from] task::JoinError),

    #[error("invalid metrics error. {0}")]
    InvalidMetrics(String),

    #[error("Vec ope Error. {0}")]
    VecOpeError(#[from] util::VecOpeError),

    #[error("Wal Error. {0}")]
    WalError(#[from] WalError),
}

type Result<T> = std::result::Result<T, StoreError>;

#[cfg(test)]
mod test {

    use crate::tsdb::*;
    use std::path::PathBuf;
    use tempdir::TempDir;

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
        let store = WritableStoreBuilder::default(
            Metrics::new("default").unwrap(),
            vec![FieldType::Float64, FieldType::Float64],
        )
        .build();

        for each in datapoints.into_iter() {
            let mut s = store.lock().await;
            let result = s.push(each).await;
            assert!(result.is_ok())
        }

        let expected = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]}
        );
        let mut s = store.lock().await;
        let data_points = s.datapoints_mut().await;

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
        let store = WritableStoreBuilder::default(
            Metrics::new("default").unwrap(),
            vec![FieldType::Float64, FieldType::Float64],
        )
        .build();

        for each in datapoints.into_iter() {
            let mut s = store.lock().await;
            let result = s.push(each).await;
            assert!(result.is_ok())
        }

        let expected = float_data_points!(
            {1629745451_715062000, vec![100f64,12f64]},
            {1629745451_715063000, vec![200f64,36f64]},
            {1629745451_715064000, vec![200f64,36f64]},
            {1629745451_715065000, vec![300f64,36f64]},
            {1629745451_715066000, vec![300f64,36f64]}
        );

        {
            //getting datapoint first

            let mut s = store.lock().await;
            let data_points = s.datapoints_mut().await;

            assert!(data_points.is_ok());
            let data_points = data_points.unwrap();
            assert_eq!(*data_points, expected);
        }

        let condition = DatapointsRange::since(TimestampNano::new(1629745451_715063000))
            .with_until(TimestampNano::new(1629745451_715065001));

        {
            let mut s = store.lock().await;
            let purge_result = s.purge(condition).await;
            assert!(purge_result.is_ok());
        }

        {
            let expected = float_data_points!(
                {1629745451_715062000, vec![100f64,12f64]},
                {1629745451_715066000, vec![300f64,36f64]}
            );
            let mut s = store.lock().await;
            let data_points = s.datapoints_mut().await;

            assert!(data_points.is_ok());
            let data_points = data_points.unwrap();
            assert_eq!(*data_points, expected);
        }
    }

    #[tokio::test]
    async fn persistence_test_1() {
        let temp_db_dir = TempDir::new("persistence_test_1").unwrap();

        let field_types = vec![FieldType::Float64, FieldType::Float64];
        let metrics: Metrics = "test_metrics".try_into().unwrap();

        let persistence = Persistence::Storage(PathBuf::from(temp_db_dir.path()), None);
        let store = WritableStore::builder(metrics.clone(), field_types)
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

            let result = {
                let mut s = store.lock().await;
                s.push_multi(input_datapoints.clone()).await
            };
            assert!(result.is_ok());

            let expected_datapoints = float_data_points!(
                {1629745451_715062000, vec![100f64,12f64]},
                {1629745451_715063000, vec![200f64,36f64]},
                {1629745451_715064000, vec![200f64,37f64]},
                {1629745451_715065000, vec![300f64,36f64]},
                {1629745451_715066000, vec![300f64,36f64]}
            );

            let mut s = store.lock().await;
            let stored_datapoints = s.datapoints_mut().await.unwrap();
            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let input_datapoints = float_data_points!(
                {1629745451_715061000, vec![100f64,4000f64]},
                {1629745451_715067000, vec![700f64,100f64]}
            );

            let result = {
                let mut s = store.lock().await;
                s.push_multi(input_datapoints.clone()).await
            };
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

            let mut s = store.lock().await;
            let stored_datapoints = s.datapoints_mut().await.unwrap();

            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let condition = PersistCondition {
                datapoint_search_condition: DatapointsRange::new(
                    Some(TimestampNano::new(1629745451_715061000)),
                    Some(TimestampNano::new(1629745451_715066001)),
                ),
                remove_from_store_after_persisted: true,
            };

            let result = {
                let mut s = store.lock().await;
                s.persist(condition).await
            };

            assert!(result.is_ok());
        }

        {
            // retaining datapoints
            let expected_datapoints = float_data_points!(
                {1629745451_715067000, vec![700f64,100f64]}
            );

            let mut s = store.lock().await;
            let stored_datapoints = s.datapoints_mut().await.unwrap();
            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let condition = DatapointsSearchCondition {
                datapoints_range: DatapointsRange::new(
                    Some(TimestampNano::new(1629745451_715062000)),
                    Some(TimestampNano::new(1629745451_715066001)),
                ),
                limit: None,
            };

            let cache_setting = api::CacheSetting::none();

            let datapoints = api::read::search_dataframe(
                "test",
                temp_db_dir.path(),
                &metrics,
                None,
                &condition,
                &cache_setting,
                None,
            )
            .await;

            assert!(datapoints.is_ok());
            let dataframe = datapoints.unwrap().unwrap();

            {
                let result = dataframe.search(&condition.datapoints_range).await;
                assert!(result.is_some());
                assert_eq!(
                    result.unwrap().into_datapoints().unwrap(),
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
                let another_condition = DatapointsRange::new(
                    Some(TimestampNano::new(1629745451_715063000)),
                    Some(TimestampNano::new(1629745451_715065001)),
                );
                let result = dataframe.search(&another_condition).await;
                assert!(result.is_some());
                assert_eq!(
                    result.unwrap().into_datapoints().unwrap(),
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
    async fn persistence_test_2() {
        let temp_db_dir = TempDir::new("persistence_test_2").unwrap();

        let field_types = vec![FieldType::Float64, FieldType::Float64];
        let metrics: Metrics = "test_metrics".try_into().unwrap();

        let persistence = Persistence::Storage(PathBuf::from(temp_db_dir.path()), None);
        let store = WritableStore::builder(metrics.clone(), field_types)
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

            let result = {
                let mut s = store.lock().await;
                s.push_multi(input_datapoints.clone()).await
            };
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

            let stored_datapoints = {
                let mut s = store.lock().await;
                s.datapoints_mut().await.unwrap().clone()
            };
            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let condition = PersistCondition {
                datapoint_search_condition: DatapointsRange::new(None, None),
                remove_from_store_after_persisted: true,
            };

            let result = {
                let mut s = store.lock().await;
                s.persist(condition).await
            };

            assert!(result.is_ok());
        }

        {
            // retaining datapoints.clone()
            let expected_datapoints = float_data_points!();

            let mut s = store.lock().await;
            let stored_datapoints = s.datapoints_mut().await.unwrap();
            assert_eq!(stored_datapoints.len(), expected_datapoints.len());
            assert_eq!(stored_datapoints.clone(), expected_datapoints);
        }

        {
            let condition = DatapointsSearchCondition {
                datapoints_range: DatapointsRange::new(None, None),
                limit: None,
            };

            let cache_setting = api::CacheSetting::none();

            let datapoints = api::read::search_dataframe(
                "test",
                temp_db_dir.path(),
                &metrics,
                None,
                &condition,
                &cache_setting,
                None,
            )
            .await;

            assert!(datapoints.is_ok());
            let dataframe = datapoints.unwrap().unwrap();

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

                let result = dataframe.search(&condition.datapoints_range).await;
                assert!(result.is_some());

                assert_eq!(result.clone().unwrap().len(), expected.len());
                for (i, each) in result
                    .unwrap()
                    .into_datapoints()
                    .unwrap()
                    .into_iter()
                    .enumerate()
                {
                    assert_eq!(&each, expected.get(i).unwrap());
                }
            }

            {
                let another_condition =
                    DatapointsRange::new(None, Some(TimestampNano::new(1639745451_715061001)));
                let result = dataframe.search(&another_condition).await;
                assert!(result.is_some());
                assert_eq!(
                    result.unwrap().into_datapoints().unwrap(),
                    float_data_points!(
                        {1629745451_715062000, vec![100f64,12f64]},
                        {1629745451_715063000, vec![200f64,36f64]},
                        {1629745451_715064000, vec![200f64,37f64]},
                        {1629745451_715065000, vec![300f64,36f64]},
                        {1629745451_715066000, vec![300f64,36f64]},
                        {1639745451_715061000, vec![1300f64,36f64]}
                    )
                );
            }
        }
    }

    #[tokio::test]
    async fn write_store_limit_test_1() {
        let field_types = vec![FieldType::Float64, FieldType::Float64];
        let metrics: Metrics = "test_metrics".try_into().unwrap();

        let persistence = Persistence::OnMemory;
        let store = WritableStore::builder(metrics.clone(), field_types)
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
                {1639745451_715062000, vec![1200f64,37f64]},
                {1639745451_715062000, vec![1201f64,38f64]}
            );

            let result = {
                let mut s = store.lock().await;
                s.push_multi(input_datapoints.clone()).await
            };
            assert!(result.is_ok());

            {
                let mut s = store.lock().await;
                let datapoints = s.datapoints_tail_limit(2).await.unwrap();

                let expected = float_data_points!(
                    {1639745451_715061000, vec![1300f64,36f64]},
                    {1639745451_715062000, vec![1200f64,37f64]},
                    {1639745451_715062000, vec![1201f64,38f64]}
                );

                assert_eq!(*datapoints, expected);
            }
        }
    }

    #[tokio::test]
    async fn write_store_limit_test_2() {
        let field_types = vec![FieldType::Float64, FieldType::Float64];
        let metrics: Metrics = "test_metrics".try_into().unwrap();

        let persistence = Persistence::OnMemory;
        let store = WritableStore::builder(metrics.clone(), field_types)
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
                {1639745451_715062000, vec![1200f64,37f64]},
                {1639745451_715062000, vec![1201f64,38f64]}
            );

            let result = {
                let mut s = store.lock().await;
                s.push_multi(input_datapoints.clone()).await
            };
            assert!(result.is_ok());

            {
                let mut s = store.lock().await;
                let datapoints = s.datapoints_tail_limit(0).await.unwrap();

                let expected = float_data_points!();

                assert_eq!(*datapoints, expected);
            }
        }
    }

    #[tokio::test]
    async fn write_store_limit_test_3() {
        let field_types = vec![FieldType::Float64, FieldType::Float64];
        let metrics: Metrics = "test_metrics".try_into().unwrap();

        let persistence = Persistence::OnMemory;
        let store = WritableStore::builder(metrics.clone(), field_types)
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
                {1639745451_715062000, vec![1200f64,37f64]},
                {1639745451_715062000, vec![1201f64,38f64]}
            );

            let result = {
                let mut s = store.lock().await;
                s.push_multi(input_datapoints.clone()).await
            };
            assert!(result.is_ok());

            {
                let mut s = store.lock().await;
                let datapoints = s.datapoints_tail_limit(7).await.unwrap();

                let expected = float_data_points!(
                    {1629745451_715062000, vec![100f64,12f64]},
                    {1629745451_715063000, vec![200f64,36f64]},
                    {1629745451_715064000, vec![200f64,37f64]},
                    {1629745451_715065000, vec![300f64,36f64]},
                    {1629745451_715066000, vec![300f64,36f64]},
                    {1639745451_715061000, vec![1300f64,36f64]},
                    {1639745451_715062000, vec![1200f64,37f64]},
                    {1639745451_715062000, vec![1201f64,38f64]}
                );

                assert_eq!(*datapoints, expected);
            }
        }
    }

    #[tokio::test]
    async fn write_store_limit_test_4() {
        let field_types = vec![FieldType::Float64, FieldType::Float64];
        let metrics: Metrics = "test_metrics".try_into().unwrap();

        let persistence = Persistence::OnMemory;
        let store = WritableStore::builder(metrics.clone(), field_types)
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
                {1639745451_715062000, vec![1200f64,37f64]},
                {1639745451_715062000, vec![1201f64,38f64]}
            );

            let result = {
                let mut s = store.lock().await;
                s.push_multi(input_datapoints.clone()).await
            };
            assert!(result.is_ok());

            {
                let mut s = store.lock().await;
                let datapoints = s.datapoints_tail_limit(8).await.unwrap();

                let expected = float_data_points!(
                    {1629745451_715062000, vec![100f64,12f64]},
                    {1629745451_715063000, vec![200f64,36f64]},
                    {1629745451_715064000, vec![200f64,37f64]},
                    {1629745451_715065000, vec![300f64,36f64]},
                    {1629745451_715066000, vec![300f64,36f64]},
                    {1639745451_715061000, vec![1300f64,36f64]},
                    {1639745451_715062000, vec![1200f64,37f64]},
                    {1639745451_715062000, vec![1201f64,38f64]}
                );

                assert_eq!(*datapoints, expected);
            }
        }
    }
}
