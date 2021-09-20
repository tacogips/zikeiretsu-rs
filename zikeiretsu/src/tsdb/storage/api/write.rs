use super::super::persisted_error;
use super::{
    block, block_list, block_list_file_path, block_timestamp_to_block_file_path, cloud_setting::*,
    lockfile_path, persisted_error_file_path, CacheSetting, Result, StorageApiError,
};

use crate::tsdb::cloudstorage::*;
use crate::tsdb::timestamp_nano::TimestampNano;
use crate::tsdb::{datapoint::*, metrics::Metrics};
use lockfile::Lockfile;
use log;
use log::*;
use std::fs::create_dir_all;
use std::path::Path;

pub async fn write_datas<P: AsRef<Path>>(
    db_dir: P,
    metrics: &Metrics,
    data_points: &[DataPoint],
    cloud_setting: Option<&CloudSetting>,
) -> Result<()> {
    debug_assert!(!data_points.is_empty());
    debug_assert!(DataPoint::check_datapoints_is_sorted(&data_points).is_ok());

    let cloud_lock_file_path = if let Some(cloud_setting) = cloud_setting {
        if cloud_setting.upload_data_after_write {
            let cloud_lock_file_path =
                CloudLockfilePath::new(metrics, &cloud_setting.cloud_storage);
            if cloud_lock_file_path.exists().await? {
                return Err(StorageApiError::CreateLockfileError(format!(
                    "cloud lock file already exists at {} ",
                    cloud_lock_file_path.as_url()
                )));
            } else {
                cloud_lock_file_path.create().await?;
                Some(cloud_lock_file_path)
            }
        } else {
            None
        }
    } else {
        None
    };

    let db_dir = db_dir.as_ref();
    let lock_file_path = lockfile_path(&db_dir, metrics);
    let _lockfile = Lockfile::create(&lock_file_path)
        .map_err(|e| StorageApiError::AcquireLockError(lock_file_path.display().to_string(), e))?;

    let head = data_points.get(0).unwrap();
    let tail = data_points.get(data_points.len() - 1).unwrap();

    let block_timestamp = block_list::BlockTimestamp {
        since_sec: head.timestamp_nano.as_timestamp_sec(),
        until_sec: tail.timestamp_nano.as_timestamp_sec() + 1,
    };

    let cache_setting = super::CacheSetting {
        read_cache: false,
        write_cache: false,
    };

    // write block list file first
    let block_list_file_path = {
        let mut block_list =
            super::read::read_block_list(db_dir, &metrics, &cache_setting, cloud_setting).await?;

        block_list.add_timestamp(block_timestamp)?;
        block_list.update_updated_at(TimestampNano::now());

        let block_list_file_path = block_list_file_path(&db_dir, &metrics);
        block_list::write_to_block_listfile(&block_list_file_path, block_list)?;
        block_list_file_path
    };

    // write block file
    let block_file_path = {
        let block_file_path =
            block_timestamp_to_block_file_path(db_dir, &metrics, &block_timestamp);
        if block_file_path.exists() {
            return Err(StorageApiError::UnsupportedStorageStatus(format!(
                "block file already exists at {}. merging block files is not supported yet...",
                block_file_path.display()
            )));
        }

        let block_file_dir = block_file_path.parent().unwrap();
        create_dir_all(block_file_dir).map_err(|e| StorageApiError::CreateBlockFileError(e))?;
        block::write_to_block_file(&block_file_path, &data_points)?;
        block_file_path
    };

    if let Some(cloud_lock_file_path) = cloud_lock_file_path {
        if let Err(e) = upload_to_cloud(
            &block_list_file_path,
            &block_file_path,
            &metrics,
            &block_timestamp,
            &cloud_setting.unwrap().cloud_storage,
        )
        .await
        {
            log::error!("failed to update block files to the cloud :{:?}", e);
            write_error_file(
                db_dir,
                TimestampNano::now(),
                &metrics,
                persisted_error::PersistedErrorType::FailedToUploadBlockOrBLockList,
                block_timestamp,
                Some(format!("error:{:?}", e)),
            )
            .await?;
        }

        cloud_lock_file_path.remove().await?
    }

    Ok(())
}

async fn upload_to_cloud(
    block_list_file_path: &Path,
    block_file_path: &Path,
    metrics: &Metrics,
    block_timestamp: &block_list::BlockTimestamp,
    cloud_storage: &CloudStorage,
) -> Result<()> {
    {
        let cloud_block_file_path =
            CloudBlockFilePath::new(metrics, block_timestamp, cloud_storage);
        cloud_block_file_path.upload(block_file_path).await?;
    }

    {
        let cloud_block_list_file_path = CloudBlockListFilePath::new(metrics, cloud_storage);
        cloud_block_list_file_path
            .upload(block_list_file_path)
            .await?;
    }

    Ok(())
}

async fn write_error_file(
    db_dir: &Path,
    error_time: TimestampNano,
    metrics: &Metrics,
    error_type: persisted_error::PersistedErrorType,
    block_timestamp: block_list::BlockTimestamp,
    detail: Option<String>,
) -> Result<()> {
    let err = persisted_error::PersistedError::new(
        error_time,
        Some(metrics.clone()),
        error_type,
        Some(block_timestamp.clone()),
        detail,
    );

    let error_file_path = persisted_error_file_path(db_dir, &error_time);
    persisted_error::write_persisted_error(error_file_path, err);
    Ok(())
}
