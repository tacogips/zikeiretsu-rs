use super::super::persisted_error;
use super::{
    block, block_list, block_list_file_path, block_timestamp_to_block_file_path, cloud_setting::*,
    lockfile_path, persisted_error_file_path, Result, StorageApiError,
};

use crate::tsdb::cloudstorage::*;
use crate::tsdb::timestamp_nano::TimestampNano;
use crate::tsdb::{datapoint::*, metrics::Metrics};
use lockfile::Lockfile;
use log;
use log::*;
use std::fs;
use std::fs::create_dir_all;
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub async fn write_datas<P: AsRef<Path>>(
    db_dir: P,
    writer_id: &Uuid,
    metrics: &Metrics,
    data_points: &[DataPoint],
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<()> {
    debug_assert!(!data_points.is_empty());
    debug_assert!(DataPoint::check_datapoints_is_sorted(data_points).is_ok());

    let cloud_infos = if let Some((cloud_storage, cloud_setting)) = cloud_storage_and_setting {
        if cloud_setting.upload_data_after_write {
            let cloud_lock_file_path = CloudLockfilePath::new(metrics, cloud_storage);
            if cloud_lock_file_path.exists().await? {
                return Err(StorageApiError::CreateLockfileError(format!(
                    "cloud lock file already exists at {lock_file_url} ",
                    lock_file_url = cloud_lock_file_path.as_url()
                )));
            } else {
                cloud_lock_file_path.create(writer_id).await?;
                Some((cloud_lock_file_path, cloud_storage, cloud_setting))
            }
        } else {
            None
        }
    } else {
        None
    };

    let cloud_infos_clone = cloud_infos.clone();
    let innner_writer_data = || async move {
        let db_dir = db_dir.as_ref();

        let write = || async {
            let WrittenBlockInfo {
                block_list_file_path,
                block_file_dir,
                block_file_path,
                block_timestamp,
            } = match write_datas_to_local(
                db_dir,
                writer_id,
                metrics,
                data_points,
                cloud_storage_and_setting,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    log::error!("failed to write block file on local: {e}");
                    return Err(e);
                }
            };

            if let Some((_, cloud_storage, cloud_setting)) = cloud_infos_clone.as_ref() {
                let upload_result = upload_to_cloud(
                    &block_list_file_path,
                    &block_file_path,
                    metrics,
                    &block_timestamp,
                    cloud_storage,
                )
                .await;
                match upload_result {
                    Ok(_) => {
                        if cloud_setting.remove_local_file_after_upload {
                            fs::remove_dir_all(block_file_dir.as_path())
                                .map_err(StorageApiError::RemoveBlockDirError)?;
                            log::debug!(
                                "remove block dir on local at {block_file_path}",
                                block_file_path = block_file_dir.as_path().display()
                            );
                        }
                    }
                    Err(e) => {
                        log::error!("failed to update block files to the cloud :{e:?}");
                        write_error_file(
                            db_dir,
                            TimestampNano::now(),
                            metrics,
                            persisted_error::PersistedErrorType::FailedToUploadBlockOrBLockList,
                            block_timestamp,
                            Some(format!("error:{e:?}")),
                        )
                        .await?;
                    }
                }
            }
            Ok(())
        };
        write().await
    };
    let result = innner_writer_data().await;

    if let Some((cloud_lock_file_path, _, _)) = cloud_infos {
        cloud_lock_file_path.remove().await?;
    }

    result
}

pub async fn remove_cloud_lock_file_if_same_writer(
    writer_id: &Uuid,
    metrics: &Metrics,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<()> {
    if let Some((cloud_storage, _)) = cloud_storage_and_setting {
        let cloud_lock_file_path = CloudLockfilePath::new(metrics, cloud_storage);
        if cloud_lock_file_path.exists().await? {
            let lock_file_writer_id = cloud_lock_file_path.read_contents().await?;
            if let Some(lock_file_writer_id) = lock_file_writer_id {
                if lock_file_writer_id == *writer_id {
                    cloud_lock_file_path.remove().await?;
                }
            }
        }
    };
    Ok(())
}

struct WrittenBlockInfo {
    block_list_file_path: PathBuf,
    block_file_dir: PathBuf,
    block_file_path: PathBuf,
    block_timestamp: block_list::BlockTimestamp,
}

async fn write_datas_to_local(
    db_dir: &Path,
    writer_id: &Uuid,
    metrics: &Metrics,
    data_points: &[DataPoint],
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<WrittenBlockInfo> {
    let lock_file_path = lockfile_path(db_dir, metrics);
    let mut lockfile = Lockfile::create(&lock_file_path)
        .map_err(|e| StorageApiError::AcquireLockError(lock_file_path.display().to_string(), e))?;
    lockfile.write_all(writer_id.as_bytes()).map_err(|e| {
        StorageApiError::CreateLockfileError(format!(
            "could not write writer id to lock file {:?}, error:{}, path:{:?}",
            writer_id, e, lock_file_path
        ))
    })?;

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
        let block_list = super::read::read_block_list(
            "", // buggy: database name is unnecessary if no cache wil be used
            db_dir,
            metrics,
            &cache_setting,
            cloud_storage_and_setting,
        )
        .await;

        let mut block_list = match block_list {
            Ok(block_list) => block_list,
            Err(StorageApiError::NoBlockListFile(_)) => {
                block_list::BlockList::new(metrics.clone(), TimestampNano::now(), vec![])
            }
            Err(e) => return Err(e),
        };

        block_list.add_timestamp(block_timestamp)?;
        block_list.update_updated_at(TimestampNano::now());

        let block_list_file_path = block_list_file_path(db_dir, metrics);
        block_list::write_to_block_listfile(&block_list_file_path, block_list)?;
        block_list_file_path
    };

    // write block file
    let (block_file_dir, block_file_path) = {
        let (block_file_dir, block_file_path) =
            block_timestamp_to_block_file_path(db_dir, metrics, &block_timestamp);
        if block_file_path.exists() {
            return Err(StorageApiError::UnsupportedStorageStatus(format!(
                "block file already exists at {block_file_path}. merging block files is not supported yet...",
                block_file_path =block_file_path.display()
            )));
        }

        create_dir_all(block_file_dir.as_path()).map_err(StorageApiError::CreateBlockFileError)?;
        block::write_to_block_file(&block_file_path, data_points)?;
        (block_file_dir, block_file_path)
    };
    Ok(WrittenBlockInfo {
        block_list_file_path,
        block_file_dir,
        block_file_path,
        block_timestamp,
    })
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
        Some(block_timestamp),
        detail,
    );

    let error_file_path = persisted_error_file_path(db_dir, &error_time);
    persisted_error::write_persisted_error(error_file_path, err)?;
    Ok(())
}
