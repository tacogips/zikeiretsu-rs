use crate::tsdb::storage::api;
use crate::tsdb::*;
use lockfile::Lockfile;
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

type Result<T> = std::result::Result<T, StorageApiError>;

pub async fn repair_block_list_file<P: AsRef<Path>>(
    database_name: &str,
    db_dir: P,
    cloud_storage: Option<&CloudStorage>,
) -> Result<()> {
    let cloud_setting = CloudStorageSetting::builder()
        .force_update_block_list(true)
        .download_block_if_not_exits(false)
        .download_block_if_not_exits(false)
        .upload_data_after_write(false)
        .build();

    let cloud_storage_and_setting =
        cloud_storage.map(|cloud_storage| (cloud_storage, &cloud_setting));
    let p: Option<PathBuf> = None;
    let metricses = api::read::fetch_all_metrics(p, cloud_storage_and_setting.clone()).await?;

    for each_metrics in metricses.into_iter() {
        if let Some(bloken_block_list) = validate_block_list(
            db_dir.as_ref(),
            database_name,
            &each_metrics,
            cloud_storage_and_setting.clone(),
        )
        .await?
        {
            override_and_update_block_list_file(
                db_dir.as_ref(),
                &each_metrics,
                bloken_block_list,
                cloud_storage_and_setting.map(|(cloud_storage, _)| cloud_storage),
            )
            .await?;
        }
    }

    Ok(())
}

type NeedRepairBlockList = Option<block_list::BlockList>;

async fn validate_block_list(
    db_dir: &Path,
    database_name: &str,
    metrics: &Metrics,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<NeedRepairBlockList> {
    let cache_setting = CacheSetting::none();
    let mut block_list = api::read::read_block_list(
        database_name,
        db_dir,
        metrics,
        &cache_setting,
        cloud_storage_and_setting,
    )
    .await?;

    let mut broken_timestamps: Vec<block_list::BlockTimestamp> = vec![];
    for block_timestamp in block_list.block_timestamps.iter() {
        let block = api::read::read_block(
            database_name,
            db_dir,
            metrics,
            None,
            block_timestamp,
            &cache_setting,
            cloud_storage_and_setting,
        )
        .await;

        if block.is_err() {
            broken_timestamps.push(block_timestamp.clone())
        }
    }

    if broken_timestamps.is_empty() {
        Ok(None)
    } else {
        let broken_timstamps: HashSet<block_list::BlockTimestamp> =
            broken_timestamps.into_iter().collect();
        block_list
            .block_timestamps
            .retain(|block_timestamp| !broken_timstamps.contains(block_timestamp));
        Ok(Some(block_list))
    }
}

async fn override_and_update_block_list_file(
    db_dir: &Path,
    metrics: &Metrics,
    block_list: block_list::BlockList,
    cloud_storage: Option<&CloudStorage>,
) -> Result<()> {
    let block_list_file_path = block_list_file_path(db_dir, metrics);

    let lock_file_path = lockfile_path(db_dir, metrics);
    let mut lockfile = Lockfile::create(&lock_file_path)
        .map_err(|e| StorageApiError::AcquireLockError(lock_file_path.display().to_string(), e))?;
    let writer_id = Uuid::new_v4().to_string();
    //TODO(tacogips) modulize lockfile
    lockfile
        .write_all(writer_id.to_string().as_bytes())
        .map_err(|e| {
            StorageApiError::CreateLockfileError(format!(
                "could not write writer id to lock file {:?}, error:{}, path:{:?}",
                writer_id, e, lock_file_path
            ))
        })?;

    block_list::write_to_block_listfile(&block_list_file_path, block_list)?;

    if let Some(cloud_storage) = cloud_storage {
        upload_to_cloud(block_list_file_path.as_path(), metrics, cloud_storage).await?;
    }

    Ok(())
}

async fn upload_to_cloud(
    block_list_file_path: &Path,
    metrics: &Metrics,
    cloud_storage: &CloudStorage,
) -> Result<()> {
    let cloud_lock_file_path = CloudLockfilePath::new(metrics, cloud_storage);

    let cloud_block_list_file_path = CloudBlockListFilePath::new(metrics, cloud_storage);
    let upload = || async move {
        cloud_block_list_file_path
            .upload(block_list_file_path)
            .await?;
        Ok(())
    };
    let result = upload().await;

    cloud_lock_file_path.remove().await?;

    result
}
