use crate::tsdb::storage::api;
use crate::tsdb::*;
use lockfile::Lockfile;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

type Result<T> = std::result::Result<T, StorageApiError>;

pub async fn recreate_block_list_from_cloud<P: AsRef<Path>>(
    database_name: &str,
    db_dir: Option<P>,
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
    let metrics = api::read::fetch_all_metrics(p, cloud_storage_and_setting).await;
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

//async fn override_and_update_block_list_file(
//    db_dir: &Path,
//    metrics: &Metrics,
//    block_list: block_list::BlockList,
//) -> Result<()> {
//    let block_list_file_path = block_list_file_path(db_dir, metrics);
//    block_list::write_to_block_listfile(&block_list_file_path, block_list)?;
//    Ok(())
//}
//
//async fn override_and_update_block_list_file(
//    db_dir: &Path,
//    metrics: &Metrics,
//    block_list: block_list::BlockList,
//) -> Result<()> {
//    let block_list_file_path = block_list_file_path(db_dir, metrics);
//    block_list::write_to_block_listfile(&block_list_file_path, block_list)?;
//    Ok(())
//}
//
//async fn upload_to_cloud_if_need(
//    block_list_file_path: &Path,
//    block_file_path: &Path,
//    metrics: &Metrics,
//    block_timestamp: &block_list::BlockTimestamp,
//    cloud_storage: &CloudStorage,
//) -> Result<()> {
//    {
//        let cloud_block_list_file_path = CloudBlockListFilePath::new(metrics, cloud_storage);
//        cloud_block_list_file_path
//            .upload(block_list_file_path)
//            .await?;
//    }
//
//    Ok(())
//}
