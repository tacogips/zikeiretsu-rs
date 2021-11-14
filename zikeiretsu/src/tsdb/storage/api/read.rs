use super::{
    block_list_dir_path, block_list_file_path, block_timestamp_to_block_file_path,
    cloud_setting::*, lockfile_path, CacheSetting, Result, StorageApiError,
};
use crate::tsdb::{
    cloudstorage::*,
    storage::{block, block_list, cache},
};
use crate::tsdb::{datapoint::*, metrics::Metrics};
use futures::future::join_all;
use lazy_static::lazy_static;
use lockfile::Lockfile;
use log;
use regex::Regex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

lazy_static! {
    static ref CACHE: Arc<RwLock<cache::Cache>> = Arc::new(RwLock::new(cache::Cache::new()));
    static ref LOCAL_BLOCK_LIST_FILE_PATTERN: Regex =
        Regex::new(block_list::BLOCK_LIST_FILE_NAME_PATTERN).unwrap();
}

pub async fn fetch_all_metrics<P: AsRef<Path>>(
    db_dir: Option<P>,
    cloud_setting: Option<&CloudStorageSetting>,
) -> Result<Vec<Metrics>> {
    //TODO(tacogips) need some lock
    if let Some(cloud_setting) = cloud_setting {
        if cloud_setting.update_block_list {
            let block_file_urls =
                CloudBlockListFilePath::list_files_urls(&cloud_setting.cloud_storage).await?;

            let mut result: Vec<Metrics> = vec![];
            for each_block_file_url in block_file_urls.iter() {
                match CloudBlockListFilePath::extract_metrics_from_url(
                    each_block_file_url,
                    &cloud_setting.cloud_storage,
                ) {
                    Ok(metrics) => {
                        result.push(metrics);
                    }
                    Err(e) => {
                        log::warn!("invalid block list file path found when listing metrics. it has skipped. {}",e)
                    }
                }
            }
            return Ok(result);
        }
    };

    if db_dir.is_none() {
        return Err(StorageApiError::DbDirPathRequired(
            "fetch metrics list on local".to_string(),
        ));
    }
    let db_dir = db_dir.unwrap();

    let file_paths = list_local_block_list_files(db_dir.as_ref());

    let mut metrics = Vec::<Metrics>::new();
    for each_file_path in file_paths {
        let each_metrics = extract_metrics_from_file_name(&each_file_path)?;
        metrics.push(each_metrics);
    }

    Ok(metrics)
}

pub(crate) fn extract_metrics_from_file_name(file_name: &str) -> Result<Metrics> {
    let captured = LOCAL_BLOCK_LIST_FILE_PATTERN.captures(file_name);
    if let Some(captured) = captured {
        if let Some(matched) = captured.get(1) {
            return Ok(Metrics::new(matched.as_str()));
        }
    }
    Err(StorageApiError::InvalidBlockListFileName(
        file_name.to_string(),
    ))
}

pub(crate) fn list_local_block_list_files(db_dir: &Path) -> Vec<String> {
    let db_dir = db_dir.as_ref();
    let mut block_list_dir = block_list_dir_path(db_dir);
    let mut file_names = vec![];

    while block_list_dir.pop() {
        if let Some(file_name) = block_list_dir.file_name() {
            if let Some(file_name) = file_name.to_str() {
                file_names.push(file_name.to_string());
            }
        }
    }
    file_names
}

pub async fn search_datas<P: AsRef<Path>>(
    db_dir: P,
    metrics: &Metrics,
    condition: &DatapointSearchCondition,
    cache_setting: &CacheSetting,
    cloud_setting: Option<&CloudStorageSetting>,
) -> Result<Vec<DataPoint>> {
    let db_dir = db_dir.as_ref();
    let lock_file_path = lockfile_path(&db_dir, metrics);
    let _lockfile = Lockfile::create(&lock_file_path)
        .map_err(|e| StorageApiError::AcquireLockError(lock_file_path.display().to_string(), e))?;
    let block_list = read_block_list(db_dir, &metrics, cache_setting, cloud_setting).await?;

    let (since_sec, until_sec) = condition.as_secs();

    let since_sec_ref = (&since_sec).as_ref();
    let until_sec_ref = (&until_sec).as_ref();

    let block_timestamps = block_list.search(since_sec_ref, until_sec_ref)?;

    let result = match block_timestamps {
        None => Ok(vec![]),
        Some(block_timestamps) => {
            if !no_block_timestamps_overlapping_nor_unsorted(block_timestamps) {
                return Err(StorageApiError::UnsupportedStorageStatus("timestamps of datapoints overlapping or unsorted. `zikeiretsu` not supported datas like this yet...".to_string()));
            }

            let tasks = block_timestamps.iter().map(|block_timestamp| {
                read_block(&db_dir, &metrics, &block_timestamp, cloud_setting)
            });

            let data_points_of_blocks = join_all(tasks).await;
            let data_points_of_blocks: Result<Vec<Vec<_>>> =
                data_points_of_blocks.into_iter().collect();

            let data_points_of_blocks: Vec<_> =
                data_points_of_blocks?.into_iter().flatten().collect();

            Ok(data_points_of_blocks)
        }
    };

    result
}

fn no_block_timestamps_overlapping_nor_unsorted(
    block_timestamps: &[block_list::BlockTimestamp],
) -> bool {
    block_timestamps
        .iter()
        .zip(block_timestamps[1..].iter())
        .all(|(l, r)| l.is_before(r) || l == r)
}

async fn read_block(
    root_dir: &Path,
    metrics: &Metrics,
    block_timestamp: &block_list::BlockTimestamp,
    cloud_setting: Option<&CloudStorageSetting>,
) -> Result<Vec<DataPoint>> {
    let block_file_path = block_timestamp_to_block_file_path(root_dir, metrics, block_timestamp);

    if let Some(cloud_setting) = cloud_setting {
        if !block_file_path.exists() {
            if cloud_setting.download_block_if_not_exits {
                let cloud_block_file_path = CloudBlockFilePath::new(
                    &metrics,
                    &block_timestamp,
                    &cloud_setting.cloud_storage,
                );

                let download_result = cloud_block_file_path.download(&block_file_path).await?;

                if download_result.is_none() {
                    return Err(StorageApiError::NoBlockFile(
                        block_file_path.display().to_string(),
                    ));
                }
            } else {
                return Err(StorageApiError::NoBlockFile(
                    block_file_path.display().to_string(),
                ));
            }
        }
    }

    read_from_block_file(&block_file_path)
}

fn read_from_block_file(block_file_path: &PathBuf) -> Result<Vec<DataPoint>> {
    let result = block::read_from_block_file(block_file_path)?;
    Ok(result)
}

pub(crate) async fn read_block_list(
    db_dir: &Path,
    metrics: &Metrics,
    cache_setting: &CacheSetting,
    cloud_setting: Option<&CloudStorageSetting>,
) -> Result<block_list::BlockList> {
    let block_list_path = block_list_file_path(&db_dir, metrics);
    let downloaded_from_cloud = if let Some(cloud_setting) = cloud_setting {
        if cloud_setting.update_block_list
            || (!block_list_path.exists() && cloud_setting.download_block_list_if_not_exits)
        {
            let cloud_block_list_file_path =
                CloudBlockListFilePath::new(&metrics, &cloud_setting.cloud_storage);

            let download_result = cloud_block_list_file_path
                .download(&block_list_path)
                .await?;

            if download_result.is_none() {
                log::warn!("downloading block list failed")
            }
        }
        true
    } else {
        false
    };
    let use_cache = if downloaded_from_cloud {
        false
    } else {
        cache_setting.read_cache
    };

    let block_list = if use_cache {
        let cache = CACHE.read().await;
        let block_list = cache.block_list_cache.get(metrics).await;
        block_list.map(|e| e.clone())
    } else {
        None
    };

    //TODO(tacogips) handle illegal block_list
    // - duplicate
    // - not ordered???

    let block_list = match block_list {
        Some(bl) => bl,
        None => {
            if !block_list_path.exists() {
                //
                //TODO(tacogips) call google cloud hear
                return Err(StorageApiError::NoBlockListFile(metrics.to_string()));
            }
            block_list::read_from_blocklist_file(block_list_path)?
        }
    };

    if cache_setting.write_cache {
        let mut cache = CACHE.write().await;
        cache
            .block_list_cache
            .write(&metrics, block_list.clone())
            .await;
    }

    Ok(block_list)
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::tsdb::metrics::Metrics;
    #[test]
    pub fn extract_metrics_from_file_name_test() {
        let result = extract_metrics_from_file_name("some-met_rics.list");
        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(Metrics::new("some-met_rics"), result);
    }
}
