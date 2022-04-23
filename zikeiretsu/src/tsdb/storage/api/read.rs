use super::{
    block_list_dir_path, block_list_file_path, block_timestamp_to_block_file_path,
    cloud_setting::*, lockfile_path, CacheSetting, Result, StorageApiError,
};
use crate::tsdb::{
    cloudstorage::*,
    storage::{block, block_list, cache},
};
use crate::tsdb::{datapoint::*, metrics::Metrics, time_series_dataframe::*};
use futures::future::join_all;
use lazy_static::lazy_static;
use lockfile::Lockfile;
use log;
use regex::Regex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use walkdir::WalkDir;

lazy_static! {
    static ref CACHE: Arc<RwLock<cache::Cache>> = Arc::new(RwLock::new(cache::Cache::new()));
    static ref LOCAL_BLOCK_LIST_FILE_PATTERN: Regex =
        Regex::new(block_list::BLOCK_LIST_FILE_NAME_PATTERN).unwrap();
}

pub async fn fetch_all_metrics<P: AsRef<Path>>(
    db_dir: Option<P>,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<Vec<Metrics>> {
    //TODO(tacogips) need some lock
    if let Some((cloud_storage, cloud_setting)) = cloud_storage_and_setting {
        if cloud_setting.update_block_list {
            let block_file_urls = CloudBlockListFilePath::list_files_urls(&cloud_storage).await?;

            let mut result: Vec<Metrics> = vec![];

            for each_block_file_url in block_file_urls.iter() {
                match CloudBlockListFilePath::extract_metrics_from_url(
                    each_block_file_url,
                    &cloud_storage,
                ) {
                    Ok(metrics) => {
                        result.push(metrics);
                    }
                    Err(e) => {
                        log::warn!("invalid block list file path found when listing metrics. it has skipped. {e}")
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
            let metrics =
                Metrics::new(matched.as_str()).map_err(StorageApiError::InvalidMetricsName)?;
            return Ok(metrics);
        }
    }
    Err(StorageApiError::InvalidBlockListFileName(
        file_name.to_string(),
    ))
}

pub(crate) fn list_local_block_list_files(db_dir: &Path) -> Vec<String> {
    let block_list_dir = block_list_dir_path(db_dir);
    let mut file_names = vec![];

    for each_entry in WalkDir::new(block_list_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let file_name = each_entry.file_name();
        if let Some(file_name) = file_name.to_str() {
            if file_name.ends_with(".list") {
                file_names.push(file_name.to_string());
            }
        }
    }
    file_names
}

pub async fn search_dataframe<P: AsRef<Path>>(
    db_dir: P,
    metrics: &Metrics,
    field_selectors: Option<&[usize]>,
    condition: &DatapointSearchCondition,
    cache_setting: &CacheSetting,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<Option<TimeSeriesDataFrame>> {
    log::debug!("search_dataframe. field_selectors: {:?}", field_selectors);
    log::debug!("search_dataframe. condition: {}", condition);

    let db_dir = db_dir.as_ref();
    let lock_file_path = lockfile_path(db_dir, metrics);
    let _lockfile = Lockfile::create(&lock_file_path)
        .map_err(|e| StorageApiError::AcquireLockError(lock_file_path.display().to_string(), e))?;
    let block_list =
        read_block_list(db_dir, metrics, cache_setting, cloud_storage_and_setting).await?;

    let (since_sec, until_sec) = condition.as_secs();

    let since_sec_ref = (&since_sec).as_ref();
    let until_sec_ref = (&until_sec).as_ref();

    log::debug!(
        "search_dataframe. block search range: ({:?} : {:?})",
        since_sec_ref,
        until_sec_ref
    );

    let block_timestamps = block_list.search(since_sec_ref, until_sec_ref)?;

    log::debug!("search_dataframe. block timestamps: {:?}", block_timestamps);

    let result = match block_timestamps {
        None => Ok(None),
        Some(block_timestamps) => {
            let tasks = block_timestamps.iter().map(|block_timestamp| async move {
                let mut block = read_block(
                    db_dir,
                    metrics,
                    field_selectors,
                    block_timestamp,
                    cloud_storage_and_setting,
                )
                .await?;
                // cut out partial datas from the dataframe
                if !condition.contains_whole(
                    &block_timestamp.since_sec.as_timestamp_nano(),
                    &(block_timestamp.until_sec + 1).as_timestamp_nano(),
                ) {
                    block.retain_matches(condition).await?;
                }

                Ok((block, block_timestamp))
            });

            let dataframes_of_blocks = join_all(tasks).await;
            let dataframes_of_blocks: Result<
                Vec<(TimeSeriesDataFrame, &block_list::BlockTimestamp)>,
            > = dataframes_of_blocks.into_iter().collect();

            let mut dataframes_of_blocks = dataframes_of_blocks?;
            if dataframes_of_blocks.is_empty() {
                Ok(None)
            } else {
                let (mut merged_dataframe, mut prev_block_timestamp) =
                    dataframes_of_blocks.remove(0);

                for (mut each_dataframes_block, each_block_timestamp) in
                    dataframes_of_blocks.into_iter()
                {
                    if prev_block_timestamp.is_before(each_block_timestamp)
                        || prev_block_timestamp.adjacent_before_of(each_block_timestamp)
                    {
                        merged_dataframe.append(&mut each_dataframes_block).unwrap();
                    } else {
                        merged_dataframe
                            .merge(&mut each_dataframes_block)
                            .await
                            .unwrap();
                    }

                    prev_block_timestamp = each_block_timestamp;
                }

                Ok(Some(merged_dataframe))
            }
        }
    };

    result
}

async fn read_block(
    root_dir: &Path,
    metrics: &Metrics,
    field_selectors: Option<&[usize]>,
    block_timestamp: &block_list::BlockTimestamp,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<TimeSeriesDataFrame> {
    let (_, block_file_path) =
        block_timestamp_to_block_file_path(root_dir, metrics, block_timestamp);

    if let Some((cloud_storage, cloud_setting)) = cloud_storage_and_setting {
        if !block_file_path.exists() {
            if cloud_setting.download_block_if_not_exits {
                let cloud_block_file_path =
                    CloudBlockFilePath::new(metrics, block_timestamp, cloud_storage);

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

    read_from_block_file(&block_file_path, field_selectors)
}

fn read_from_block_file(
    block_file_path: &PathBuf,
    field_selectors: Option<&[usize]>,
) -> Result<TimeSeriesDataFrame> {
    let result = block::read_from_block_file(block_file_path, field_selectors)?;
    Ok(result)
}

pub(crate) async fn read_block_list<'a>(
    db_dir: &Path,
    metrics: &Metrics,
    cache_setting: &CacheSetting,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<block_list::BlockList> {
    let block_list_path = block_list_file_path(db_dir, metrics);
    let downloaded_from_cloud = if let Some((cloud_storage, cloud_setting)) =
        cloud_storage_and_setting
    {
        if cloud_setting.update_block_list
            || (!block_list_path.exists() && cloud_setting.download_block_list_if_not_exits)
        {
            let cloud_block_list_file_path = CloudBlockListFilePath::new(metrics, cloud_storage);

            let download_result = cloud_block_list_file_path
                .download(&block_list_path)
                .await?;

            if download_result.is_none() {
                log::warn!("downloading block list failed.metrics: {metrics}")
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
        block_list.cloned()
    } else {
        None
    };

    let block_list = match block_list {
        Some(bl) => bl,
        None => {
            if !block_list_path.exists() {
                //TODO(tacogips) fetch from  cloud storage hear??
                return Err(StorageApiError::NoBlockListFile(metrics.to_string()));
            }
            block_list::read_from_blocklist_file(metrics, block_list_path)?
        }
    };

    if cache_setting.write_cache {
        let mut cache = CACHE.write().await;
        cache
            .block_list_cache
            .write(metrics, block_list.clone())
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

        assert_eq!(Metrics::new("some-met_rics").unwrap(), result);
    }
}
