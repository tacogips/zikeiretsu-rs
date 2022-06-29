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
use lockfile::Lockfile;
use log;
use once_cell::sync::{Lazy, OnceCell};
use regex::Regex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use walkdir::WalkDir;

static CACHE: OnceCell<Arc<RwLock<cache::Cache>>> = OnceCell::new();
static LOCAL_BLOCK_LIST_FILE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(block_list::BLOCK_LIST_FILE_NAME_PATTERN).unwrap());

fn shared_cache() -> Arc<RwLock<cache::Cache>> {
    let block_cache_size = std::env::var("ZDB_BLOCK_CACHE_SIZE")
        .unwrap_or_else(|_| "1000".to_string())
        .parse::<usize>()
        .unwrap_or(1000);
    let cached = CACHE.get_or_init(|| Arc::new(RwLock::new(cache::Cache::new(block_cache_size))));
    cached.clone()
}

pub async fn fetch_all_metrics<P: AsRef<Path>>(
    db_dir: Option<P>,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<Vec<Metrics>> {
    let local_block_file_paths_is_empty = db_dir
        .as_ref()
        .map(|db_dir| list_local_block_list_files(db_dir.as_ref()).is_empty())
        .unwrap_or(true);

    log::debug!("fetch all metrics  cloud storage setting:{cloud_storage_and_setting:?}");
    if let Some((cloud_storage, cloud_setting)) = cloud_storage_and_setting {
        if cloud_setting.force_update_block_list || local_block_file_paths_is_empty {
            let block_file_urls = CloudBlockListFilePath::list_files_urls(cloud_storage).await?;

            let mut result: Vec<Metrics> = vec![];

            for each_block_file_url in block_file_urls.iter() {
                match CloudBlockListFilePath::extract_metrics_from_url(
                    each_block_file_url,
                    cloud_storage,
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

fn filter_block_metas_by_limit<'a>(
    block_metas: &'a [block_list::BlockMetaInfo],
    limit: &SearchDatapointsLimit,
) -> &'a [block_list::BlockMetaInfo] {
    //TODO(tacogips) consider about wrapping case
    //  time range ==> [TR]
    //
    //  |(TR 1)  | (TR 2)  |  (TR 3)  |
    //        | (TR 4)                     |
    //  When the ranges as above. Given that the time ranges are sorted by begining datetime, the tail block supposed to be TR 3 butactually it should be TR 4

    if block_metas.is_empty() {
        block_metas
    } else {
        let (n, is_head) = match limit {
            SearchDatapointsLimit::Head(n) => (*n, true),
            SearchDatapointsLimit::Tail(n) => (*n, false),
        };
        if n == 1 {
            if is_head && block_metas[0].timestamp_num > 1 {
                return &block_metas[..1];
            } else if !is_head && block_metas[block_metas.len() - 1].timestamp_num > 1 {
                return &block_metas[block_metas.len() - 1..];
            }
        }

        let mut range: Vec<usize> = (0..=(block_metas.len() - 1)).into_iter().collect();
        if !is_head {
            range.reverse();
        }

        let mut timestamp_num_sum = 0;

        for idx in range {
            timestamp_num_sum += block_metas[idx].timestamp_num;

            if timestamp_num_sum >= n {
                // return with continuguous block
                // in the case next block start or ends with the same timestamp
                if is_head {
                    if timestamp_num_sum == n && idx < (block_metas.len() - 1) {
                        return &block_metas[..idx + 2];
                    } else {
                        return &block_metas[..idx + 1];
                    }
                } else {
                    if timestamp_num_sum == n && idx > 0 {
                        return &block_metas[idx - 1..];
                    } else {
                        return &block_metas[idx..];
                    }
                }
            }
        }

        block_metas
    }
}

pub async fn search_dataframe<P: AsRef<Path>>(
    database_name: &str,
    db_dir: P,
    metrics: &Metrics,
    field_selectors: Option<&[usize]>,
    condition: &DatapointsSearchCondition,
    cache_setting: &CacheSetting,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<Option<TimeSeriesDataFrame>> {
    log::debug!("search_dataframe. seaching db_dir: {:?}", db_dir.as_ref());
    log::debug!("search_dataframe. field_selectors: {:?}", field_selectors);
    log::debug!("search_dataframe. condition: {:?}", condition);

    let db_dir = db_dir.as_ref();
    let lock_file_path = lockfile_path(db_dir, metrics);
    let _lockfile = Lockfile::create(&lock_file_path)
        .map_err(|e| StorageApiError::AcquireLockError(lock_file_path.display().to_string(), e))?;
    let block_list = read_block_list(
        database_name,
        db_dir,
        metrics,
        cache_setting,
        cloud_storage_and_setting,
    )
    .await?;

    let (since_sec, until_sec) = condition.datapoints_range.as_secs();

    let since_sec_ref = (&since_sec).as_ref();
    let until_sec_ref = (&until_sec).as_ref();

    log::debug!(
        "search_dataframe. block search range: ({:?} : {:?})",
        since_sec_ref,
        until_sec_ref
    );

    let block_metas = block_list.search(since_sec_ref, until_sec_ref)?;

    log::debug!("search_dataframe. block timestamps: {:?}", block_metas);

    let result = match block_metas {
        None => Ok(None),
        Some(mut block_metas) => {
            if let Some(limit) = condition.limit.as_ref() {
                block_metas = filter_block_metas_by_limit(block_metas, limit)
            }

            let tasks = block_metas.iter().map(|block_meta| async move {
                let mut block = read_block(
                    database_name,
                    db_dir,
                    metrics,
                    field_selectors,
                    &block_meta.block_timestamp,
                    cache_setting,
                    cloud_storage_and_setting,
                )
                .await?;
                // cut out partial datas from the dataframe
                if !condition.datapoints_range.contains_whole(
                    &block_meta.block_timestamp.since_sec.as_timestamp_nano(),
                    &(block_meta.block_timestamp.until_sec + 1).as_timestamp_nano(),
                ) {
                    block.retain_matches(&condition.datapoints_range).await?;
                }

                Ok((block, &block_meta.block_timestamp))
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
                        || prev_block_timestamp.is_adjacent_before_of(each_block_timestamp)
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

                if let Some(limit) = condition.limit.as_ref() {
                    merged_dataframe.limit(limit);
                }
                Ok(Some(merged_dataframe))
            }
        }
    };

    result
}

pub async fn read_block(
    database_name: &str,
    root_dir: &Path,
    metrics: &Metrics,
    field_selectors: Option<&[usize]>,
    block_timestamp: &block_list::BlockTimestamp,
    cache_setting: &CacheSetting,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<TimeSeriesDataFrame> {
    log::debug!("reading block file metrics:{metrics} ,timestamps:{block_timestamp}");
    let (_, block_file_path) =
        block_timestamp_to_block_file_path(root_dir, metrics, block_timestamp);

    let mut block_file_downloaded = false;
    if let Some((cloud_storage, cloud_setting)) = cloud_storage_and_setting {
        if !block_file_path.exists() {
            if cloud_setting.download_block_if_not_exits {
                let cloud_block_file_path =
                    CloudBlockFilePath::new(metrics, block_timestamp, cloud_storage);

                let download_result = cloud_block_file_path.download(&block_file_path).await?;
                block_file_downloaded = true;

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

    let cached_df = if cache_setting.read_cache && !block_file_downloaded {
        let s_cache = shared_cache();
        let mut cache = s_cache.write().await;
        let cached_df = cache
            .block_cache
            .get(database_name.to_string(), metrics.clone(), *block_timestamp)
            .await;
        cached_df.cloned()
    } else {
        None
    };

    let read_df = match cached_df {
        Some(cached_df) => {
            log::debug!("block cache hit {},{}", metrics, block_timestamp);
            cached_df
        }
        None => read_from_block_file(&block_file_path, field_selectors)?,
    };

    if cache_setting.write_cache {
        let s_cache = shared_cache();
        let mut cache = s_cache.write().await;
        cache
            .block_cache
            .write(
                database_name.to_string(),
                metrics.clone(),
                *block_timestamp,
                read_df.clone(),
            )
            .await;
    }

    Ok(read_df)
}

fn read_from_block_file(
    block_file_path: &PathBuf,
    field_selectors: Option<&[usize]>,
) -> Result<TimeSeriesDataFrame> {
    let result = block::read_from_block_file(block_file_path, field_selectors)?;
    Ok(result)
}

pub(crate) async fn read_block_list(
    database_name: &str,
    db_dir: &Path,
    metrics: &Metrics,
    cache_setting: &CacheSetting,
    cloud_storage_and_setting: Option<(&CloudStorage, &CloudStorageSetting)>,
) -> Result<block_list::BlockList> {
    let block_list_path = block_list_file_path(db_dir, metrics);
    let downloaded_from_cloud = if let Some((cloud_storage, cloud_setting)) =
        cloud_storage_and_setting
    {
        if cloud_setting.force_update_block_list
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
    let use_read_cache = if downloaded_from_cloud {
        false
    } else {
        cache_setting.read_cache
    };

    let block_list = if use_read_cache {
        let s_cache = shared_cache();
        let cache = s_cache.read().await;
        let block_list = cache
            .block_list_cache
            .get(database_name.to_string(), metrics.clone())
            .await;
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
        let s_cache = shared_cache();
        let mut cache = s_cache.write().await;
        cache
            .block_list_cache
            .write(
                database_name.to_string(),
                metrics.clone(),
                block_list.clone(),
            )
            .await;
    }

    Ok(block_list)
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::tsdb::metrics::Metrics;
    use crate::tsdb::*;

    #[test]
    pub fn extract_metrics_from_file_name_test() {
        let result = extract_metrics_from_file_name("some-met_rics.list");
        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(Metrics::new("some-met_rics").unwrap(), result);
    }

    macro_rules! blmeta {
        ($since:expr,$until:expr,$num:expr) => {
            block_list::BlockMetaInfo::new(
                block_list::BlockTimestamp::new(
                    TimestampSec::new($since),
                    TimestampSec::new($until),
                ),
                $num,
            )
        };
    }

    macro_rules! block_metas {
        ($({$since:expr,$until:expr,$num:expr}),*) => {
            vec![
                $(blmeta!($since,$until, $num) ),*
            ]
        };
    }

    #[test]
    fn test_filter_blocklist_1() {
        let block_metas = block_metas!({10,20,2}, {21,30,4}, {31,40,5});

        let result = filter_block_metas_by_limit(&block_metas, &SearchDatapointsLimit::Head(1));
        assert_eq!(result, block_metas!({10,20,2}));

        let result = filter_block_metas_by_limit(&block_metas, &SearchDatapointsLimit::Tail(1));
        assert_eq!(result, block_metas!({31,40,5}));
    }

    #[test]
    fn test_filter_blocklist_2() {
        let block_metas = block_metas!({10,20,3}, {21,30,4}, {31,40,3});

        let result = filter_block_metas_by_limit(&block_metas, &SearchDatapointsLimit::Head(2));
        assert_eq!(result, block_metas!({10,20,3}));

        let result = filter_block_metas_by_limit(&block_metas, &SearchDatapointsLimit::Tail(2));
        assert_eq!(result, block_metas!({31,40,3}));
    }

    #[test]
    fn test_filter_blocklist_3() {
        let block_metas = block_metas!({10,20,3}, {21,30,4}, {31,40,3});

        let result = filter_block_metas_by_limit(&block_metas, &SearchDatapointsLimit::Head(3));
        assert_eq!(result, block_metas!({10,20,3},{21,30,4}));

        let result = filter_block_metas_by_limit(&block_metas, &SearchDatapointsLimit::Tail(3));
        assert_eq!(result, block_metas!({21,30,4},{31,40,3}));
    }

    #[test]
    fn test_filter_blocklist_4() {
        let block_metas = block_metas!({10,20,3}, {21,30,4}, {31,40,3});

        let result = filter_block_metas_by_limit(&block_metas, &SearchDatapointsLimit::Head(4));
        assert_eq!(result, block_metas!({10,20,3},{21,30,4}));

        let result = filter_block_metas_by_limit(&block_metas, &SearchDatapointsLimit::Tail(4));
        assert_eq!(result, block_metas!({21,30,4},{31,40,3}));
    }
}
