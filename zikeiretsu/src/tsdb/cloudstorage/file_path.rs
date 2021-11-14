use super::gcp;
use super::{CloudStorage, Result};
use crate::tsdb::metrics::Metrics;
use crate::tsdb::storage::block_list;
use std::path::Path;

#[derive(Debug)]
pub struct CloudBlockFilePath<'a> {
    metrics: &'a Metrics, //TODO(tacogips) tobe reference
    block_timestamp: &'a block_list::BlockTimestamp,
    cloud_storage: &'a CloudStorage,
}

impl<'a> CloudBlockFilePath<'a> {
    pub(crate) fn new(
        metrics: &'a Metrics,
        block_timestamp: &'a block_list::BlockTimestamp,
        cloud_storage: &'a CloudStorage,
    ) -> Self {
        Self {
            metrics,
            block_timestamp,
            cloud_storage,
        }
    }

    pub fn as_url(&self) -> String {
        let timestamp_head: u64 = self.block_timestamp.since_sec.0 / (10u64.pow(5u32));

        let block_path = format!(
            "block/{}/{}/{}_{}/block",
            self.metrics,
            timestamp_head,
            self.block_timestamp.since_sec,
            self.block_timestamp.until_sec,
        );

        format!("{}{}", self.cloud_storage.as_url(), block_path)
    }

    pub async fn upload(&self, src: &Path) -> Result<()> {
        match self.cloud_storage {
            CloudStorage::Gcp(_, _) => gcp::upload_block_file(src, &self).await,
        }
    }

    pub async fn download(&self, dest: &Path) -> Result<Option<()>> {
        match self.cloud_storage {
            CloudStorage::Gcp(_, _) => gcp::download_block_file(&self, dest).await,
        }
    }
}

#[derive(Debug)]
pub struct CloudBlockListFilePath<'a> {
    metrics: &'a Metrics, //TODO(tacogips) tobe reference
    cloud_storage: &'a CloudStorage,
}

impl<'a> CloudBlockListFilePath<'a> {
    pub(crate) fn new(metrics: &'a Metrics, cloud_storage: &'a CloudStorage) -> Self {
        Self {
            metrics,
            cloud_storage,
        }
    }

    pub(crate) fn extract_metrics_from_url(
        url: &str,
        cloud_storage: &CloudStorage,
    ) -> Result<Metrics> {
        match cloud_storage {
            CloudStorage::Gcp(_, _) => gcp::extract_metrics_from_url(url),
        }
    }

    pub(crate) async fn list_files_urls(cloud_storage: &CloudStorage) -> Result<Vec<String>> {
        match cloud_storage {
            CloudStorage::Gcp(_, _) => gcp::list_block_list_files(cloud_storage.as_url()).await,
        }
    }

    pub fn as_url(&self) -> String {
        let path = format!("{}.list", self.metrics);
        format!("{}blocklist/{}", self.cloud_storage.as_url(), path)
    }

    pub async fn upload(&self, src: &Path) -> Result<()> {
        match self.cloud_storage {
            CloudStorage::Gcp(_, _) => gcp::upload_block_list_file(src, &self).await,
        }
    }

    pub async fn download(&self, dest: &Path) -> Result<Option<()>> {
        match self.cloud_storage {
            CloudStorage::Gcp(_, _) => gcp::download_block_list_file(&self, dest).await,
        }
    }
}

pub struct CloudLockfilePath<'a> {
    metrics: &'a Metrics, //TODO(tacogips) tobe reference
    cloud_storage: &'a CloudStorage,
}

impl<'a> CloudLockfilePath<'a> {
    pub(crate) fn new(metrics: &'a Metrics, cloud_storage: &'a CloudStorage) -> Self {
        Self {
            metrics,
            cloud_storage,
        }
    }

    pub fn as_url(&self) -> String {
        let path = format!("{}.lock", self.metrics);
        format!("{}{}", self.cloud_storage.as_url(), path)
    }

    pub async fn exists(&self) -> Result<bool> {
        match self.cloud_storage {
            CloudStorage::Gcp(_, _) => gcp::is_lock_file_exists(&self).await,
        }
    }

    pub async fn create(&self) -> Result<()> {
        match self.cloud_storage {
            CloudStorage::Gcp(_, _) => gcp::create_lock_file(&self).await,
        }
    }

    pub async fn remove(&self) -> Result<()> {
        match self.cloud_storage {
            CloudStorage::Gcp(_, _) => gcp::remove_lock_file(&self).await,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tsdb::storage::block_list::*;
    use crate::tsdb::*;

    #[test]
    pub fn cloud_block_list_file_path() {
        let metrics = Metrics::new("some_metrics");

        let storage = CloudStorage::new_gcp("some_bucket", "some_dir");
        let file_path = CloudBlockListFilePath::new(&metrics, &storage);

        assert_eq!(
            "gs://some_bucket/some_dir/blocklist/some_metrics.list".to_string(),
            file_path.as_url()
        );
    }

    #[test]
    pub fn cloud_block_file_path() {
        let metrics = Metrics::new("some_metrics");

        let storage = CloudStorage::new_gcp("some_bucket", "some_dir");

        let ts = BlockTimestamp::new(TimestampSec::new(1629745452), TimestampSec::new(1629745453));

        let file_path = CloudBlockFilePath::new(&metrics, &ts, &storage);

        assert_eq!(
            "gs://some_bucket/some_dir/block/some_metrics/16297/1629745452_1629745453/block"
                .to_string(),
            file_path.as_url()
        );
    }

    #[test]
    pub fn cloud_lock_file_path() {
        let metrics = Metrics::new("some_metrics");

        let storage = CloudStorage::new_gcp("some_bucket", "some_dir");
        let file_path = CloudLockfilePath::new(&metrics, &storage);

        assert_eq!(
            "gs://some_bucket/some_dir/some_metrics.lock".to_string(),
            file_path.as_url()
        );
    }
}
