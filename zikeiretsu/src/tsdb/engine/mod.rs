pub mod context;
use crate::tsdb::cloudstorage::CloudStorage;
use crate::tsdb::data_types::TimeSeriesDataFrame;
use crate::tsdb::field::FieldType;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::store::writable_store::DatapointDefaultSorter;
use crate::tsdb::{datapoint::DatapointsRange, storage::*, store::*};
use crate::tsdb::{storage::api as storage_api, store};
pub use context::*;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("failed to create lock file {0}")]
    FailedToGetLockfile(#[from] std::io::Error),

    #[error("storage api error {0}")]
    StorageApiError(#[from] storage_api::StorageApiError),

    #[error("store error {0}")]
    StoreError(#[from] store::StoreError),
}

pub type Result<T> = std::result::Result<T, EngineError>;

pub struct DBConfig {
    pub cache_setting: api::CacheSetting,
    pub cloud_storage: Option<CloudStorage>,
    pub cloud_setting: api::CloudStorageSetting,
}

impl DBConfig {
    pub fn builder_with_cache() -> SearchSettingsBuilder {
        Self::builder_with_cache_setting(true, true)
    }

    pub fn builder_with_no_cache() -> SearchSettingsBuilder {
        Self::builder_with_cache_setting(false, false)
    }

    pub fn builder_with_cache_setting(
        read_cache: bool,
        write_cache: bool,
    ) -> SearchSettingsBuilder {
        let cache_setting = api::CacheSetting {
            read_cache,
            write_cache,
        };
        SearchSettingsBuilder {
            cache_setting,
            cloud_storage: None,
            cloud_setting: CloudStorageSetting::default(),
        }
    }

    pub fn cloud_storage_and_setting(&self) -> Option<(&CloudStorage, &CloudStorageSetting)> {
        self.cloud_storage
            .as_ref()
            .map(|cloud_storage| (cloud_storage, &self.cloud_setting))
    }
}

impl Default for DBConfig {
    fn default() -> Self {
        Self {
            cache_setting: api::CacheSetting {
                read_cache: true,
                write_cache: true,
            },

            cloud_storage: None,
            cloud_setting: api::CloudStorageSetting::default(),
        }
    }
}

pub struct SearchSettingsBuilder {
    cache_setting: api::CacheSetting,
    cloud_storage: Option<CloudStorage>,
    cloud_setting: api::CloudStorageSetting,
}

impl SearchSettingsBuilder {
    pub fn cache_setting(mut self, cache_setting: api::CacheSetting) -> SearchSettingsBuilder {
        self.cache_setting = cache_setting;
        self
    }

    pub fn cloud_storage_setting(
        mut self,
        cloud_setting: api::CloudStorageSetting,
    ) -> SearchSettingsBuilder {
        self.cloud_setting = cloud_setting;
        self
    }

    pub fn build(self) -> DBConfig {
        DBConfig {
            cache_setting: self.cache_setting,
            cloud_storage: self.cloud_storage,
            cloud_setting: self.cloud_setting,
        }
    }
}

pub struct Engine;
impl Engine {
    pub async fn list_metrics<P: AsRef<Path>>(
        db_dir: Option<P>,
        config: &DBConfig,
    ) -> Result<Vec<Metrics>> {
        let metrics =
            api::read::fetch_all_metrics(db_dir, config.cloud_storage_and_setting()).await?;

        Ok(metrics)
    }

    pub async fn block_list_data<P: AsRef<Path>>(
        database_name: &str,
        db_dir: P,
        metrics: &Metrics,
        config: &DBConfig,
    ) -> Result<block_list::BlockList> {
        let block_list = api::read::read_block_list(
            database_name,
            db_dir.as_ref(),
            metrics,
            &config.cache_setting,
            config.cloud_storage_and_setting(),
        )
        .await?;

        Ok(block_list)
    }

    pub fn writable_store_builder(
        metics: Metrics,
        field_types: Vec<FieldType>,
    ) -> WritableStoreBuilder<DatapointDefaultSorter> {
        WritableStore::builder(metics, field_types)
    }

    pub async fn search<P: AsRef<Path>>(
        database_name: &str,
        db_dir: P,
        metrics: &Metrics,
        field_selectors: Option<&[usize]>,
        condition: &DatapointsRange,
        db_config: &DBConfig,
    ) -> Result<Option<TimeSeriesDataFrame>> {
        let dataframe = api::read::search_dataframe(
            database_name,
            db_dir,
            metrics,
            field_selectors,
            condition,
            &db_config.cache_setting,
            db_config.cloud_storage_and_setting(),
        )
        .await?;
        Ok(dataframe)
    }
}
