use crate::tsdb::field::FieldType;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::store::writable_store::DatapointDefaultSorter;
use crate::tsdb::{datapoint::DatapointSearchCondition, storage::*, store::*};
use crate::tsdb::{storage::api as storage_api, store};
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
    cache_setting: api::CacheSetting,
    cloud_setting: Option<api::CloudStorageSetting>,
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
            cloud_setting: None,
        }
    }
}

pub struct SearchSettingsBuilder {
    cache_setting: api::CacheSetting,
    cloud_setting: Option<api::CloudStorageSetting>,
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
        self.cloud_setting = Some(cloud_setting);
        self
    }

    pub fn build(self) -> DBConfig {
        DBConfig {
            cache_setting: self.cache_setting,
            cloud_setting: self.cloud_setting,
        }
    }
}

pub struct Zikeiretsu;

impl Zikeiretsu {
    pub async fn list_metrics<P: AsRef<Path>>(
        db_dir: Option<P>,
        setting: &DBConfig,
    ) -> Result<Vec<Metrics>> {
        let metrics = api::read::fetch_all_metrics(db_dir, setting.cloud_setting.as_ref()).await?;

        Ok(metrics)
    }

    pub async fn block_list_data<P: AsRef<Path>>(
        db_dir: P,
        metrics: &Metrics,
        setting: &DBConfig,
    ) -> Result<block_list::BlockList> {
        let block_list = api::read::read_block_list(
            db_dir.as_ref(),
            &metrics,
            &setting.cache_setting,
            setting.cloud_setting.as_ref(),
        )
        .await?;

        Ok(block_list)
    }

    pub fn writable_store_builder<M: Into<Metrics>>(
        metics: M,
        field_types: Vec<FieldType>,
    ) -> WritableStoreBuilder<DatapointDefaultSorter> {
        WritableStore::builder(metics, field_types)
    }

    pub async fn readonly_store<P: AsRef<Path>, M>(
        db_dir: P,
        metrics: M,
        field_selectors: Option<&[usize]>,
        condition: &DatapointSearchCondition,
        setting: &DBConfig,
    ) -> Result<Option<ReadonlyStore>>
    where
        M: TryInto<Metrics, Error = String>,
    {
        let dataframe = api::read::search_dataframe(
            db_dir,
            &metrics
                .try_into()
                .map_err(|e| StorageApiError::InvalidMetricsName(e))?,
            field_selectors,
            condition,
            &setting.cache_setting,
            setting.cloud_setting.as_ref(),
        )
        .await?;
        match dataframe {
            None => Ok(None),
            Some(dataframe) => {
                let store = ReadonlyStore::new(dataframe, false)?;
                Ok(Some(store))
            }
        }
    }
}
