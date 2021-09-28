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

pub struct SearchSettings {
    cache_setting: api::CacheSetting,
    cloud_setting: Option<api::CloudStorageSetting>,
}

impl SearchSettings {
    pub fn builder_with_cache() -> SearchSettingsBuilder {
        Self::builder_with_cache_setting(true, true)
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

    pub fn builder_with_no_cache() -> SearchSettingsBuilder {
        Self::builder_with_cache_setting(false, false)
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

    pub fn build(self) -> SearchSettings {
        SearchSettings {
            cache_setting: self.cache_setting,
            cloud_setting: self.cloud_setting,
        }
    }
}

pub struct Zikeiretsu;

impl Zikeiretsu {
    pub fn writable_store_builder<M: Into<Metrics>>(
        metics: M,
        field_types: Vec<FieldType>,
    ) -> WritableStoreBuilder<DatapointDefaultSorter> {
        WritableStore::builder(metics, field_types)
    }

    pub async fn readonly_store<P: AsRef<Path>, M: Into<Metrics>>(
        db_dir: P,
        metrics: M,
        condition: &DatapointSearchCondition,
        setting: &SearchSettings,
    ) -> Result<ReadonlyStore> {
        let datapoints = api::read::search_datas(
            db_dir,
            &metrics.into(),
            condition,
            &setting.cache_setting,
            setting.cloud_setting.as_ref(),
        )
        .await?;

        let store = ReadonlyStore::new(datapoints, false)?;
        Ok(store)
    }
}
