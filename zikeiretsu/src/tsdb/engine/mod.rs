use crate::tsdb::field::FieldType;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::store::writable_store::{DatapointDefaultSorter, DatapointSorter};
use crate::tsdb::{datapoint::DatapointSearchCondition, metrics::*, storage::*, store::*};
use crate::tsdb::{storage::api as storage_api, store};
use std::path::PathBuf;
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

pub struct Builder {
    db_dir: PathBuf,
}

impl Builder {
    pub fn defualt<P: Into<PathBuf>>(db_dir: P) -> Self {
        Self {
            db_dir: db_dir.into(),
        }
    }

    pub fn build(self) -> Zikeiretsu {
        Zikeiretsu {
            db_dir: self.db_dir,
        }
    }
}

pub struct SearchSettings {
    cache_setting: api::CacheSetting,
    cloud_setting: Option<api::CloudSetting>,
}

pub struct Zikeiretsu {
    db_dir: PathBuf,
}

impl Zikeiretsu {
    pub fn new_writable_store_builder<M: Into<Metrics>>(
        metics: M,
        field_types: Vec<FieldType>,
    ) -> WritableStoreBuilder<DatapointDefaultSorter> {
        WritableStore::builder(metics, field_types)
    }

    pub async fn search(
        self,
        metrics: &Metrics,
        condition: &DatapointSearchCondition,
        setting: &SearchSettings,
    ) -> Result<ReadonlyStore> {
        let datapoints = api::read::search_datas(
            self.db_dir,
            metrics,
            condition,
            &setting.cache_setting,
            setting.cloud_setting.as_ref(),
        )
        .await?;

        let store = ReadonlyStore::new(datapoints, false)?;
        Ok(store)
    }
}
