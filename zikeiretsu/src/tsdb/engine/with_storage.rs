use super::Result;
use crate::tsdb::{datapoint::DatapointSearchCondition, metrics::*, storage::*, store::*};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Builder {
    lock: Arc<Mutex<()>>,
    db_dir: PathBuf,
}

impl Builder {
    pub fn defualt<P: Into<PathBuf>>(db_dir: P) -> Self {
        Self {
            lock: Arc::new(Mutex::new(())),
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
