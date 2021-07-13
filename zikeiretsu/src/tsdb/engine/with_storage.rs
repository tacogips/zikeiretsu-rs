use super::{EngineError, Result};
use crate::tsdb::datapoint::DatapointSearchCondition;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::{Semaphore, TryAcquireError};

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
            lock: self.lock,
            db_dir: self.db_dir,
        }
    }
}

struct SearchOpt {
    not_keep_in_cache: bool,
}

pub struct Zikeiretsu {
    lock: Arc<Mutex<()>>,
    db_dir: PathBuf,
}

impl Zikeiretsu {
    async fn search<P: AsRef<Path>>(
        db_dir: P,
        condition: DatapointSearchCondition,
        search_opt: Option<SearchOpt>,
    ) -> Result<()> {
        unimplemented!()
    }
}
