use crate::tsdb::CloudStorage;
use std::path::PathBuf;
pub struct DBContext {
    pub db_dir: Option<PathBuf>,
    pub cloud_storage: Option<CloudStorage>,
}

impl DBContext {
    pub fn new(db_dir: Option<PathBuf>, cloud_storage: Option<CloudStorage>) -> Self {
        Self {
            db_dir,
            cloud_storage,
        }
    }
}
