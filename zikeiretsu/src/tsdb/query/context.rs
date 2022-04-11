use crate::tsdb::CloudStorage;
pub struct DBContext {
    pub db_dir: Option<String>,
    pub cloud_storage: Option<CloudStorage>,
}

impl DBContext {
    pub fn new(db_dir: Option<String>, cloud_storage: Option<CloudStorage>) -> Self {
        Self {
            db_dir,
            cloud_storage,
        }
    }
}
