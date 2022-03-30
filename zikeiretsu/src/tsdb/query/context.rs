use super::output;

use crate::*;

pub struct DBContext {
    pub db_dir: String,
    pub db_config: DBConfig,
}

impl DBContext {
    pub fn new(db_dir: String, db_config: DBConfig) -> Self {
        Self { db_dir, db_config }
    }
}
