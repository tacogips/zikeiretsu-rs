use super::output;
use crate::*;

pub struct QueryContext {
    pub db_dir: String,
    pub search_setting: SearchSettings,
    //pub output_setting: output::OutputSetting,
}

impl QueryContext {
    pub fn new(db_dir: String, search_setting: SearchSettings) -> Self {
        Self {
            db_dir,
            search_setting,
        }
    }
}
