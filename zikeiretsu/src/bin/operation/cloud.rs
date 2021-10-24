use std::env;
use std::time::Duration;
use thiserror::Error;

pub struct ZikeiretsuReadConfig {
    pub db_dir: String,
    pub gcp_bucket: Option<String>,
    pub bucket_sub_dir: Option<String>,
}

pub struct ZikeiretsuReadConfigBuilder {
    pub db_dir: String,
    pub gcp_bucket: Option<String>,
    pub bucket_sub_dir: Option<String>,
}

impl ZikeiretsuReadConfigBuilder {
    pub fn builder(db_dir: String) -> Self {
        Self {
            db_dir,
            gcp_bucket: None,
            bucket_sub_dir: None,
        }
    }
    pub fn build(self) -> ZikeiretsuReadConfig {
        let ZikeiretsuReadConfigBuilder {
            db_dir,
            gcp_bucket,
            bucket_sub_dir,
        } = self;

        ZikeiretsuReadConfig {
            db_dir,
            gcp_bucket,
            bucket_sub_dir,
        }
    }
}
