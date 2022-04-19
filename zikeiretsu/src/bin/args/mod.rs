use ::zikeiretsu::{Bucket, CloudStorage, DBContext, SubDir};
use clap::Parser;
use std::env;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ArgsError {
    #[error("invalid cloud type {0}")]
    InvalidCloudType(String),

    #[error("cloud type required")]
    NoCloudType,

    #[error("bucket required")]
    NoBucket,

    #[error("subpath required")]
    NoSubPath,
}

type Result<T> = std::result::Result<T, ArgsError>;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
pub struct Args {
    #[clap(long = "db_dir", short = 'd', env = "ZDB_DIR")]
    db_dir: Option<String>,

    #[clap(long = "cloud_type", short = 't', env = "ZDB_CLOUD_TYPE")]
    cloud_type: Option<String>,

    #[clap(long = "bucket", short = 'b', env = "ZDB_BUCKET")]
    bucket: Option<String>,

    #[clap(long = "bucket_sub_path", short = 'p', env = "ZDB_BUCKET_SUBPATH")]
    bucket_sub_path: Option<String>,

    #[clap(long = "service_account", env = "ZDB_SERVICE_ACCOUNT")]
    sevice_account_file_path: Option<PathBuf>,

    #[clap(long = "table_width", env = "ZDB_TABLE_WIDTH")]
    table_width: Option<u16>,

    #[clap(long = "table_row", env = "ZDB_TABLE_ROW")]
    table_row: Option<usize>,

    #[clap(long = "table_col", env = "ZDB_TABLE_COL")]
    table_col: Option<usize>,

    pub query: Option<String>,
}

impl Args {
    pub fn setup(&self) -> Result<()> {
        if let Some(service_account) = self.sevice_account_file_path.as_ref() {
            env::set_var("SERVICE_ACCOUNT", service_account);
        }
        if let Some(table_width) = self.table_width {
            // default 100
            env::set_var("POLARS_TABLE_WIDTH", table_width.to_string());
        }

        if let Some(table_row) = self.table_row {
            //default 25
            env::set_var("POLARS_FMT_MAX_ROWS", table_row.to_string());
        }

        if let Some(table_col) = self.table_col {
            //default 75
            env::set_var("POLARS_FMT_MAX_COLS", table_col.to_string());
        }

        Ok(())
    }

    fn parse_cloud_storage(&self) -> Result<Option<CloudStorage>> {
        match &self.cloud_type {
            Some(cloud_type) => {
                let bucket = if let Some(bucket) = self.bucket.as_ref() {
                    bucket
                } else {
                    return Err(ArgsError::NoBucket);
                };

                let subpath = if let Some(subpath) = self.bucket_sub_path.as_ref() {
                    subpath
                } else {
                    return Err(ArgsError::NoSubPath);
                };
                match cloud_type.as_str() {
                    "gcp" => Ok(Some(CloudStorage::Gcp(
                        Bucket(bucket.to_string()),
                        SubDir(subpath.to_string()),
                    ))),
                    invalid_cloud_type @ _ => {
                        Err(ArgsError::InvalidCloudType(invalid_cloud_type.to_string()))
                    }
                }
            }
            _ => Ok(None),
        }
    }

    pub fn as_db_context(&self) -> Result<DBContext> {
        let cloud_stroage = self.parse_cloud_storage()?;
        let ctx = DBContext::new(self.db_dir.clone(), cloud_stroage);
        Ok(ctx)
    }
}
