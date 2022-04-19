mod config;
use ::zikeiretsu::{Bucket, CloudStorage, DBContext, SubDir};

use clap::Parser;
use config::*;
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

    #[error("{0}")]
    TomlError(#[from] toml::de::Error),

    #[error("{0}")]
    IoError(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, ArgsError>;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
pub struct Args {
    #[clap(long = "db_dir", short = 'd', env = "ZDB_DIR")]
    db_dir: Option<PathBuf>,

    #[clap(long = "cloud_type", short = 't', env = "ZDB_CLOUD_TYPE")]
    cloud_type: Option<String>,

    #[clap(long = "bucket", short = 'b', env = "ZDB_BUCKET")]
    bucket: Option<String>,

    #[clap(long = "bucket_sub_path", short = 'p', env = "ZDB_BUCKET_SUBPATH")]
    bucket_sub_path: Option<String>,

    #[clap(long = "service_account", env = "ZDB_SERVICE_ACCOUNT")]
    service_account_file_path: Option<PathBuf>,

    #[clap(long = "df_width", env = "ZDB_DATAFRAME_WIDTH")]
    df_width: Option<u16>,

    #[clap(long = "df_row", env = "ZDB_DATAFRAME_ROW")]
    df_row_num: Option<usize>,

    #[clap(long = "df_col", env = "ZDB_DATAFRAME_COL")]
    df_col_num: Option<usize>,

    #[clap(long = "config", short)]
    config: Option<PathBuf>,

    pub query: Option<String>,
}

impl Args {
    fn merge_with_config(&mut self, config: Config) {
        if let Some(db_dir) = config.db_dir {
            self.db_dir = Some(db_dir);
        }

        if let Some(cloud_type) = config.cloud_type {
            self.cloud_type = Some(cloud_type);
        }

        if let Some(bucket) = config.bucket {
            self.bucket = Some(bucket);
        }

        if let Some(bucket_sub_path) = config.bucket_sub_path {
            self.bucket_sub_path = Some(bucket_sub_path);
        }

        if let Some(service_account_file_path) = config.service_account_file_path {
            self.service_account_file_path = Some(service_account_file_path);
        }

        if let Some(df_width) = config.dataframe_width {
            self.df_width = Some(df_width);
        }

        if let Some(df_row_num) = config.dataframe_row_num {
            self.df_row_num = Some(df_row_num);
        }

        if let Some(df_col_num) = config.dataframe_col_num {
            self.df_col_num = Some(df_col_num);
        }
    }

    pub fn init(&mut self) -> Result<()> {
        if let Some(config_path) = &self.config {
            let config = Config::read(config_path.as_path())?;
            self.merge_with_config(config);
        }

        if let Some(service_account) = self.service_account_file_path.as_ref() {
            env::set_var("SERVICE_ACCOUNT", service_account);
        }
        if let Some(table_width) = self.df_width {
            // default 100
            env::set_var("POLARS_TABLE_WIDTH", table_width.to_string());
        }

        if let Some(table_row) = self.df_row_num {
            //default 25
            env::set_var("POLARS_FMT_MAX_ROWS", table_row.to_string());
        }

        if let Some(table_col) = self.df_col_num {
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
