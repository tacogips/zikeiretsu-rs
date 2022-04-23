mod config;
use ::zikeiretsu::{CloudStorage, CloudStorageError, DBContext, Database};

use clap::Parser;
use config::*;
use std::env;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Parser, Debug, Default)]
#[clap(author, version, about)]
pub struct Args {
    #[clap(long = "data_dir", short = 'd', env = "ZDB_DIR")]
    data_dir: Option<PathBuf>,

    #[clap(
        long = "databases",
        env = "ZDB_DATABASES",
        help = "config for server. pass pair of database name and the bucket name join by '=' or just database name.
        the value be separated by comma if pass multiple setting. e.g. databases=test_db_name=gs://test_bucket,test_db2,test_db3=gs://aaaa/bbb/cccc"
    )]
    databases: Option<String>,

    #[clap(
        long = "service_account",
        env = "ZDB_SERVICE_ACCOUNT",
        help = "config for server. path to google service account file"
    )]
    service_account_file_path: Option<PathBuf>,

    #[clap(
        long = "df_width",
        env = "ZDB_DATAFRAME_WIDTH",
        help = "config for server. "
    )]
    df_width: Option<u16>,

    #[clap(
        long = "df_row",
        env = "ZDB_DATAFRAME_ROW",
        help = "config for server. "
    )]
    df_row_num: Option<usize>,

    #[clap(
        long = "df_col",
        env = "ZDB_DATAFRAME_COL",
        help = "config for server. "
    )]
    df_col_num: Option<usize>,

    #[clap(long = "config", short, help = "config for server and client. ")]
    config: Option<PathBuf>,

    #[clap(skip)]
    parsed_databases: Option<Vec<Database>>,

    pub query: Option<String>,
}

impl Args {
    fn merge_with_config(&mut self, config: Config) -> Result<()> {
        if let Some(data_dir) = config.data_dir {
            self.data_dir = Some(data_dir);
        }

        if let Some(database_configs) = config.databases {
            let mut databases = Vec::new();
            for each_config in database_configs.into_iter() {
                databases.push(each_config.into_database()?);
            }

            self.parsed_databases = Some(databases);
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
        Ok(())
    }

    pub fn init(&mut self) -> Result<()> {
        self.parse_database_args()?;

        if let Some(config_path) = &self.config {
            let config = Config::read(config_path.as_path())?;
            self.merge_with_config(config)?;
        } else {
            if let Some(config) = Config::try_load_default() {
                log::info!("loading default config");
                self.merge_with_config(config)?;
            }
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

    fn parse_database_args(&mut self) -> Result<()> {
        if let Some(database) = &self.databases {
            let mut parsed_databases = Vec::<Database>::new();
            for each_database_config in database.split(',') {
                let database_name_and_cloud_storage =
                    each_database_config.split('=').collect::<Vec<&str>>();
                match database_name_and_cloud_storage.len() {
                    1 => {
                        let db = Database::new(
                            database_name_and_cloud_storage
                                .get(0)
                                .unwrap()
                                .trim()
                                .to_string(),
                            None,
                        );

                        parsed_databases.push(db);
                    }
                    2 => {
                        let storage_url = database_name_and_cloud_storage.get(1).unwrap();
                        let cloud_storage = CloudStorage::from_url(storage_url.trim())?;
                        let db = Database::new(
                            database_name_and_cloud_storage
                                .get(0)
                                .unwrap()
                                .trim()
                                .to_string(),
                            Some(cloud_storage),
                        );
                        parsed_databases.push(db);
                    }
                    _ => {
                        return Err(ArgsError::InvalidDatabaseDefinition(
                            each_database_config.to_string(),
                        ))
                    }
                }
            }

            self.parsed_databases = Some(parsed_databases);
        }
        Ok(())
    }

    pub fn as_db_context(&self) -> Result<DBContext> {
        let parsed_databases = match &self.parsed_databases {
            Some(parsed_databases) => parsed_databases.clone(),
            None => return Err(ArgsError::NoDatabaseDefinition),
        };

        let data_dir = match &self.data_dir {
            Some(data_dir) => data_dir.clone(),
            None => return Err(ArgsError::NoDataDir),
        };

        let ctx = DBContext::new(data_dir, parsed_databases);
        Ok(ctx)
    }
}

#[derive(Error, Debug)]
pub enum ArgsError {
    #[error("not data dir path ")]
    NoDataDir,

    #[error("{0}")]
    TomlError(#[from] toml::de::Error),

    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("{0}")]
    CloudStorageError(#[from] CloudStorageError),

    #[error("invalid database definition.{0}")]
    InvalidDatabaseDefinition(String),

    #[error("not database definition.")]
    NoDatabaseDefinition,

    #[error("no such config file.")]
    NoSuchConfigFile(String),
}

type Result<T> = std::result::Result<T, ArgsError>;

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_parse_databases_1() {
        let mut args = Args::default();
        let mut data_dir = PathBuf::new();
        data_dir.push("/tmp/test_dir/");
        args.data_dir = Some(data_dir.clone());
        args.databases = Some("t_db=gs://some/thing".to_string());
        args.init().unwrap();

        let db_context = args.as_db_context().unwrap();
        assert_eq!(
            db_context,
            DBContext::new(
                data_dir,
                vec![Database {
                    database_name: "t_db".to_string(),
                    cloud_storage: Some(CloudStorage::new_gcp("some", "thing")),
                }]
            )
        )
    }

    #[test]
    fn test_parse_databases_2() {
        let mut args = Args::default();
        let mut data_dir = PathBuf::new();
        data_dir.push("/tmp/test_dir/");
        args.data_dir = Some(data_dir.clone());
        args.databases =
            Some("t_db=gs://some/thing,t_db2, t_db_3 = gs://some/thing/else".to_string());
        args.init().unwrap();

        let db_context = args.as_db_context().unwrap();
        assert_eq!(
            db_context,
            DBContext::new(
                data_dir,
                vec![
                    Database {
                        database_name: "t_db".to_string(),
                        cloud_storage: Some(CloudStorage::new_gcp("some", "thing")),
                    },
                    Database {
                        database_name: "t_db2".to_string(),
                        cloud_storage: None
                    },
                    Database {
                        database_name: "t_db_3".to_string(),
                        cloud_storage: Some(CloudStorage::new_gcp("some", "thing/else")),
                    },
                ]
            )
        )
    }
}
