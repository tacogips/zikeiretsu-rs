mod config;

use ::zikeiretsu::{CloudStorage, CloudStorageError, DBContext, Database};

use clap::Parser;
use config::*;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use thiserror::Error;

#[derive(Parser, PartialEq, Debug)]
pub enum Mode {
    Adhoc,
    Server,
    Client,
}
impl FromStr for Mode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "adhoc" => Ok(Self::Adhoc),
            "server" => Ok(Self::Server),
            "client" => Ok(Self::Client),
            r => Err(format!("unknown mode {r}")),
        }
    }
}

#[derive(Parser, Debug, Default)]
#[clap(author, version, about)]
pub struct Args {
    #[clap(long = "data_dir", short = 'd', env = "ZDB_DIR")]
    data_dir: Option<PathBuf>,

    #[clap(long = "mode", short = 'm')]
    pub mode: Option<Mode>,

    #[clap(
        long = "databases",
        env = "ZDB_DATABASES",
        help = "config for server. pass pair of database name and the bucket name join by '=' or just database name.the value be separated by comma if pass multiple setting. e.g. databases=test_db_name=gs://test_bucket,test_db2,test_db3=gs://aaaa/bbb/cccc"
    )]
    databases: Option<String>,

    #[clap(
        long = "service_account",
        env = "ZDB_SERVICE_ACCOUNT",
        help = "config for server. path to google service account file"
    )]
    service_account_file_path: Option<PathBuf>,

    #[clap(
        long = "config",
        short,
        help = "config file path for server and client. Read ~/.config/zikeiretsu/config.toml by default if it exists."
    )]
    config: Option<PathBuf>,

    #[clap(long = "https", help = "config for server and client. ")]
    pub https: bool,

    #[clap(long = "host", help = "config for server and client. ")]
    pub host: Option<String>,

    #[clap(long = "port", help = "config for server and client. ")]
    pub port: Option<usize>,

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

        if let Some(https) = config.https {
            self.https = https;
        }

        if let Some(host) = config.host {
            self.host = Some(host);
        }

        if let Some(port) = config.port {
            self.port = Some(port);
        }

        Ok(())
    }

    pub fn init(&mut self, load_default_config: bool) -> Result<()> {
        self.parse_database_args()?;

        if let Some(config_path) = &self.config {
            let config = Config::read(config_path.as_path())?;
            self.merge_with_config(config)?;
        } else if load_default_config {
            if let Some(config) = Config::try_load_default() {
                log::info!("loading default config");
                self.merge_with_config(config)?;
            }
        }

        if let Some(service_account) = self.service_account_file_path.as_ref() {
            env::set_var("SERVICE_ACCOUNT", service_account);
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
        args.init(false).unwrap();

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
        args.init(false).unwrap();

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
