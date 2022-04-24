use super::{ArgsError, Result};
use ::zikeiretsu::{CloudStorage, Database};
use dirs::home_dir;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Deserialize, Debug, PartialEq)]
pub struct Config {
    pub data_dir: Option<PathBuf>,
    pub databases: Option<Vec<DatabaseConfig>>,
    pub service_account_file_path: Option<PathBuf>,
    pub dataframe_width: Option<u16>,
    pub dataframe_row_num: Option<usize>,
    pub dataframe_col_num: Option<usize>,
    pub host: Option<String>,
    pub port: Option<usize>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct DatabaseConfig {
    database_name: String,
    cloud_storage_url: Option<String>,
}

impl DatabaseConfig {
    pub fn into_database(self) -> Result<Database> {
        let cloud_storage = match self.cloud_storage_url {
            None => None,
            Some(cloud_storage_url) => Some(CloudStorage::from_url(cloud_storage_url.as_str())?),
        };

        Ok(Database {
            database_name: self.database_name,
            cloud_storage,
        })
    }
}

impl Config {
    pub fn try_load_default() -> Option<Self> {
        default_config_path()
            .as_ref()
            .and_then(|f| match Self::read(f) {
                Err(_) => None,
                Ok(c) => Some(c),
            })
    }

    pub fn read(config_path: &Path) -> Result<Self> {
        if config_path.exists() && config_path.is_file() {
            let config_file_contents = fs::read_to_string(config_path)?;
            Self::read_str(config_file_contents.as_ref())
        } else {
            Err(ArgsError::NoSuchConfigFile(
                config_path.display().to_string(),
            ))
        }
    }

    pub fn read_str(contents: &str) -> Result<Self> {
        let config: Config = toml::from_str(contents)?;
        Ok(config)
    }
}

fn default_config_path() -> Option<PathBuf> {
    let dir = home_dir();
    dir.map(|mut d| {
        d.push(".config/zikeiretsu/config.toml");
        d
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_config() {
        let test_contents = r#"

            data_dir = "/tmp/db_dir"
            service_account_file_path= "/path/to/service_account"
            dataframe_width = 120
            dataframe_row_num = 9
            dataframe_col_num = 11
            host = "localhost"
            port = 1234

            [[databases]]
            database_name="test_db"
            cloud_storage_url ="gs://some/where"

            "#;

        let config: Config = Config::read_str(test_contents).unwrap();
        assert_eq!(
            config,
            Config {
                data_dir: Some("/tmp/db_dir".into()),
                service_account_file_path: Some("/path/to/service_account".into()),
                dataframe_width: Some(120),
                dataframe_row_num: Some(9),
                dataframe_col_num: Some(11),

                host: Some("localhost".to_string()),
                port: Some(1234),

                databases: Some(vec![DatabaseConfig {
                    database_name: "test_db".to_string(),
                    cloud_storage_url: Some("gs://some/where".to_string()),
                }])
            }
        );
    }
}
