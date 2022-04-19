use super::Result;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Deserialize, Debug, PartialEq)]
pub struct Config {
    pub db_dir: Option<String>,
    pub cloud_type: Option<String>,
    pub bucket: Option<String>,
    pub bucket_sub_path: Option<String>,
    pub service_account_file_path: Option<PathBuf>,
    pub dataframe_width: Option<u16>,
    pub dataframe_row_num: Option<usize>,
    pub dataframe_col_num: Option<usize>,
}

impl Config {
    pub fn read(config_path: &Path) -> Result<Self> {
        let config_file_contents = fs::read_to_string(config_path)?;
        Self::read_str(config_file_contents.as_ref())
    }

    pub fn read_str(contents: &str) -> Result<Self> {
        let config: Config = toml::from_str(contents.as_ref())?;
        Ok(config)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn parse_config() {
        let test_contents = "

            db_dir = /tmp/db_dir
            cloud_type = gcp
            bucket = test_bucket
            bucket_sub_path = some_path
            service_account_file_path= /path/to/service_account
            dataframe_width = 120
            dataframe_row_num = 9
            dataframe_col_num = 11

            ";

        let mut pb = PathBuf::new();
        pb.push("/path/to/service_account");

        let config: Config = Config::read_str(test_contents).unwrap();
        assert_eq!(
            config,
            Config {
                db_dir: Some("/tmp/db_dir".to_string()),
                cloud_type: Some("gcp".to_string()),
                bucket: Some("test_bucket".to_string()),
                bucket_sub_path: Some("test_bucket".to_string()),
                service_account_file_path: Some(pb),
                dataframe_width: Some(120),
                dataframe_row_num: Some(9),
                dataframe_col_num: Some(11),
            }
        );
    }
}
