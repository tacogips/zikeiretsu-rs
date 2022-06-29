use crate::tsdb::{cloudstorage::CloudStorageError, Bucket, CloudStorage, SubDir};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBContextError {
    #[error("database not found: {0}")]
    DatabaseNotFount(String),

    #[error("invalid database definition.{0}")]
    InvalidDatabaseDefinition(String),

    #[error("cloud storage error.{0}")]
    CloudStorageError(#[from] CloudStorageError),
}

pub type Result<T> = std::result::Result<T, DBContextError>;

#[derive(Clone, Debug, PartialEq)]
pub struct Database {
    pub database_name: String,
    pub cloud_storage: Option<CloudStorage>,
}

impl Database {
    pub fn new(database_name: String, cloud_storage: Option<CloudStorage>) -> Self {
        Self {
            database_name,
            cloud_storage,
        }
    }
    pub fn name(&self) -> String {
        self.database_name.to_string()
    }

    pub fn parse(database: &str) -> Result<Vec<Self>> {
        let mut parsed_databases = Vec::<Self>::new();
        for each_database_config in database.split(',') {
            let database_name_and_cloud_storage =
                each_database_config.split('=').collect::<Vec<&str>>();
            match *database_name_and_cloud_storage.as_slice() {
                [database_name, storage_url, ..] => {
                    //let storage_url = database_name_and_cloud_storage.get(1).unwrap();
                    let cloud_storage = CloudStorage::from_url(storage_url.trim())?;
                    let db = Database::new(database_name.trim().to_string(), Some(cloud_storage));
                    parsed_databases.push(db);
                }

                [database_name] => {
                    let db = Database::new(database_name.trim().to_string(), None);

                    parsed_databases.push(db);
                }
                _ => {
                    return Err(DBContextError::InvalidDatabaseDefinition(
                        each_database_config.to_string(),
                    ))
                }
            }
        }

        Ok(parsed_databases)
    }

    pub fn as_local_db_dir(&self, data_dir: &Path) -> PathBuf {
        let mut pb = PathBuf::new();
        let dir_str = match &self.cloud_storage {
            Some(cloud_storage) => match cloud_storage {
                CloudStorage::Gcp(Bucket(bucket), subdir) => match subdir {
                    Some(SubDir(subdir)) => format!(
                        "{data_dir}/{db_name}_{bucket}/{subdir}",
                        data_dir = data_dir.display(),
                        db_name = self.database_name
                    ),
                    None => format!(
                        "{data_dir}/{db_name}_{bucket}",
                        data_dir = data_dir.display(),
                        db_name = self.database_name
                    ),
                },
            },
            None => {
                format!(
                    "{data_dir}/{db_name}",
                    data_dir = data_dir.display(),
                    db_name = self.database_name
                )
            }
        };
        pb.push(dir_str);
        pb
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DBContext {
    pub data_dir: PathBuf,
    pub default_database: Option<String>,
    databases: HashMap<String, Database>,
}

impl DBContext {
    pub fn new(
        data_dir: PathBuf,
        default_database: Option<String>,
        databases_vec: Vec<Database>,
    ) -> Self {
        let mut databases = HashMap::<String, Database>::new();
        for each_database in databases_vec.into_iter() {
            databases.insert(each_database.database_name.clone(), each_database);
        }

        Self {
            data_dir,
            default_database,
            databases,
        }
    }

    pub fn get_database(&self, db_name: Option<&str>) -> Result<Option<&Database>> {
        match db_name {
            Some(db_name) => Ok(self.databases.get(db_name)),
            None => {
                if self.databases.len() == 1 {
                    Ok(self.databases.values().next())
                } else {
                    match self.default_database.as_ref() {
                        Some(default_database) => Ok(self.databases.get(default_database)),
                        None => Err(DBContextError::DatabaseNotFount(
                            "no database name specified".to_string(),
                        )),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn database_path_1() {
        let db = Database {
            database_name: "test_db".to_string(),
            cloud_storage: Some(CloudStorage::Gcp(
                Bucket("test_bucket".to_string()),
                Some(SubDir("test_dir/aaa".to_string())),
            )),
        };

        assert_eq!(
            db.as_local_db_dir(Path::new("/data_dir"))
                .display()
                .to_string(),
            "/data_dir/test_db_test_bucket/test_dir/aaa".to_string()
        );
    }
}
