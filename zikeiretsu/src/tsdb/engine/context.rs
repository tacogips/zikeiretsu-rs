use crate::tsdb::{Bucket, CloudStorage, SubDir};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBContextError {
    #[error("database not found: {0}")]
    DatabaseNotFount(String),
}

pub type Result<T> = std::result::Result<T, DBContextError>;

#[derive(Clone)]
pub struct Database {
    pub database_name: String,
    pub cloud_storage: Option<CloudStorage>,
}

impl Database {
    pub fn new(db_name: String, cloud_storage: Option<CloudStorage>) -> Self {
        Self {
            database_name: db_name,
            cloud_storage,
        }
    }
    pub fn as_local_db_dir(&self, data_dir: &Path) -> PathBuf {
        let mut pb = PathBuf::new();
        let dir_str = match &self.cloud_storage {
            Some(cloud_storage) => match cloud_storage {
                CloudStorage::Gcp(Bucket(bucket), SubDir(subdir)) => {
                    format!(
                        "{data_dir}/{db_name}_{bucket}/{subdir}",
                        data_dir = data_dir.display(),
                        db_name = self.database_name
                    )
                }
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

pub struct DBContext {
    pub data_dir: PathBuf,
    databases: HashMap<String, Database>,
}

impl DBContext {
    pub fn new(data_dir: PathBuf, databases_vec: Vec<Database>) -> Self {
        let mut databases = HashMap::<String, Database>::new();
        for each_database in databases_vec.into_iter() {
            databases.insert(each_database.database_name.clone(), each_database);
        }

        Self {
            data_dir,
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
                    Err(DBContextError::DatabaseNotFount(
                        "no database name specified".to_string(),
                    ))
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
                SubDir("test_dir/aaa".to_string()),
            )),
        };

        assert_eq!(
            db.as_local_db_dir("/data_dir").display().to_string(),
            "/data_dir/test_db_test_bucket/test_dir/aaa".to_string()
        );
    }
}
