pub mod file_path;

pub mod gcp;

use file_dougu::gcs::{FileUtilGcsError, GcsBucket, GcsFile};
use file_dougu::FileUtilError;
pub use file_path::*;
use std::fmt::{Display, Formatter, Result as FormatterResult};
use std::io;
use thiserror::Error;
use url::Url;
use uuid::Error as UuidError;

type Result<T> = std::result::Result<T, CloudStorageError>;

#[derive(Error, Debug)]
pub enum CloudStorageError {
    #[error("invalid url. {0} {1}")]
    InvalidUrlError(String, String),

    #[error("invalid path. {0} ")]
    InvalidPathError(String),

    #[error("cloud storage url error. {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("file util error. {0}")]
    FileUtilError(#[from] FileUtilError),

    #[error("file util gcs error. {0}")]
    FileUtilGcsError(#[from] FileUtilGcsError),

    #[error("cloud open file error. {0}")]
    IoError(#[from] io::Error),

    #[error("invalid block list file url. {0}")]
    InvalidBlockListFileUrl(String),

    #[error("invalid metrics name. {0}")]
    InvalidMetricsName(String),

    #[error("invalid url. {0}")]
    InvalidUrl(String),

    #[error("unsupported cloud storage url. {0}")]
    UnsupportedCloudStorageUrl(String),

    #[error("uuid error. {0}")]
    UuidError(#[from] UuidError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bucket(pub String);

impl Display for Bucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "{bucket}", bucket = self.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubDir(pub String);
impl Display for SubDir {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "{subdir}", subdir = self.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CloudStorage {
    Gcp(Bucket, Option<SubDir>),
}

impl CloudStorage {
    pub fn new_gcp(bucket: &str, sub_dir: Option<&str>) -> Self {
        let sub_dir = match sub_dir {
            Some(sub_dir) => {
                let sub_dir: &str = if let Some(stripped) = sub_dir.strip_suffix('/') {
                    stripped
                } else {
                    sub_dir
                };

                let sub_dir: &str = if let Some(stripped) = sub_dir.strip_prefix('/') {
                    stripped
                } else {
                    sub_dir
                };
                Some(SubDir(sub_dir.to_string()))
            }
            None => None,
        };

        Self::Gcp(Bucket(bucket.to_string()), sub_dir)
    }

    pub fn as_url(&self) -> String {
        match self {
            Self::Gcp(Bucket(bucket_str), sub_dir) => match sub_dir {
                Some(sub_dir) => {
                    format!("gs://{bucket_str}/{sub_dir}/")
                }
                None => {
                    format!("gs://{bucket_str}/")
                }
            },
        }
    }

    pub fn from_url(url: &str) -> Result<Self> {
        let url = Url::parse(url)
            .map_err(|e| CloudStorageError::InvalidUrl(format!("{} ({:?})", url, e)))?;

        match GcsFile::new_with_url(&url) {
            Ok(GcsFile { bucket, name, .. }) => Ok(CloudStorage::new_gcp(&bucket, Some(&name))),
            Err(_e) => match GcsBucket::new_with_url(&url) {
                Ok(GcsBucket { bucket }) => Ok(CloudStorage::new_gcp(&bucket, None)),
                Err(_e) => Err(CloudStorageError::UnsupportedCloudStorageUrl(format!(
                    "{}",
                    url
                ))),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn cloud_block_list_file_path_1() {
        let storage = CloudStorage::new_gcp("some_bucket", Some("some_dir"));

        assert_eq!("gs://some_bucket/some_dir/".to_string(), storage.as_url());
    }

    #[test]
    pub fn cloud_block_list_file_path_2() {
        let storage = CloudStorage::new_gcp("some_bucket", Some("some_dir"));

        assert_eq!(
            CloudStorage::from_url("gs://some_bucket/some_dir/").unwrap(),
            storage
        );
    }

    #[test]
    pub fn cloud_block_list_file_path_3() {
        let storage = CloudStorage::new_gcp("some_bucket", Some("some_dir/aaa"));

        assert_eq!(
            CloudStorage::from_url("gs://some_bucket/some_dir/aaa").unwrap(),
            storage
        );
    }

    #[test]
    pub fn cloud_block_list_file_path_5() {
        let storage = CloudStorage::new_gcp("some_bucket", Some("some_dir/aaa"));

        assert_eq!(
            CloudStorage::from_url("gs://some_bucket/some_dir/aaa/").unwrap(),
            storage
        );
    }

    #[test]
    pub fn cloud_block_list_file_path_6() {
        let storage = CloudStorage::new_gcp("some_bucket", None);

        assert_eq!(CloudStorage::from_url("gs://some_bucket").unwrap(), storage);
    }
}
