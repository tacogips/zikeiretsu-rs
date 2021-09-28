pub mod file_path;

pub mod gcp;

use file_dougu::gcs::FileUtilGcsError;
use file_dougu::FileUtilError;
pub use file_path::*;
use std::fmt::{Display, Formatter, Result as FormatterResult};
use std::io;
use thiserror::Error;

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
}

#[derive(Debug, Clone)]
pub struct Bucket(pub String);

impl Display for Bucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct SubDir(pub String);

impl Display for SubDir {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub enum CloudStorage {
    Gcp(Bucket, Option<SubDir>),
}

impl CloudStorage {
    pub fn new_gcp(bucket: &str, sub_dir: Option<&str>) -> Self {
        let sub_dir = sub_dir.map(|sub_dir| {
            let sub_dir: &str = if sub_dir.ends_with("/") {
                &sub_dir[..sub_dir.len() - 1]
            } else {
                sub_dir
            };

            let sub_dir: &str = if sub_dir.starts_with("/") {
                &sub_dir[1..]
            } else {
                sub_dir
            };
            SubDir(sub_dir.to_string())
        });

        Self::Gcp(Bucket(bucket.to_string()), sub_dir)
    }

    pub fn as_url(&self) -> String {
        match self {
            Self::Gcp(Bucket(bucket_str), sub_dir) => {
                let gcs_url = format!("gs://{}", bucket_str);

                let gcs_url = match sub_dir {
                    Some(sub_dir) => {
                        let sub_dir_str = &sub_dir.0;

                        format!("{}/{}", gcs_url, sub_dir_str)
                    }
                    None => gcs_url,
                };

                gcs_url
            }
        }
    }
}
