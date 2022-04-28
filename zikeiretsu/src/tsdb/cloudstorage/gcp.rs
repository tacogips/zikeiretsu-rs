use super::{file_path::*, CloudStorageError, Result};
use crate::tsdb::storage::block_list;
use crate::Metrics;
use file_dougu;
use memmap2::MmapOptions;
use once_cell::sync::Lazy;
use regex::Regex;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::Write;
use std::path::Path;

static BLOCK_LIST_FILE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        format!(
            r"gs://.*/{file_name_pattern}",
            file_name_pattern = block_list::BLOCK_LIST_FILE_NAME_PATTERN
        )
        .as_str(),
    )
    .unwrap()
});

fn create_parent_dir_if_not_exists(dest: &Path) -> Result<()> {
    let parent_dir = dest
        .parent()
        .ok_or_else(|| CloudStorageError::InvalidPathError(dest.display().to_string()))?;

    create_dir_all(parent_dir)?;
    Ok(())
}

pub async fn download_block_file<'a>(
    src: &CloudBlockFilePath<'a>,
    dest: &Path,
) -> Result<Option<()>> {
    let src_url = src.as_url();

    let contents = file_dougu::get_file_contents(&src_url, None, None).await?;
    match contents {
        Some(contents_data) => {
            let mut block_file = if dest.exists() {
                OpenOptions::new().write(true).truncate(true).open(dest)?
            } else {
                create_parent_dir_if_not_exists(dest)?;
                OpenOptions::new().create(true).write(true).open(dest)?
            };

            block_file.write_all(&contents_data)?;
            Ok(Some(()))
        }
        None => Ok(None),
    }
}

pub async fn upload_block_file<'a>(src: &Path, dest: &CloudBlockFilePath<'a>) -> Result<()> {
    let block_file = File::open(src)?;
    let block_data = unsafe { MmapOptions::new().map(&block_file)? };

    let dest_url = dest.as_url();

    file_dougu::write_contents(
        &dest_url,
        &block_data,
        file_dougu::mime::MimeType::OctetStream,
        None,
        None,
    )
    .await?;
    Ok(())
}

pub async fn download_block_list_file<'a>(
    src: &CloudBlockListFilePath<'a>,
    dest: &Path,
) -> Result<Option<()>> {
    let src_url = src.as_url();

    let contents = file_dougu::get_file_contents(&src_url, None, None).await?;

    match contents {
        Some(contents_data) => {
            let mut block_list_file = if dest.exists() {
                OpenOptions::new().write(true).truncate(true).open(dest)?
            } else {
                create_parent_dir_if_not_exists(dest)?;
                OpenOptions::new().create(true).write(true).open(dest)?
            };

            block_list_file.write_all(&contents_data)?;
            Ok(Some(()))
        }
        None => Ok(None),
    }
}

pub fn extract_metrics_from_url(dest_url: &str) -> Result<Metrics> {
    let captured = BLOCK_LIST_FILE_PATTERN.captures(dest_url);
    if let Some(captured) = captured {
        if let Some(matched) = captured.get(1) {
            let metrics =
                Metrics::new(matched.as_str()).map_err(CloudStorageError::InvalidMetricsName)?;
            return Ok(metrics);
        }
    }

    Err(CloudStorageError::InvalidBlockListFileUrl(
        dest_url.to_string(),
    ))
}

pub async fn list_block_list_files<'a>(dest_url: String) -> Result<Vec<String>> {
    let list = file_dougu::list_files(&dest_url, None).await?;

    Ok(list)
}

pub async fn upload_block_list_file<'a>(
    src: &Path,
    dest: &CloudBlockListFilePath<'a>,
) -> Result<()> {
    let block_file = File::open(src)?;
    let block_data = unsafe { MmapOptions::new().map(&block_file)? };

    let dest_url = dest.as_url();

    file_dougu::write_contents(
        &dest_url,
        &block_data,
        file_dougu::mime::MimeType::OctetStream,
        None,
        None,
    )
    .await?;
    Ok(())
}

pub async fn is_lock_file_exists<'a>(lock_file_path: &CloudLockfilePath<'a>) -> Result<bool> {
    let gcs_file = file_dougu::gcs::GcsFile::new(lock_file_path.as_url())?;

    let exists = gcs_file.is_exists_with_retry(None).await?;
    Ok(exists)
}

pub async fn create_lock_file<'a>(lock_file_path: &CloudLockfilePath<'a>) -> Result<()> {
    let gcs_file = file_dougu::gcs::GcsFile::new(lock_file_path.as_url())?;
    gcs_file
        .write_with_retry("l".as_bytes(), file_dougu::mime::MimeType::Text, None, None)
        .await?;
    Ok(())
}

pub async fn remove_lock_file<'a>(lock_file_path: &CloudLockfilePath<'a>) -> Result<()> {
    let gcs_file = file_dougu::gcs::GcsFile::new(lock_file_path.as_url())?;
    gcs_file.delete_with_retry(None).await?;
    Ok(())
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::Metrics;

    #[test]
    pub fn extract_metrics_from_url_test() {
        let result = extract_metrics_from_url("gs://some_bucket/aaa/blocklist/some_metrics.list");
        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(Metrics::new("some_metrics").unwrap(), result);
    }
}
