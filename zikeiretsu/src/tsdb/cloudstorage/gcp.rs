use super::{file_path::*, CloudStorageError, Result};
use file_dougu;
use memmap2::MmapOptions;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::Write;
use std::path::Path;

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

            block_file.write(&contents_data)?;
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

            block_list_file.write(&contents_data)?;
            Ok(Some(()))
        }
        None => Ok(None),
    }
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
