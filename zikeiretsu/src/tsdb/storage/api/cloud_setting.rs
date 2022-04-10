use crate::tsdb::cloudstorage::CloudStorage;

#[derive(Clone, Debug)]
pub struct CloudStorageSetting {
    pub update_block_list: bool,
    pub download_block_list_if_not_exits: bool,
    pub download_block_if_not_exits: bool,
    pub upload_data_after_write: bool,
    pub remove_local_file_after_upload: bool,
}

impl Default for CloudStorageSetting {
    fn default() -> Self {
        Self {
            update_block_list: true,
            download_block_list_if_not_exits: true,
            download_block_if_not_exits: true,
            upload_data_after_write: true,
            remove_local_file_after_upload: false,
        }
    }
}

impl CloudStorageSetting {
    pub fn builder() -> CloudStorageSettingBuilder {
        let CloudStorageSetting {
            update_block_list,
            download_block_list_if_not_exits,
            download_block_if_not_exits,
            upload_data_after_write,
            remove_local_file_after_upload,
        } = CloudStorageSetting::default();

        CloudStorageSettingBuilder {
            update_block_list,
            download_block_list_if_not_exits,
            download_block_if_not_exits,
            upload_data_after_write,
            remove_local_file_after_upload,
        }
    }
}

pub struct CloudStorageSettingBuilder {
    update_block_list: bool,
    download_block_list_if_not_exits: bool,
    download_block_if_not_exits: bool,
    upload_data_after_write: bool,
    remove_local_file_after_upload: bool,
}

impl CloudStorageSettingBuilder {
    pub fn new_with_sync_when_download(cloud_storage: CloudStorage) -> Self {
        CloudStorageSettingBuilder {
            update_block_list: true,
            download_block_list_if_not_exits: true,
            download_block_if_not_exits: true,
            upload_data_after_write: false,
            remove_local_file_after_upload: false,
        }
    }

    pub fn remove_local_file_after_upload(
        mut self,
        remove_local_file_after_upload: bool,
    ) -> CloudStorageSettingBuilder {
        self.remove_local_file_after_upload = remove_local_file_after_upload;
        self
    }

    pub fn update_block_list(mut self, update_block_list: bool) -> CloudStorageSettingBuilder {
        self.update_block_list = update_block_list;
        self
    }

    pub fn download_block_if_not_exits(
        mut self,
        download_block_if_not_exits: bool,
    ) -> CloudStorageSettingBuilder {
        self.download_block_if_not_exits = download_block_if_not_exits;
        self
    }

    pub fn download_block_list_if_not_exits(
        mut self,
        download_block_list_if_not_exits: bool,
    ) -> CloudStorageSettingBuilder {
        self.download_block_list_if_not_exits = download_block_list_if_not_exits;
        self
    }

    pub fn build(self) -> CloudStorageSetting {
        let CloudStorageSettingBuilder {
            update_block_list,
            download_block_list_if_not_exits,
            download_block_if_not_exits,
            upload_data_after_write,
            remove_local_file_after_upload,
        } = self;

        CloudStorageSetting {
            update_block_list,
            download_block_list_if_not_exits,
            download_block_if_not_exits,
            upload_data_after_write,
            remove_local_file_after_upload,
        }
    }
}
