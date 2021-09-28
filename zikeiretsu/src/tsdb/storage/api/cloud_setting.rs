use crate::tsdb::cloudstorage::CloudStorage;

#[derive(Clone)]
pub struct CloudStorageSetting {
    pub update_block_list: bool,
    pub download_block_list_if_not_exits: bool,
    pub download_block_if_not_exits: bool,
    pub upload_data_after_write: bool,
    pub cloud_storage: CloudStorage,
}

impl CloudStorageSetting {
    pub fn builder(cloud_storage: CloudStorage) -> CloudStorageSettingBuilder {
        CloudStorageSettingBuilder {
            update_block_list: true,
            download_block_list_if_not_exits: true,
            download_block_if_not_exits: true,
            upload_data_after_write: true,
            cloud_storage,
        }
    }
}

pub struct CloudStorageSettingBuilder {
    update_block_list: bool,
    download_block_list_if_not_exits: bool,
    download_block_if_not_exits: bool,
    upload_data_after_write: bool,
    cloud_storage: CloudStorage,
}

impl CloudStorageSettingBuilder {
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
            cloud_storage,
        } = self;

        CloudStorageSetting {
            update_block_list,
            download_block_list_if_not_exits,
            download_block_if_not_exits,
            upload_data_after_write,
            cloud_storage,
        }
    }
}
