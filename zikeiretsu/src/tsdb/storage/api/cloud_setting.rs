use crate::tsdb::cloudstorage::CloudStorage;

#[derive(Clone)]
pub struct CloudSetting {
    pub update_block_list: bool,
    pub download_block_list_if_not_exits: bool,
    pub download_block_if_not_exits: bool,
    pub upload_data_after_write: bool,
    pub cloud_storage: CloudStorage,
}

impl CloudSetting {
    pub fn builder(cloud_storage: CloudStorage) -> CloudSettingBuilder {
        CloudSettingBuilder {
            update_block_list: false,
            download_block_list_if_not_exits: false,
            download_block_if_not_exits: false,
            upload_data_after_write: false,
            cloud_storage,
        }
    }
}

pub struct CloudSettingBuilder {
    update_block_list: bool,
    download_block_list_if_not_exits: bool,
    download_block_if_not_exits: bool,
    upload_data_after_write: bool,
    cloud_storage: CloudStorage,
}

impl CloudSettingBuilder {
    pub fn update_block_list(mut self, update_block_list: bool) -> CloudSettingBuilder {
        self.update_block_list = update_block_list;
        self
    }

    pub fn download_block_if_not_exits(
        mut self,
        download_block_if_not_exits: bool,
    ) -> CloudSettingBuilder {
        self.download_block_if_not_exits = download_block_if_not_exits;
        self
    }

    pub fn download_block_list_if_not_exits(
        mut self,
        download_block_list_if_not_exits: bool,
    ) -> CloudSettingBuilder {
        self.download_block_list_if_not_exits = download_block_list_if_not_exits;
        self
    }

    pub fn build(self) -> CloudSetting {
        let CloudSettingBuilder {
            update_block_list,
            download_block_list_if_not_exits,
            download_block_if_not_exits,
            upload_data_after_write,
            cloud_storage,
        } = self;

        CloudSetting {
            update_block_list,
            download_block_list_if_not_exits,
            download_block_if_not_exits,
            upload_data_after_write,
            cloud_storage,
        }
    }
}
