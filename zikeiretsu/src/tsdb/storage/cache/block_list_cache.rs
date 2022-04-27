use super::super::block_list::BlockList;
use crate::tsdb::metrics::Metrics;
use std::collections::HashMap;

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct BlockListCacheKey {
    database_name: String,
    metrics: Metrics,
}

pub(crate) struct BlockListCache {
    pub block_lists: HashMap<BlockListCacheKey, BlockList>,
}

impl BlockListCache {
    pub fn new() -> Self {
        let block_lists = HashMap::<BlockListCacheKey, BlockList>::new();
        Self { block_lists }
    }

    pub async fn get(&self, database_name: String, metrics: Metrics) -> Option<&BlockList> {
        let key = BlockListCacheKey {
            database_name,
            metrics,
        };
        self.block_lists.get(&key)
    }

    pub async fn write(&mut self, database_name: String, metrics: Metrics, block_list: BlockList) {
        let key = BlockListCacheKey {
            database_name,
            metrics,
        };
        self.block_lists.insert(key, block_list);
    }
}
