use crate::tsdb::metrics::Metrics;
use crate::tsdb::storage::block_list::BlockTimestamp;
use crate::tsdb::TimeSeriesDataFrame;
use lru::LruCache;

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct BlockCacheKey {
    database_name: String,
    metrics: Metrics,
    block_timestamp: BlockTimestamp,
}

pub(crate) struct BlockCache {
    pub block_dfs: LruCache<BlockCacheKey, TimeSeriesDataFrame>,
}

impl BlockCache {
    pub fn new(cache_size: usize) -> Self {
        let block_dfs = LruCache::new(cache_size);
        Self { block_dfs }
    }

    pub async fn get(
        &mut self,
        database_name: String,
        metrics: Metrics,
        block_timestamp: BlockTimestamp,
    ) -> Option<&TimeSeriesDataFrame> {
        let key = BlockCacheKey {
            database_name,
            block_timestamp,
            metrics,
        };
        self.block_dfs.get(&key)
    }

    pub async fn write(
        &mut self,
        database_name: String,
        metrics: Metrics,
        block_timestamp: BlockTimestamp,
        block: TimeSeriesDataFrame,
    ) {
        let key = BlockCacheKey {
            database_name,
            metrics,
            block_timestamp,
        };
        self.block_dfs.put(key, block);
    }
}
