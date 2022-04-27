use crate::tsdb::metrics::Metrics;
use crate::tsdb::TimeSeriesDataFrame;
use std::collections::HashMap;

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct BlockCacheKey {
    database_name: String,
    metrics: Metrics,
}

pub(crate) struct BlockCache {
    pub block_lists: HashMap<BlockCacheKey, TimeSeriesDataFrame>,
}

impl BlockCache {
    pub fn new() -> Self {
        let block_lists = HashMap::<BlockCacheKey, TimeSeriesDataFrame>::new();
        Self { block_lists }
    }

    pub async fn get(
        &self,
        database_name: String,
        metrics: Metrics,
    ) -> Option<&TimeSeriesDataFrame> {
        let key = BlockCacheKey {
            database_name,
            metrics,
        };
        self.block_lists.get(&key)
    }

    pub async fn write(
        &mut self,
        database_name: String,
        metrics: Metrics,
        block: TimeSeriesDataFrame,
    ) {
        let key = BlockCacheKey {
            database_name,
            metrics,
        };
        self.block_lists.insert(key, block);
    }
}
