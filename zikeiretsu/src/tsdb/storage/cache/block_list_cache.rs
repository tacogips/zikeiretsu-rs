use super::super::block_list::BlockList;
use crate::tsdb::metrics::Metrics;
use std::collections::HashMap;

pub(crate) struct BlockListCache {
    pub block_lists: HashMap<Metrics, BlockList>,
}

impl BlockListCache {
    pub fn new() -> Self {
        let block_lists = HashMap::<Metrics, BlockList>::new();
        Self { block_lists }
    }

    //TODO(tacogips) block list caceh shoud be unique by database_naem and metrics name
    pub async fn get(&self, metrics: &Metrics) -> Option<&BlockList> {
        self.block_lists.get(metrics)
    }

    pub async fn write(&mut self, metrics: &Metrics, block_list: BlockList) {
        self.block_lists.insert(metrics.clone(), block_list);
    }
}
