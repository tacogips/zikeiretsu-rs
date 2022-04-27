mod block_cache;
mod block_list_cache;
pub(crate) use block_cache::*;
pub(crate) use block_list_cache::*;

pub(crate) struct Cache {
    pub block_list_cache: block_list_cache::BlockListCache,
    pub block_cache: block_cache::BlockCache,
}

impl Cache {
    pub fn new(block_cache_size: usize) -> Self {
        Cache {
            block_list_cache: BlockListCache::new(),
            block_cache: block_cache::BlockCache::new(block_cache_size),
        }
    }
}
