mod block_list_cache;
pub use block_list_cache::*;

pub(crate) struct Cache {
    pub block_list_cache: block_list_cache::BlockListCache,
}

impl Cache {
    pub fn new() -> Self {
        Cache {
            block_list_cache: BlockListCache::new(),
        }
    }
}
