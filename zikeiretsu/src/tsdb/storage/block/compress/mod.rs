pub mod bools;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, CompressError>;

#[derive(Error, Debug)]
pub enum CompressError {
    #[error("compress boolean error {0}")]
    BoolCompress(String),

    #[error("decompress boolean error {0}")]
    BoolDecompress(String),

    #[error("bits ope error {0}")]
    BitsOpe(#[from] bits_ope::Error),
}
