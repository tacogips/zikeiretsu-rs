pub mod bools;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, CompressError>;

#[derive(Error, Debug)]
pub enum CompressError {
    #[error("compress boolean error {0}")]
    BoolCompressError(String),

    #[error("decompress boolean error {0}")]
    BoolDecompressError(String),

    #[error("bits ope error {0}")]
    BitsOpeError(#[from] bits_ope::Error),
}
