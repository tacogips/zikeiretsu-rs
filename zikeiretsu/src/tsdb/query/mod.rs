mod context;
mod output;

use crate::EngineError;
pub use context::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("engine error :{0}")]
    EngineError(#[from] EngineError),
}
