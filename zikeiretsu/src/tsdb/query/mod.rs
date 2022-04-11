pub mod context;
pub mod eval;
pub mod lexer;
pub mod parser;

use crate::EngineError;
use context::*;
pub use context::*;
pub use eval::*;
pub use lexer::*;
use parser::*;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, QueryError>;
#[derive(Error, Debug)]
pub enum QueryError {
    #[error("engine error :{0}")]
    EngineError(#[from] EngineError),

    #[error("engine error :{0}")]
    ParserError(#[from] ParserError),
}
