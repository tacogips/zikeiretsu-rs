pub mod metrics;
pub mod metrics_list;
pub mod output;

use crate::tsdb::query::{
    parser::{parse_query, ParserError},
    DBContext,
};

use crate::tsdb::engine::EngineError;
use crate::tsdb::lexer::{interpret, InterpretedQuery, InterpretedQueryCondition, LexerError};
pub use metrics::*;
pub use metrics_list::*;
pub use output::*;
use std::io::Error as IoError;
use thiserror::Error;

use crate::tsdb::dataframe::DataframeError;
#[derive(Error, Debug)]
pub enum EvalError {
    #[error("repl read line error {0}")]
    IoError(#[from] IoError),

    #[error("dataframe error {0}")]
    DataframeError(#[from] DataframeError),

    #[error("parser error {0}")]
    ParserError(#[from] ParserError),

    #[error("lexer error {0}")]
    LexerError(#[from] LexerError),

    #[error("engine error {0}")]
    EngineError(#[from] EngineError),
}

pub type Result<T> = std::result::Result<T, EvalError>;

pub async fn execute(ctx: &DBContext, query: &str) -> Result<()> {
    let parsed_query = parse_query(query)?;
    let interpreted_query = interpret(parsed_query)?;
    match interpreted_query {
        InterpretedQuery::ListMetrics(output_condition) => {}
        InterpretedQuery::Metrics(query_condition) => {}
    }

    Ok(())
}
