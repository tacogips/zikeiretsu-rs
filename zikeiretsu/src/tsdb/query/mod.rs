pub mod executor;
pub mod lexer;
pub mod parser;

use crate::EngineError;
pub use executor::*;
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_and_interpret_1() {
        let query = "select * from .metrics;";
        let result = parse_query(query);

        assert!(result.is_ok());

        let result = interpret(result.unwrap());

        assert!(result.is_ok());
    }
}
