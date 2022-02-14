use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

use super::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<WithClause<'q>> {
    pair.into_inner();
    Ok(WithClause {
        def_columns: None,
        def_timezone: None,
    })
}
