use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

use super::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<WhereClause<'q>> {
    pair.into_inner();

    Ok(WhereClause {
        datetime_filter: None,
    })
}
