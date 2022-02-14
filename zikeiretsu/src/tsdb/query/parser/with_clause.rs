use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

use super::*;

pub fn parse<'q>(with_clause_pair: Pair<'q, Rule>) -> Result<WithClause<'q>> {
    with_clause_pair.into_inner();
    unimplemented!()
}
