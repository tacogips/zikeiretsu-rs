use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

use crate::tsdb::query::parser::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<OrderOrLimitClause<'q>> {
    pair.into_inner();

    Ok(OrderOrLimitClause {
        order_by: None,
        limit: None,
        offset: None,
    })
}
