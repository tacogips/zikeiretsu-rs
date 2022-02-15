use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

use super::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<WithClause<'q>> {
    let mut with_clause = WithClause {
        def_columns: None,
        def_timezone: None,
    };
    //for each in pair.into_inner() {
    //
    //    match each.as_rule() {
    //        Rule::DEFINE_COLUMNS => {
    //            //let with_clause = with_clause::parse(each_pair)?;
    //            panic!("{:?}", each)
    //        }
    //        _ => {}
    //    }
    //}

    Ok(with_clause)
}
