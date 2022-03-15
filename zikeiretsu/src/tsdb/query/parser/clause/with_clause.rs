use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

use crate::tsdb::query::parser::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<WithClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::WITH_CLAUSE {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::WITH_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut with_clause = WithClause {
        def_columns: None,
        def_timezone: None,
    };
    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::DEFINE_COLUMNS => {
                for each_in_define_columns in each.into_inner() {
                    if each_in_define_columns.as_rule() == Rule::COLUMNS {
                        let columns = columns_parser::parse(each_in_define_columns, false)?;
                        with_clause.def_columns = Some(columns)
                    }
                }
            }

            Rule::DEFINE_TZ => {
                for each_in_define_tz in each.into_inner() {
                    if each_in_define_tz.as_rule() == Rule::TIMEZONE_OFFSET_VAL {
                        let timezone = timezone_parser::parse_timezone_offset(each_in_define_tz)?;

                        with_clause.def_timezone = Some(timezone)
                    }
                }
            }

            _ => {}
        }
    }

    Ok(with_clause)
}