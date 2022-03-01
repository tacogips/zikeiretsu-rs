use once_cell::sync::OnceCell;
use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use std::collections::HashSet;
use thiserror::Error;

use super::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<DatetimeFilter<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::DATETIME_FILTER {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::DATETIME_FILTER),
            format!("{:?}", pair.as_rule()),
        ));
    }

    unimplemented!()
    //let mut columns = Vec::<Column<'q>>::new();
    //for each_pair_in_columns in pair.into_inner() {
    //    if each_pair_in_columns.as_rule() == Rule::COLUMN_NAME {
    //        let column_str = each_pair_in_columns.as_str();
    //        if column_str == "*" {
    //            if allow_asterisk {
    //                columns.push(Column::Asterick)
    //            } else {
    //                return Err(QueryError::InvalidColumnName(column_str.to_string()));
    //            }
    //        } else {
    //            columns.push(Column::ColumnName(ColumnName(
    //                each_pair_in_columns.as_str(),
    //            )))
    //        }
    //    }
    //}
    //Ok(columns)
}
