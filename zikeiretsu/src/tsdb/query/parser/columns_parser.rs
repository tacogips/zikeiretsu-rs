use once_cell::sync::OnceCell;
use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use std::collections::HashSet;
use thiserror::Error;

use super::*;

pub fn parse<'q>(pair: Pair<'q, Rule>, allow_asterisk: bool) -> Result<Vec<Column<'q>>> {
    if pair.as_rule() != Rule::COLUMNS {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::COLUMNS),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut columns = Vec::<Column<'q>>::new();
    for each_pair_in_columns in pair.into_inner() {
        if each_pair_in_columns.as_rule() == Rule::COLUMN_NAME {
            let column_str = each_pair_in_columns.as_str();
            if column_str == "*" {
                if allow_asterisk {
                    columns.push(Column::Asterick)
                } else {
                    return Err(QueryError::InvalidColumnName(column_str.to_string()));
                }
            } else {
                columns.push(Column::ColumnName(ColumnName(
                    each_pair_in_columns.as_str(),
                )))
            }
        }
    }
    Ok(columns)
}

static INVALID_COLUMN_NAME: OnceCell<HashSet<&'static str>> = OnceCell::new();

pub(crate) fn invalid_colum_names() -> &'static HashSet<&'static str> {
    INVALID_COLUMN_NAME.get_or_init(|| {
        let mut s = HashSet::new();
        s.insert("SELECT");
        s.insert("FROM");
        s.insert("WITH");
        s.insert("WHERE");
        s.insert("AND");
        s.insert("OR");
        s.insert("DESC");
        s.insert("ASC");
        s.insert("OFFSET");
        s.insert("LIMIT");
        s.insert("COLS");
        s.insert("TZ");
        s
    })
}

fn validate_column_name(column_name: &str) -> Result<()> {
    if invalid_colum_names().contains(column_name.to_uppercase().as_str()) {
        return Err(QueryError::InvalidColumnName(column_name.to_string()));
    }
    Ok(())
}
