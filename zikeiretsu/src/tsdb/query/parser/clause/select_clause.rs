use pest::iterators::Pair;

use crate::tsdb::query::parser::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<SelectClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::SELECT_CLAUSE {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::SELECT_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut select_clause = SelectClause {
        select_columns: None,
    };

    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::COLUMNS => {
                let columns = columns_parser::parse(each, false)?;
                select_clause.select_columns = Some(columns)
            }
            Rule::KW_ASTERISK => select_clause.select_columns = Some(vec![Column::Asterick]),
            _ => {}
        }
    }

    // if it might be a bug if the result could not pass validation below.
    match select_clause.select_columns {
        Some(cols) if cols.is_empty() => {
            return Err(QueryError::EmptyColumns("select clause".to_string()))
        }
        None => return Err(QueryError::EmptyColumns("select clause".to_string())),
        _ => { /* pass */ }
    };

    Ok(select_clause)
}
