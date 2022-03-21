use pest::iterators::Pair;

use crate::tsdb::query::parser::*;

#[derive(Debug)]
pub struct FromClause<'q> {
    pub from: Option<&'q str>,
}

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<FromClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::FROM_CLAUSE {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::FROM_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut from_clause = FromClause { from: None };

    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::TABLE_NAME => from_clause.from = Some(each.as_str()),
            _ => {}
        }
    }

    // if it might be a bug if the result could not pass validation below.
    if from_clause.from.is_none() {
        return Err(QueryError::EmptyColumns("select clause".to_string()));
    };

    Ok(from_clause)
}
