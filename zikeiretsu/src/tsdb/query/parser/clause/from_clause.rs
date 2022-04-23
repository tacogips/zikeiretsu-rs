use pest::iterators::Pair;

use crate::tsdb::query::parser::*;

#[derive(Debug)]
pub struct FromClause<'q> {
    pub from: &'q str,
}

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<FromClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::FROM_CLAUSE {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::FROM_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut from: Option<&'q str> = None;
    for each in pair.into_inner() {
        if each.as_rule() == Rule::METRICS_NAME {
            from = Some(each.as_str())
        }
    }

    // if it might be a bug if the result could not pass validation below.
    match from {
        None => Err(ParserError::EmptyColumns("select clause".to_string())),
        Some(from) => {
            let from_clause = FromClause { from };
            Ok(from_clause)
        }
    }
}
