use pest::iterators::Pair;

use crate::tsdb::query::parser::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<WhereClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::WHERE_CLAUSE {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::WHERE_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut where_clause = WhereClause {
        datetime_filter: None,
    };

    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::FILTER => {
                for each_filter in each.into_inner() {
                    match each_filter.as_rule() {
                        Rule::DATETIME_FILTER => {
                            let datetime_filter = datetime_filter_parser::parse(each_filter)?;
                            where_clause.datetime_filter = Some(datetime_filter);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok(where_clause)
}
