use pest::iterators::Pair;

use crate::tsdb::query::parser::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<OrderOrLimitClause<'q>> {
    pair.into_inner();

    //TODO(tacogips) imple
    Ok(OrderOrLimitClause {
        order_by: None,
        limit: None,
        offset: None,
    })
}
