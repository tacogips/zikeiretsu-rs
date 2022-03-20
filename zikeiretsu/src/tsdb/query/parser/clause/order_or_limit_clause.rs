use pest::iterators::Pair;

use crate::tsdb::query::parser::*;

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<OrderOrLimitClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::ORDER_OR_LIMIT_CLAUSE {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::ORDER_OR_LIMIT_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut order_by: Option<Order<'q>> = None;
    let mut limit: Option<usize> = None;
    let mut offset: Option<usize> = None;

    for each_inner in pair.into_inner() {
        match each_inner.as_rule() {
            Rule::ORDER_CLAUSE => {
                order_by = Some(parse_order(each_inner)?);
            }
            Rule::OFFSET_CLAUSE => {
                limit = Some(parse_limit(each_inner)?);
            }
            Rule::LIMIT_CLAUSE => {
                offset = Some(parse_offset(each_inner)?);
            }

            r => {
                return Err(QueryError::InvalidGrammer(format!(
                    "unknown term in build in datetime delta : {r:?}"
                )));
            }
        }
    }

    //TODO(tacogips) imple
    Ok(OrderOrLimitClause {
        order_by,
        limit,
        offset,
    })
}

pub fn parse_order<'q>(pair: Pair<'q, Rule>) -> Result<Order<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::ORDER_CLAUSE {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::ORDER_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }
    let mut is_desc = false;
    let mut column_name: Option<&'q str> = None;
    // cutting the corner for now
    for each_inner in pair.into_inner() {
        match each_inner.as_rule() {
            Rule::KW_DESC => is_desc = true,
            Rule::KW_TIMESTAMP => column_name = Some(each_inner.as_str()),
            _ => { /* */ }
        }
    }

    match column_name {
        None => {
            return Err(QueryError::InvalidGrammer(format!(
                "no column name which order by "
            )))
        }

        Some(column_name) => {
            if is_desc {
                Ok(Order::DescBy(ColumnName(column_name)))
            } else {
                Ok(Order::AscBy(ColumnName(column_name)))
            }
        }
    }
}

pub fn parse_offset<'q>(pair: Pair<'q, Rule>) -> Result<usize> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::OFFSET_CLAUSE {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::OFFSET_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    // cutting the corner for now
    for each_inner in pair.into_inner() {
        match each_inner.as_rule() {
            Rule::ASCII_DIGITS => {
                let offset = each_inner.as_str().parse::<usize>()?;
                return Ok(offset);
            }
            _ => { /* */ }
        }
    }

    Err(QueryError::InvalidGrammer(format!("invlalid offset")))
}
pub fn parse_limit<'q>(pair: Pair<'q, Rule>) -> Result<usize> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::LIMIT_CLAUSE {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::LIMIT_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    // cutting the corner for now
    for each_inner in pair.into_inner() {
        match each_inner.as_rule() {
            Rule::ASCII_DIGITS => {
                let offset = each_inner.as_str().parse::<usize>()?;
                return Ok(offset);
            }
            _ => { /* */ }
        }
    }

    Err(QueryError::InvalidGrammer(format!("invlalid offset")))
}
