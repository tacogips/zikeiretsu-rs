use pest::iterators::Pair;

use crate::tsdb::query::parser::*;

#[derive(Debug, PartialEq)]
pub struct OrderOrLimitClause<'q> {
    order_by: Option<Order<'q>>,
}

#[derive(Debug, PartialEq)]
pub enum Order<'q> {
    AscBy(ColumnName<'q>),
    DescBy(ColumnName<'q>),
}

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<OrderOrLimitClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::ORDER_OR_LIMIT_CLAUSE {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::ORDER_OR_LIMIT_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut order_by: Option<Order<'q>> = None;

    for each_inner in pair.into_inner() {
        match each_inner.as_rule() {
            Rule::ORDER_CLAUSE => {
                order_by = Some(parse_order(each_inner)?);
            }

            r => {
                return Err(ParserError::InvalidGrammer(format!(
                    "unknown term in build in datetime delta : {r:?}"
                )));
            }
        }
    }

    //TODO(tacogips) imple
    Ok(OrderOrLimitClause { order_by })
}

pub fn parse_order<'q>(pair: Pair<'q, Rule>) -> Result<Order<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::ORDER_CLAUSE {
        return Err(ParserError::UnexpectedPair(
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
        None => Err(ParserError::InvalidGrammer(
            "no column name which order by ".to_string(),
        )),

        Some(column_name) => {
            if is_desc {
                Ok(Order::DescBy(ColumnName(column_name)))
            } else {
                Ok(Order::AscBy(ColumnName(column_name)))
            }
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use pest::*;

    #[test]
    fn parse_order_limit_1() {
        let order_clause = r"order by ts asc limit 10 offset 3
            ";

        let pairs = QueryGrammer::parse(Rule::ORDER_OR_LIMIT_CLAUSE, order_clause);

        assert!(pairs.is_ok());

        let parsed = parse(pairs.unwrap().next().unwrap());

        assert_eq!(
            parsed.unwrap(),
            OrderOrLimitClause {
                order_by: Some(Order::AscBy(ColumnName("ts"))),
            }
        )
    }

    #[test]
    fn parse_order_limit_2() {
        let order_clause = r"order by ts desc offset 3
            ";

        let pairs = QueryGrammer::parse(Rule::ORDER_OR_LIMIT_CLAUSE, order_clause);

        assert!(pairs.is_ok());

        let parsed = parse(pairs.unwrap().next().unwrap());

        assert_eq!(
            parsed.unwrap(),
            OrderOrLimitClause {
                order_by: Some(Order::DescBy(ColumnName("ts"))),
            }
        )
    }
}
