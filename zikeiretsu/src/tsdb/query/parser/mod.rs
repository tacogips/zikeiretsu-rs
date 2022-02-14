mod timezone;

mod from_clause;
mod select_clause;
mod where_clause;
mod with_clause;

use pest::{error::Error as PestError, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

#[derive(Parser)]
#[grammar = "tsdb/query/query.pest"]
pub struct QueryGrammer {}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("{0}")]
    PestError(#[from] PestError<Rule>),

    #[error("Invalid grammer. this might be a bug: {0}")]
    InvalidGrammer(String),
}

pub type Result<T> = std::result::Result<T, QueryError>;

pub struct ParsedQuery<'q> {
    pub with: Option<WithClause<'q>>,
    pub select: Option<Vec<&'q str>>,
    pub select_columns: Option<Vec<&'q str>>,
    pub from: Option<&'q str>,
    pub r#where: Option<WhereClause<'q>>,
    pub order_or_limit: Option<OrderOrLImitClause<'q>>,
}

impl<'q> ParsedQuery<'q> {
    pub fn empty() -> ParsedQuery<'q> {
        ParsedQuery {
            with: None,
            select: None,
            select_columns: None,
            from: None,
            r#where: None,
            order_or_limit: None,
        }
    }
}

pub struct WithClause<'q> {
    pub def_columns: Option<Vec<&'q str>>,
    pub def_timezone: Option<&'q str>,
}

pub struct WhereClause<'q> {
    ts_filter: TsFilter<'q>,
}

pub struct OrderOrLImitClause<'q> {
    order_by: Option<Order<'q>>,
    limit: Option<u64>,
    offset: Option<u64>,
}

pub enum Order<'q> {
    AscBy(&'q str),
    DescBy(&'q str),
}

pub enum TsFilter<'q> {
    In(TsFilterValue<'q>, TsFilterValue<'q>),
    Gte(TsFilterValue<'q>),
    Gt(TsFilterValue<'q>),
    Lte(TsFilterValue<'q>),
    Lt(TsFilterValue<'q>),
    Equal(TsFilterValue<'q>),
}

pub enum TsFilterValue<'a> {
    DateString(&'a str),
    Function,
}

pub fn parse_query<'q>(query: &'q str) -> Result<ParsedQuery<'q>> {
    let pairs = QueryGrammer::parse(Rule::QUERY, query)?;

    let mut parsed_query = ParsedQuery::<'q>::empty();
    for each_pair in pairs.into_iter() {
        match each_pair.as_rule() {
            Rule::WITH_CLAUSE => {
                let with_clause = with_clause::parse(each_pair)?;

                parsed_query.with = Some(with_clause);
            }
            Rule::SELECT_CLAUSE => {
                unimplemented!()
            }
            Rule::FROM_CLAUSE => {
                unimplemented!()
            }
            Rule::WHERE_CLAUSE => {
                unimplemented!()
            }
            Rule::ORDER_OR_LIMIT_CLAUSE => {
                unimplemented!()
            }
            _ => return Err(QueryError::InvalidGrammer("".to_string())),
        }
    }
    unimplemented!()
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn parse_timezone_offset_val() {
        let pairs = QueryGrammer::parse(Rule::TIMEZONE_OFFSET_VAL, "+1");

        for each in pairs.unwrap() {
            if each.as_rule() == Rule::TIMEZONE_OFFSET_VAL {
                //TODO(tacogips) for debugging
                //TODO(tacogips) for debugging
                println!("==== {:?}", each.as_rule());
            }
        }

        assert!(false);
    }
}
