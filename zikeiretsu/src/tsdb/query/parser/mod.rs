mod timezone;

mod from_clause;
mod order_or_limit_clause;
mod select_clause;
mod where_clause;
mod with_clause;

use log;
use pest::{error::Error as PestError, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

#[derive(Parser)]
#[grammar = "tsdb/query/query.pest"]
pub struct QueryGrammer {}

type ColumnName = str;
type DateString = str;
type TimeZone = str;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("{0}")]
    PestError(#[from] PestError<Rule>),

    #[error("Invalid grammer. this might be a bug: {0}")]
    InvalidGrammer(String),
}

pub type Result<T> = std::result::Result<T, QueryError>;

#[derive(Debug)]
pub struct ParsedQuery<'q> {
    pub with: Option<WithClause<'q>>,
    pub select: Option<SelectClause<'q>>,
    pub from: Option<FromClause<'q>>,
    pub r#where: Option<WhereClause<'q>>,
    pub order_or_limit: Option<OrderOrLimitClause<'q>>,
}

impl<'q> ParsedQuery<'q> {
    pub fn empty() -> ParsedQuery<'q> {
        ParsedQuery {
            with: None,
            select: None,
            from: None,
            r#where: None,
            order_or_limit: None,
        }
    }
}

#[derive(Debug)]
pub struct SelectClause<'q> {
    pub select_columns: Option<Vec<&'q str>>,
}

#[derive(Debug)]
pub struct FromClause<'q> {
    pub from: Option<&'q str>,
}

#[derive(Debug)]
pub struct WithClause<'q> {
    pub def_columns: Option<Vec<&'q ColumnName>>,
    pub def_timezone: Option<&'q TimeZone>,
}

#[derive(Debug)]
pub struct WhereClause<'q> {
    datetime_filter: Option<DatetimeFilter<'q>>,
}

#[derive(Debug)]
pub struct OrderOrLimitClause<'q> {
    order_by: Option<Order<'q>>,
    limit: Option<u64>,
    offset: Option<u64>,
}

#[derive(Debug)]
pub enum Order<'q> {
    AscBy(&'q ColumnName),
    DescBy(&'q ColumnName),
}

#[derive(Debug)]
pub enum DatetimeFilter<'q> {
    In(
        &'q ColumnName,
        DatetimeFilterValue<'q>,
        DatetimeFilterValue<'q>,
    ),
    Gte(&'q ColumnName, DatetimeFilterValue<'q>),
    Gt(&'q ColumnName, DatetimeFilterValue<'q>),
    Lte(&'q ColumnName, DatetimeFilterValue<'q>),
    Lt(&'q ColumnName, DatetimeFilterValue<'q>),
    Equal(&'q ColumnName, DatetimeFilterValue<'q>),
}

#[derive(Debug)]
pub enum DatetimeFilterValue<'a> {
    DateString(&'a DateString),
    Function(BuildinFunction),
}

#[derive(Debug)]
pub enum BuildinFunction {
    Today,
}

pub fn parse_query<'q>(query: &'q str) -> Result<ParsedQuery<'q>> {
    let pairs = QueryGrammer::parse(Rule::QUERY, query)?;

    //TODO(tacogips) for debugging
    println!("==== {:?}", "ssss");

    let mut parsed_query = ParsedQuery::<'q>::empty();
    for each_pair in pairs.into_iter() {
        match each_pair.as_rule() {
            Rule::WITH_CLAUSE => {
                let with_clause = with_clause::parse(each_pair)?;
                parsed_query.with = Some(with_clause);
            }
            Rule::SELECT_CLAUSE => {
                let select_clause = select_clause::parse(each_pair)?;
                parsed_query.select = Some(select_clause);
            }
            Rule::FROM_CLAUSE => {
                let from_clause = from_clause::parse(each_pair)?;
                parsed_query.from = Some(from_clause);
            }
            Rule::WHERE_CLAUSE => {
                let where_clause = where_clause::parse(each_pair)?;
                parsed_query.r#where = Some(where_clause);
            }
            Rule::ORDER_OR_LIMIT_CLAUSE => {
                let order_or_limit_clause = order_or_limit_clause::parse(each_pair)?;
                parsed_query.order_or_limit = Some(order_or_limit_clause);
            }
            Rule::QUERY => {}
            _ => {
                let msg = format!(
                    "invalid grammer RULE:{:?} {:?}",
                    each_pair.as_rule(),
                    each_pair.as_str()
                );
                log::error!("{} ", msg);

                return Err(QueryError::InvalidGrammer(msg));
            }
        }
    }
    Ok(parsed_query)
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn parse_timezone_offset_val() {
        let pairs = QueryGrammer::parse(Rule::TIMEZONE_OFFSET_VAL, "+1");

        assert!(pairs.is_ok());
        let mut pairs = pairs.unwrap();

        let tz = pairs.next().unwrap();
        assert_eq!(tz.as_rule(), Rule::TIMEZONE_OFFSET_VAL);
        assert_eq!(tz.as_str(), "+1");
    }

    #[test]
    fn parse_column() {
        let pairs = QueryGrammer::parse(Rule::COLUMNS, "aa,bb,cc_cc,dd");

        assert!(pairs.is_ok());
        let mut pairs = pairs.unwrap();

        let tz = pairs.next().unwrap();
        assert_eq!(tz.as_rule(), Rule::COLUMNS);
        assert_eq!(tz.as_str(), "aa,bb,cc_cc,dd");
    }

    #[test]
    fn parse_column_with_kw() {
        let pairs = QueryGrammer::parse(Rule::COLUMNS, "aa,bb,cc_cc,dd, tz");

        assert!(pairs.is_ok());
    }

    #[test]
    fn parse_define_tz() {
        let pairs = QueryGrammer::parse(Rule::DEFINE_TZ, "tz = +9");

        assert!(pairs.is_ok());
        let mut pairs = pairs.unwrap();

        let tz = pairs.next().unwrap();
        assert_eq!(tz.as_rule(), Rule::DEFINE_TZ);
        assert_eq!(tz.as_str(), "tz = +9");
    }

    #[test]
    fn parse_with() {
        let pairs = QueryGrammer::parse(
            Rule::WITH_CLAUSE,
            r#"with        cols = [is_buy, volume, price] "#,
        );

        assert!(pairs.is_ok());
        let mut pairs = pairs.unwrap();

        let tz = pairs.next().unwrap();
        assert_eq!(tz.as_rule(), Rule::WITH_CLAUSE);
        assert_eq!(
            tz.as_str(),
            r#"with        cols = [is_buy, volume, price] "#,
        );
    }

    #[test]
    fn parse_with_with_tz() {
        let pairs = QueryGrammer::parse(
            Rule::WITH_CLAUSE,
            r#"with        cols = [is_buy, volume, price ] , tz =+9"#,
        );

        //TODO(tacogips) for debugging
        println!("==== {:?}", pairs);

        assert!(pairs.is_ok());
        let mut pairs = pairs.unwrap();

        let tz = pairs.next().unwrap();
        assert_eq!(tz.as_rule(), Rule::WITH_CLAUSE);
        assert_eq!(
            tz.as_str(),
            r#"with        cols = [is_buy, volume, price ] , tz =+9"#,
        );
    }

    #[test]
    fn parse_with_with_tz_then_columns() {
        let pairs = QueryGrammer::parse(
            Rule::WITH_CLAUSE,
            r#"with  tz =+9, cols = [is_buy, volume, price ]"#,
        );

        assert!(pairs.is_ok());
        let mut pairs = pairs.unwrap();

        let tz = pairs.next().unwrap();
        assert_eq!(tz.as_rule(), Rule::WITH_CLAUSE);
        assert_eq!(
            tz.as_str(),
            r#"with  tz =+9, cols = [is_buy, volume, price ]"#,
        );
    }

    #[test]
    fn parse_where() {
        let pairs = QueryGrammer::parse(Rule::WHERE_CLAUSE, "where ts in today()");

        assert!(pairs.is_ok());
        let mut pairs = pairs.unwrap();

        let tz = pairs.next().unwrap();
        assert_eq!(tz.as_rule(), Rule::WHERE_CLAUSE);
        assert_eq!(tz.as_str(), "where ts in today()");
    }

    #[test]
    fn parse_from() {
        let pairs = QueryGrammer::parse(Rule::FROM_CLAUSE, "from aaaa");

        assert!(pairs.is_ok());
        let mut pairs = pairs.unwrap();

        let tz = pairs.next().unwrap();
        assert_eq!(tz.as_rule(), Rule::FROM_CLAUSE);
        assert_eq!(tz.as_str(), "from aaaa");
    }

    #[test]
    fn parse_from_invalid() {
        let pairs = QueryGrammer::parse(Rule::FROM_CLAUSE, "from aaaa where ts in today()");

        assert!(pairs.is_ok());

        let mut pairs = pairs.unwrap();
        let from = pairs.next().unwrap();
        assert_eq!(from.as_rule(), Rule::FROM_CLAUSE);
        assert_eq!(from.as_str(), "from aaaa");
    }

    #[test]
    fn parse_offset() {
        let pairs = QueryGrammer::parse(Rule::OFFSET_CLAUSE, "offset 10");

        assert!(pairs.is_ok());

        let mut pairs = pairs.unwrap();
        let from = pairs.next().unwrap();
        assert_eq!(from.as_rule(), Rule::OFFSET_CLAUSE);
        assert_eq!(from.as_str(), "offset 10");
    }

    #[test]
    fn parse_query_1() {
        let query = r#"with

        cols = [is_buy, volume, price],
 	   tz = +9
select *
 from trades  "#;

        let parsed_query = parse_query(query);
        //TODO(tacogips) for debugging
        println!("==== {:?}", parsed_query);

        assert!(parsed_query.is_ok());

        //TODO(tacogips) assertion
    }

    #[test]
    fn parse_query_2() {
        let query = r#"with

        cols = [is_buy, volume, price],
 	   tz = +9
select *
from trades

 where ts in today() "#;

        let parsed_query = parse_query(query);

        assert!(parsed_query.is_ok());
    }

    #[test]
    fn parse_query_3() {
        let query = r#"with

        cols = [is_buy, volume, price],
 	   tz = +9
select *
from trades

 where ts in today()
 offset 10 limit 10
 order_by  asc

 "#;

        let parsed_query = parse_query(query);

        //TODO(tacogips) for debugging
        println!("==== {:?}", parsed_query);

        assert!(parsed_query.is_ok());
    }
}
