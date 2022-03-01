mod columns_parser;
mod datetime_filter_parser;
mod from_clause;
mod function_parser;
mod order_or_limit_clause;
mod select_clause;
mod timezone_parser;
mod where_clause;
mod with_clause;

use chrono::{DateTime, FixedOffset, TimeZone};
use log;
use pest::{error::Error as PestError, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

#[derive(Parser)]
#[grammar = "tsdb/query/query.pest"]
pub struct QueryGrammer {}

type DateString = str;

#[derive(Debug)]
pub struct ColumnName<'q>(&'q str);

#[derive(Debug)]
pub enum Column<'q> {
    Asterick,
    ColumnName(ColumnName<'q>),
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("{0}")]
    PestError(#[from] PestError<Rule>),

    #[error("Invalid grammer. this might be a bug: {0}")]
    InvalidGrammer(String),

    #[error("Unexpected Pair expect: {0}, actual: {1}. This might be cause of a bug")]
    UnexpectedPair(String, String),

    #[error("invalid column name:{0}")]
    InvalidColumnName(String),

    #[error("empty columns:{0}")]
    EmptyColumns(String),

    #[error("empty table name")]
    EmptyTableName,

    #[error("invalid time offset:{0}. e.g. +09:00")]
    InvalidTimeOffset(String),

    #[error("time offset outofbound:{0}. ")]
    TimeOffsetOutOfBound(i32),
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
    pub select_columns: Option<Vec<Column<'q>>>,
}

#[derive(Debug)]
pub struct FromClause<'q> {
    pub from: Option<&'q str>,
}

#[derive(Debug)]
pub struct WithClause<'q> {
    pub def_columns: Option<Vec<Column<'q>>>,
    pub def_timezone: Option<FixedOffset>,
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
    AscBy(ColumnName<'q>),
    DescBy(ColumnName<'q>),
}

#[derive(Debug)]
pub enum DatetimeFilter<'q> {
    In(
        ColumnName<'q>,
        DatetimeFilterValue<'q>,
        DatetimeFilterValue<'q>,
    ),
    Gte(ColumnName<'q>, DatetimeFilterValue<'q>),
    Gt(ColumnName<'q>, DatetimeFilterValue<'q>),
    Lte(ColumnName<'q>, DatetimeFilterValue<'q>),
    Lt(ColumnName<'q>, DatetimeFilterValue<'q>),
    Equal(ColumnName<'q>, DatetimeFilterValue<'q>),
}

#[derive(Debug)]
pub enum DatetimeFilterValue<'a> {
    DateString(DateTime<FixedOffset>, Option<FixedOffset>),
    Function(BuildinDatetimeFunction, Option<FixedOffset>),
}

#[derive(Debug)]
pub enum TimeOffsetUnit {
    MicroSecond,
    Millisecond,
    Second,
    Minutes,
    Hour,
    Day,
    Month,
    Year,
}

#[derive(Debug)]
pub enum BuildinDatetimeFunction {
    Today,
    Yesterday,
    Tomorrow,
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
    fn parse_chronos_1() {
        let pairs = QueryGrammer::parse(Rule::DATETIME, "today()  + 2 hours");

        assert!(pairs.is_ok());

        let mut pairs = pairs.unwrap();
        let from = pairs.next().unwrap();
        assert_eq!(from.as_rule(), Rule::DATETIME);
        assert_eq!(from.as_str(), "today()  + 2 hours");
    }

    #[test]
    fn parse_chronos_2() {
        let pairs = QueryGrammer::parse(Rule::DATETIME, "'2012-12-13 9:00:00+09:00' - 1hour ");

        assert!(pairs.is_ok());

        let mut pairs = pairs.unwrap();
        let from = pairs.next().unwrap();
        assert_eq!(from.as_rule(), Rule::DATETIME);
        assert_eq!(from.as_str(), "'2012-12-13 9:00:00+09:00' - 1hour");
    }

    #[test]
    fn parse_query_1() {
        let query = r#"with

        cols = [is_buy, volume, price],
 	   tz = +9
select *
 from trades  "#;

        let parsed_query = parse_query(query);

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

        assert!(parsed_query.is_ok());
    }

    #[test]
    fn parse_query_4() {
        let query = r#"with

        cols = [is_buy, volume, price],
 	   tz = +9
select *
from trades
 offset 10 limit 10

 "#;

        let parsed_query = parse_query(query);

        assert!(parsed_query.is_ok());
    }
}
