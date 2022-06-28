use pest::iterators::Pair;

use crate::tsdb::query::parser::*;
use crate::tsdb::Metrics;

use crate::tsdb::query::parser::parts::DatetimeFilter;
#[derive(Debug, PartialEq)]
pub struct WhereClause<'q> {
    pub datetime_filter: Option<DatetimeFilter<'q>>,
    pub metrics_filter: Option<Metrics>,
}

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<WhereClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::WHERE_CLAUSE {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::WHERE_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut datetime_filter: Option<DatetimeFilter<'q>> = None;
    let mut metrics_filter: Option<Metrics> = None;
    for each in pair.into_inner() {
        if each.as_rule() == Rule::FILTER {
            for each_filter in each.into_inner() {
                match each_filter.as_rule() {
                    Rule::DATETIME_FILTER => {
                        let parsed_datetime_filter = datetime_filter_parser::parse(each_filter)?;
                        datetime_filter = Some(parsed_datetime_filter);
                    }

                    Rule::METRICS_FILTER => {
                        metrics_filter = Some(parse_metrics_filter(each_filter)?);
                    }

                    _ => { /* do nothing*/ }
                }
            }
        }
    }

    log::debug!("datetime filter clause : {datetime_filter:?}");
    Ok(WhereClause {
        datetime_filter,
        metrics_filter,
    })
}

pub fn parse_metrics_filter(pair: Pair<'_, Rule>) -> Result<Metrics> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::METRICS_FILTER {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::METRICS_FILTER),
            format!("{:?}", pair.as_rule()),
        ));
    }

    for each in pair.into_inner() {
        if each.as_rule() == Rule::METRICS_NAME {
            return Metrics::try_from(each.as_str())
                .map_err(|_| ParserError::InvalidMetricsError(each.as_str().to_string()));
        }
    }

    Err(ParserError::InvalidGrammer(
        "no metrics in metrics filter".to_string(),
    ))
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::tsdb::query::parser::parts::DatetimeDelta;
    use pest::*;

    use chrono::{format as chrono_format, DateTime, NaiveDateTime, NaiveTime, Utc};

    #[test]
    fn parse_where_1() {
        let where_clause = r"where  ts >= '2012-12-30'
            ";

        let pairs = QueryGrammer::parse(Rule::WHERE_CLAUSE, where_clause);

        assert!(pairs.is_ok());
        let parsed = parse(pairs.unwrap().next().unwrap());

        assert!(parsed.is_ok());

        let mut dt = chrono_format::Parsed::new();
        chrono_format::parse(
            &mut dt,
            "2012-12-30",
            chrono_format::StrftimeItems::new("%Y-%m-%d"),
        )
        .unwrap();

        let expected_datetime = DateTime::from_utc(
            NaiveDateTime::new(dt.to_naive_date().unwrap(), NaiveTime::from_hms(0, 0, 0)),
            Utc,
        );
        let expected = DatetimeFilterValue::DateString(expected_datetime, None);

        assert_eq!(
            parsed.unwrap(),
            WhereClause {
                datetime_filter: Some(DatetimeFilter::Gte(ColumnName("ts"), expected, None)),

                metrics_filter: None,
            }
        );
    }

    #[test]
    fn parse_where_2() {
        let where_clause = r"where  ts in ('2012-12-30', '2013-01-02') ";

        let pairs = QueryGrammer::parse(Rule::WHERE_CLAUSE, where_clause);

        assert!(pairs.is_ok());
        let parsed = parse(pairs.unwrap().next().unwrap());

        assert!(parsed.is_ok());

        let mut dt = chrono_format::Parsed::new();
        chrono_format::parse(
            &mut dt,
            "2012-12-30",
            chrono_format::StrftimeItems::new("%Y-%m-%d"),
        )
        .unwrap();
        let expected_datetime = DateTime::from_utc(
            NaiveDateTime::new(dt.to_naive_date().unwrap(), NaiveTime::from_hms(0, 0, 0)),
            Utc,
        );
        let expected_from = DatetimeFilterValue::DateString(expected_datetime, None);

        let mut dt = chrono_format::Parsed::new();
        chrono_format::parse(
            &mut dt,
            "2013-01-02",
            chrono_format::StrftimeItems::new("%Y-%m-%d"),
        )
        .unwrap();
        let expected_datetime = DateTime::from_utc(
            NaiveDateTime::new(dt.to_naive_date().unwrap(), NaiveTime::from_hms(0, 0, 0)),
            Utc,
        );
        let expected_to = DatetimeFilterValue::DateString(expected_datetime, None);

        assert_eq!(
            parsed.unwrap(),
            WhereClause {
                datetime_filter: Some(DatetimeFilter::In(
                    ColumnName("ts"),
                    expected_from,
                    expected_to
                )),

                metrics_filter: None,
            }
        );
    }

    #[test]
    fn parse_where_3() {
        let where_clause = r"where  ts in ('2012-12-30',2 hours)";
        let pairs = QueryGrammer::parse(Rule::WHERE_CLAUSE, where_clause);

        assert!(pairs.is_ok());
        let parsed = parse(pairs.unwrap().next().unwrap());

        assert!(parsed.is_ok());

        let mut dt = chrono_format::Parsed::new();
        chrono_format::parse(
            &mut dt,
            "2012-12-30",
            chrono_format::StrftimeItems::new("%Y-%m-%d"),
        )
        .unwrap();
        let expected_datetime = DateTime::from_utc(
            NaiveDateTime::new(dt.to_naive_date().unwrap(), NaiveTime::from_hms(0, 0, 0)),
            Utc,
        );
        let expected_from = DatetimeFilterValue::DateString(expected_datetime, None);

        let mut dt = chrono_format::Parsed::new();
        chrono_format::parse(
            &mut dt,
            "2012-12-30",
            chrono_format::StrftimeItems::new("%Y-%m-%d"),
        )
        .unwrap();
        let expected_datetime = DateTime::from_utc(
            NaiveDateTime::new(dt.to_naive_date().unwrap(), NaiveTime::from_hms(0, 0, 0)),
            Utc,
        );
        let expected_to = DatetimeFilterValue::DateString(
            expected_datetime,
            Some(DatetimeDelta::MicroSec(2 * 60 * 60 * 1_000_000)),
        );

        assert_eq!(
            parsed.unwrap(),
            WhereClause {
                datetime_filter: Some(DatetimeFilter::In(
                    ColumnName("ts"),
                    expected_from,
                    expected_to
                )),

                metrics_filter: None,
            }
        );
    }

    #[test]
    fn parse_where_4() {
        let where_clause = r"where  ts in (yesterday(),+ 9:00)";
        let pairs = QueryGrammer::parse(Rule::WHERE_CLAUSE, where_clause);

        assert!(pairs.is_ok());
        let parsed = parse(pairs.unwrap().next().unwrap());

        assert!(parsed.is_ok());

        let expected_from = DatetimeFilterValue::Function(BuildinDatetimeFunction::Yesterday, None);

        let expected_to = DatetimeFilterValue::Function(
            BuildinDatetimeFunction::Yesterday,
            Some(DatetimeDelta::FixedOffset(FixedOffset::east(9 * 60 * 60))),
        );

        assert_eq!(
            parsed.unwrap(),
            WhereClause {
                datetime_filter: Some(DatetimeFilter::In(
                    ColumnName("ts"),
                    expected_from,
                    expected_to
                )),
                metrics_filter: None,
            }
        );
    }

    #[test]
    fn parse_where_5() {
        let where_clause = r"where  ts >=|2 '2012-12-30'
            ";

        let pairs = QueryGrammer::parse(Rule::WHERE_CLAUSE, where_clause);

        assert!(pairs.is_ok());
        let parsed = parse(pairs.unwrap().next().unwrap());

        assert!(parsed.is_ok());

        let mut dt = chrono_format::Parsed::new();
        chrono_format::parse(
            &mut dt,
            "2012-12-30",
            chrono_format::StrftimeItems::new("%Y-%m-%d"),
        )
        .unwrap();

        let expected_datetime = DateTime::from_utc(
            NaiveDateTime::new(dt.to_naive_date().unwrap(), NaiveTime::from_hms(0, 0, 0)),
            Utc,
        );
        let expected = DatetimeFilterValue::DateString(expected_datetime, None);

        assert_eq!(
            parsed.unwrap(),
            WhereClause {
                datetime_filter: Some(DatetimeFilter::Gte(ColumnName("ts"), expected, Some(2))),

                metrics_filter: None,
            }
        );
    }

    #[test]
    fn parse_where_6() {
        let where_clause = r"where  ts <=|10 '2012-12-30'
            ";

        let pairs = QueryGrammer::parse(Rule::WHERE_CLAUSE, where_clause);

        assert!(pairs.is_ok());
        let parsed = parse(pairs.unwrap().next().unwrap());

        assert!(parsed.is_ok());

        let mut dt = chrono_format::Parsed::new();
        chrono_format::parse(
            &mut dt,
            "2012-12-30",
            chrono_format::StrftimeItems::new("%Y-%m-%d"),
        )
        .unwrap();

        let expected_datetime = DateTime::from_utc(
            NaiveDateTime::new(dt.to_naive_date().unwrap(), NaiveTime::from_hms(0, 0, 0)),
            Utc,
        );
        let expected = DatetimeFilterValue::DateString(expected_datetime, None);

        assert_eq!(
            parsed.unwrap(),
            WhereClause {
                datetime_filter: Some(DatetimeFilter::Lte(ColumnName("ts"), expected, Some(10))),

                metrics_filter: None,
            }
        );
    }

    #[test]
    fn parse_where_7() {
        let where_clause = r"where  ts <|10 '2012-12-30'
            ";

        let pairs = QueryGrammer::parse(Rule::WHERE_CLAUSE, where_clause);

        assert!(pairs.is_ok());
        let parsed = parse(pairs.unwrap().next().unwrap());

        assert!(parsed.is_ok());

        let mut dt = chrono_format::Parsed::new();
        chrono_format::parse(
            &mut dt,
            "2012-12-30",
            chrono_format::StrftimeItems::new("%Y-%m-%d"),
        )
        .unwrap();

        let expected_datetime = DateTime::from_utc(
            NaiveDateTime::new(dt.to_naive_date().unwrap(), NaiveTime::from_hms(0, 0, 0)),
            Utc,
        );
        let expected = DatetimeFilterValue::DateString(expected_datetime, None);

        assert_eq!(
            parsed.unwrap(),
            WhereClause {
                datetime_filter: Some(DatetimeFilter::Lt(ColumnName("ts"), expected, Some(10))),

                metrics_filter: None,
            }
        );
    }
}
