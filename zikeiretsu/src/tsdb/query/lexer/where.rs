use crate::tsdb::datapoint::DatapointSearchCondition;
use crate::tsdb::datetime::DatetimeAccuracy;
use crate::tsdb::query::parser::clause::WhereClause;
use crate::tsdb::query::parser::DatetimeFilter;
use chrono::{Duration, FixedOffset};

use super::Result as LexerResult;

pub(crate) fn interpret_datatime_search_condition<'q>(
    timezone: &FixedOffset,
    where_clause: Option<&WhereClause<'q>>,
) -> LexerResult<DatapointSearchCondition> {
    match where_clause {
        None => Ok(DatapointSearchCondition::all()),
        Some(where_clause) => match &where_clause.datetime_filter {
            None => Ok(DatapointSearchCondition::all()),
            Some(datetime_filter) => datetime_filter_to_condition(timezone, &datetime_filter),
        },
    }
}

fn datetime_filter_to_condition<'q>(
    timezone: &FixedOffset,
    datetime_filter: &DatetimeFilter<'q>,
) -> LexerResult<DatapointSearchCondition> {
    match &datetime_filter {
        DatetimeFilter::In(_, from, to) => Ok(DatapointSearchCondition::new(
            Some(from.to_timestamp_nano(&timezone)),
            Some(to.to_timestamp_nano(&timezone)),
        )),
        DatetimeFilter::Gte(_, from) => Ok(DatapointSearchCondition::new(
            Some(from.to_timestamp_nano(&timezone)),
            None,
        )),
        DatetimeFilter::Gt(_, from) => Ok(DatapointSearchCondition::new(
            Some(from.to_timestamp_nano(&timezone) + Duration::nanoseconds(1)),
            None,
        )),
        DatetimeFilter::Lte(_, to) => Ok(DatapointSearchCondition::new(
            None,
            Some(to.to_timestamp_nano(&timezone) + Duration::nanoseconds(1)),
        )),
        DatetimeFilter::Lt(_, to) => Ok(DatapointSearchCondition::new(
            None,
            Some(to.to_timestamp_nano(&timezone)),
        )),
        DatetimeFilter::Equal(_, datetime_value) => {
            let from_dt_nano = datetime_value.to_timestamp_nano(&timezone);
            let from_dt = from_dt_nano.as_datetime_with_tz(timezone);
            let until_date_offset = match DatetimeAccuracy::from_datetime(from_dt) {
                DatetimeAccuracy::NanoSecond => Duration::nanoseconds(1),
                DatetimeAccuracy::MicroSecond => Duration::microseconds(1),
                DatetimeAccuracy::MilliSecond => Duration::milliseconds(1),
                DatetimeAccuracy::Second => Duration::seconds(1),
                DatetimeAccuracy::Minute => Duration::minutes(1),
                DatetimeAccuracy::Hour => Duration::hours(1),
                DatetimeAccuracy::Day => Duration::days(1),
            };

            Ok(DatapointSearchCondition::new(
                Some(from_dt_nano),
                Some((from_dt + until_date_offset).into()),
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tsdb::datetime::*;
    use crate::tsdb::parser::*;

    fn jst() -> FixedOffset {
        FixedOffset::east(9 * 3600)
    }

    #[test]
    fn lexer_datetime_eq_1() {
        let dt = parse_datetime_str("'2021-09-27'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst(), &filter).unwrap();

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointSearchCondition::new(
                Some(expected_from),
                Some((expected_from + Duration::days(1)).into()),
            ),
            filter_cond
        );
    }

    #[test]
    fn lexer_datetime_eq_2() {
        let dt = parse_datetime_str("'2021-09-27 23:00'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst(), &filter).unwrap();

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointSearchCondition::new(
                Some(expected_from),
                Some((expected_from + Duration::hours(1)).into()),
            ),
            filter_cond
        );
    }

    #[test]
    fn lexer_datetime_eq_3() {
        let dt = parse_datetime_str("'2021-09-27 23:10'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst(), &filter).unwrap();

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointSearchCondition::new(
                Some(expected_from),
                Some((expected_from + Duration::minutes(1)).into()),
            ),
            filter_cond
        );
    }

    #[test]
    fn lexer_datetime_eq_4() {
        let dt = parse_datetime_str("'2021-09-27 23:00:01'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst(), &filter).unwrap();

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointSearchCondition::new(
                Some(expected_from),
                Some((expected_from + Duration::seconds(1)).into()),
            ),
            filter_cond
        );
    }

    #[test]
    fn lexer_datetime_in_1() {
        let filter = DatetimeFilter::In(
            ColumnName("ts"),
            DatetimeFilterValue::Function(BuildinDatetimeFunction::Yesterday, None),
            DatetimeFilterValue::Function(BuildinDatetimeFunction::Yesterday, None),
        );

        let filter_cond = datetime_filter_to_condition(&jst(), &filter);

        assert!(filter_cond.is_ok())
    }

    #[test]
    fn lexer_datetime_in_2() {
        let filter = DatetimeFilter::In(
            ColumnName("ts"),
            DatetimeFilterValue::Function(
                BuildinDatetimeFunction::Yesterday,
                Some(DatetimeDelta::FixedOffset(FixedOffset::east(-9 * 60 * 60))),
            ),
            DatetimeFilterValue::Function(BuildinDatetimeFunction::Yesterday, None),
        );

        let filter_cond = datetime_filter_to_condition(&jst(), &filter);

        assert!(filter_cond.is_ok())
    }
}
