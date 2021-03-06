use crate::tsdb::datapoint::{DatapointsRange, DatapointsSearchCondition, SearchDatapointsLimit};
use crate::tsdb::datetime::DatetimeAccuracy;
use crate::tsdb::query::parser::clause::WhereClause;
use crate::tsdb::query::parser::DatetimeFilter;
use chrono::{Duration, FixedOffset};

use super::Result as LexerResult;

pub(crate) fn interpret_datatime_search_condition<'q>(
    timezone: &FixedOffset,
    where_clause: &WhereClause<'q>,
) -> LexerResult<DatapointsSearchCondition> {
    match &where_clause.datetime_filter {
        None => Ok(DatapointsSearchCondition::all()),
        Some(datetime_filter) => datetime_filter_to_condition(timezone, datetime_filter),
    }
}

fn datetime_filter_to_condition<'q>(
    offset: &FixedOffset,
    datetime_filter: &DatetimeFilter<'q>,
) -> LexerResult<DatapointsSearchCondition> {
    match &datetime_filter {
        DatetimeFilter::In(_, from, to) => Ok(DatapointsSearchCondition {
            datapoints_range: DatapointsRange::new(
                Some(from.to_timestamp_nano(offset)),
                Some(to.to_timestamp_nano(offset)),
            ),
            limit: None,
        }),

        DatetimeFilter::Gte(_, from, limit) => Ok(DatapointsSearchCondition {
            datapoints_range: DatapointsRange::new(Some(from.to_timestamp_nano(offset)), None),
            limit: limit.map(SearchDatapointsLimit::Head),
        }),
        DatetimeFilter::Gt(_, from, limit) => Ok(DatapointsSearchCondition {
            datapoints_range: DatapointsRange::new(
                Some(from.to_timestamp_nano(offset) + Duration::nanoseconds(1)),
                None,
            ),
            limit: limit.map(SearchDatapointsLimit::Head),
        }),
        DatetimeFilter::Lte(_, to, limit) => Ok(DatapointsSearchCondition {
            datapoints_range: DatapointsRange::new(
                None,
                Some(to.to_timestamp_nano(offset) + Duration::nanoseconds(1)),
            ),
            limit: limit.map(SearchDatapointsLimit::Tail),
        }),
        DatetimeFilter::Lt(_, to, limit) => Ok(DatapointsSearchCondition {
            datapoints_range: DatapointsRange::new(None, Some(to.to_timestamp_nano(offset))),
            limit: limit.map(SearchDatapointsLimit::Tail),
        }),
        DatetimeFilter::Equal(_, datetime_value) => {
            let from_dt_nano = datetime_value.to_timestamp_nano(offset);
            let from_dt = from_dt_nano.as_datetime_with_tz(offset);
            let until_date_offset = match DatetimeAccuracy::from_datetime(from_dt) {
                DatetimeAccuracy::NanoSecond => Duration::nanoseconds(1),
                DatetimeAccuracy::MicroSecond => Duration::microseconds(1),
                DatetimeAccuracy::MilliSecond => Duration::milliseconds(1),
                DatetimeAccuracy::Second => Duration::seconds(1),
                DatetimeAccuracy::Minute => Duration::minutes(1),
                DatetimeAccuracy::Hour => Duration::hours(1),
                DatetimeAccuracy::Day => Duration::days(1),
            };

            Ok(DatapointsSearchCondition {
                datapoints_range: DatapointsRange::new(
                    Some(from_dt_nano),
                    Some((from_dt + until_date_offset).into()),
                ),

                limit: None,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tsdb::datetime::*;
    use crate::tsdb::parser::*;
    use chrono::FixedOffset;

    fn jst_offset() -> FixedOffset {
        FixedOffset::east(9 * 3600)
    }

    #[test]
    fn lexer_datetime_eq_1() {
        let dt = parse_datetime_str("'2021-09-27'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst_offset(), &filter).unwrap();
        let (datapoints_range, limit) = (filter_cond.datapoints_range, filter_cond.limit);

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointsRange::new(
                Some(expected_from),
                Some((expected_from + Duration::days(1)).into()),
            ),
            datapoints_range
        );

        assert!(limit.is_none());
    }

    #[test]
    fn lexer_datetime_eq_2() {
        let dt = parse_datetime_str("'2021-09-27 23:00'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst_offset(), &filter).unwrap();
        let (date_range, limit) = (filter_cond.datapoints_range, filter_cond.limit);

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointsRange::new(
                Some(expected_from),
                Some((expected_from + Duration::hours(1)).into()),
            ),
            date_range
        );

        assert!(limit.is_none());
    }

    #[test]
    fn lexer_datetime_eq_3() {
        let dt = parse_datetime_str("'2021-09-27 23:10'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst_offset(), &filter).unwrap();
        let (date_range, limit) = (filter_cond.datapoints_range, filter_cond.limit);

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointsRange::new(
                Some(expected_from),
                Some((expected_from + Duration::minutes(1)).into()),
            ),
            date_range
        );

        assert!(limit.is_none());
    }

    #[test]
    fn lexer_datetime_eq_4() {
        let dt = parse_datetime_str("'2021-09-27 23:00:01'").unwrap();
        let filter_value = DatetimeFilterValue::DateString(dt.clone(), None);
        let col = "ts";
        let filter = DatetimeFilter::Equal(ColumnName(col), filter_value);
        let filter_cond = datetime_filter_to_condition(&jst_offset(), &filter).unwrap();
        let (date_range, limit) = (filter_cond.datapoints_range, filter_cond.limit);

        let expected_from: TimestampNano = (dt - Duration::hours(9)).into();
        assert_eq!(
            DatapointsRange::new(
                Some(expected_from),
                Some((expected_from + Duration::seconds(1)).into()),
            ),
            date_range
        );

        assert!(limit.is_none());
    }

    #[test]
    fn lexer_datetime_in_1() {
        let filter = DatetimeFilter::In(
            ColumnName("ts"),
            DatetimeFilterValue::Function(BuildinDatetimeFunction::Yesterday, None),
            DatetimeFilterValue::Function(BuildinDatetimeFunction::Yesterday, None),
        );

        let filter_cond = datetime_filter_to_condition(&jst_offset(), &filter);

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

        let filter_cond = datetime_filter_to_condition(&jst_offset(), &filter);

        assert!(filter_cond.is_ok())
    }
}
