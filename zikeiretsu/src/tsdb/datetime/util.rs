use chrono::{
    format as chrono_format, Date, DateTime, Duration, NaiveDateTime, NaiveTime, TimeZone,
    Timelike, Utc,
};

pub fn today<Tz: TimeZone>(tz: Tz) -> Date<Tz> {
    Utc::today().with_timezone(&tz)
}

pub fn yesterday<Tz: TimeZone>(tz: Tz) -> Date<Tz> {
    Utc::today().with_timezone(&tz) - Duration::days(1)
}

pub fn tomorrow<Tz: TimeZone>(tz: Tz) -> Date<Tz> {
    Utc::today().with_timezone(&tz) + Duration::days(1)
}

pub enum DatetimeAccuracy {
    NanoSecond,
    MicroSecond,
    MilliSecond,
    Second,
    Minute,
    Hour,
    Day,
}

impl DatetimeAccuracy {
    pub fn from_datetime<Tz: TimeZone>(dt: DateTime<Tz>) -> Self {
        let nano_sec = dt.nanosecond();
        if nano_sec == 0 {
            match ((dt.hour(), dt.minute(), dt.second())) {
                (0, 0, 0) => DatetimeAccuracy::Day,
                (_, 0, 0) => DatetimeAccuracy::Hour,
                (_, _, 0) => DatetimeAccuracy::Minute,
                _ => DatetimeAccuracy::Second,
            }
        } else {
            if nano_sec % 1_000 != 0 {
                DatetimeAccuracy::MicroSecond
            } else if nano_sec % 1_000_000 != 0 {
                DatetimeAccuracy::MilliSecond
            } else {
                DatetimeAccuracy::NanoSecond
            }
        }
    }

    /////// Returns the number of nanoseconds since the whole non-leap second.
    /////// The range from 1,000,000,000 to 1,999,999,999 represents
    /////// the [leap second](./naive/struct.NaiveTime.html#leap-second-handling).
    //fn nanosecond(&self) -> u32;

    //    dt.hour()
    //    let naive_local_datetime = dt.naive_local();
    //    naive_local_datetime.hour();
    //    unimplemented!()
    //}
}

/// availabe formats
/// 'yyyy-MM-DD hh:mm:ss.ZZZZZZ'
/// 'yyyy-MM-DD hh:mm:ss'
/// 'yyyy-MM-DD hh:mm'
/// 'yyyy-MM-DD'
pub(crate) fn parse_datetime_str(datetime_str: &str) -> Result<DateTime<Utc>> {
    if datetime_str.len() < 2 {
        return Err(QueryError::InvalidDatetimeFormat(datetime_str.to_string()));
    }
    if !datetime_str.starts_with("'") || !datetime_str.ends_with("'") {
        return Err(QueryError::InvalidDatetimeFormat(datetime_str.to_string()));
    }

    //strip single quotes
    let datetime_str: &str = &datetime_str[1..][..datetime_str.len() - 2];
    for (each_format, is_naive_date) in datetime_formats() {
        let mut parsed = chrono_format::Parsed::new();

        if let Ok(_) = chrono_format::parse(&mut parsed, datetime_str, each_format.clone()) {
            if *is_naive_date {
                let naive = parsed.to_naive_date()?;
                let naive = NaiveDateTime::new(naive, NaiveTime::from_hms(0, 0, 0));
                return Ok(DateTime::from_utc(naive, Utc));
            } else {
                let parsed = parsed.to_datetime_with_timezone(&Utc)?;

                return Ok(parsed);
            }
        }
    }

    Err(QueryError::InvalidDatetimeFormat(datetime_str.to_string()))
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn parse_datetetime_test() {
        let parse_result = parse_datetime_str("'2019-12-13 23:33:12'");
        assert!(parse_result.is_ok());

        let parse_result = parse_datetime_str("'2019-12-13 23:33:12.023'");
        assert!(parse_result.is_ok());

        let parse_result = parse_datetime_str("'2019-12-13 23:33'");
        assert!(parse_result.is_ok());

        let parse_result = parse_datetime_str("'2019-12-13'");
        assert!(parse_result.is_ok());

        let parse_result = parse_datetime_str("'2019-12-13");
        assert!(parse_result.is_err());
    }
}
