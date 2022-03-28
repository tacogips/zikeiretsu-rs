use chrono::{Date, DateTime, Duration, TimeZone, Timelike, Utc};

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
