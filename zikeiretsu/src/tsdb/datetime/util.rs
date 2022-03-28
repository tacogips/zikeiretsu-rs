use chrono::{Date, Duration, FixedOffset, TimeZone, Utc};

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
    MicroSecond,
    MilliSecond,
    Second,
    Minute,
    Hour,
    Day,
}
