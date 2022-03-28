use chrono::{Date, Duration, FixedOffset, TimeZone, Utc};

pub fn today<Tz: TimeZone>(tz: Tz) -> Date<Tz> {
    Utc::today().with_timezone(&tz)
}

pub fn yesterday<Tz: TimeZone>(tz: Tz) -> Date<Tz> {
    Utc::today().with_timezone(&tz) - Duration::day(1)
}
