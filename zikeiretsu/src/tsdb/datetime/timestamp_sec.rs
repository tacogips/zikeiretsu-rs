use super::TimestampNano;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Deref, Sub};

#[derive(PartialEq, Debug, Copy, Clone, Eq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct TimestampSec(pub u64);
impl TimestampSec {
    pub fn new(inner: u64) -> Self {
        TimestampSec(inner)
    }

    pub fn as_i64(&self) -> i64 {
        self.0 as i64
    }

    pub fn now() -> Self {
        let timestamp = Utc::now().timestamp();
        debug_assert!(timestamp >= 0);

        Self::new(timestamp as u64)
    }

    pub fn as_formated_datetime<Tz: TimeZone>(&self, tz: Option<&Tz>) -> String
    where
        Tz::Offset: std::fmt::Display,
    {
        match tz {
            Some(tz) => self
                .as_datetime_with_tz(tz)
                .to_rfc3339_opts(SecondsFormat::Secs, true),

            None => self
                .as_datetime()
                .to_rfc3339_opts(SecondsFormat::Secs, true),
        }
    }

    pub fn as_timestamp_nano(&self) -> TimestampNano {
        TimestampNano::new(self.0 * 1_000_000_000)
    }

    pub fn as_datetime(&self) -> DateTime<Utc> {
        let ndt = NaiveDateTime::from_timestamp(self.0 as i64, 0);
        DateTime::from_utc(ndt, Utc)
    }

    pub fn as_datetime_with_tz<Tz: TimeZone>(&self, tz: &Tz) -> DateTime<Tz> {
        self.as_datetime().with_timezone(tz)
    }

    pub fn into_datetime_with_tz<Tz: TimeZone>(self, tz: &Tz) -> DateTime<Tz> {
        self.as_datetime().with_timezone(tz)
    }

    pub fn zero() -> Self {
        Self::new(0)
    }

    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl<Tz: TimeZone> From<DateTime<Tz>> for TimestampSec {
    fn from(dt: DateTime<Tz>) -> Self {
        let v = dt.timestamp() as u64;
        TimestampSec(v)
    }
}

impl Deref for TimestampSec {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Sub<TimestampSec> for TimestampSec {
    type Output = u64;
    fn sub(self, other: Self) -> Self::Output {
        *self - *other
    }
}

impl Sub<&TimestampSec> for &TimestampSec {
    type Output = u64;
    fn sub(self, other: &TimestampSec) -> Self::Output {
        **self - **other
    }
}

impl Add<u64> for &TimestampSec {
    type Output = TimestampSec;
    fn add(self, other: u64) -> Self::Output {
        TimestampSec::new(**self + other)
    }
}

impl Add<u64> for TimestampSec {
    type Output = TimestampSec;
    fn add(self, other: u64) -> Self::Output {
        TimestampSec::new(*self + other)
    }
}

impl fmt::Display for TimestampSec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{nano} ({formated_date})",
            nano = self.0,
            formated_date = self.as_datetime().to_rfc3339()
        )
    }
}
