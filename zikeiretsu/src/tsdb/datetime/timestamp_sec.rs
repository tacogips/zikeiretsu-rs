use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Deref, Sub};

#[derive(PartialEq, Debug, Copy, Clone, Eq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TimestampSec(pub u64);
impl TimestampSec {
    pub fn new(inner: u64) -> Self {
        TimestampSec(inner)
    }

    pub fn now() -> Self {
        let timestamp = Utc::now().timestamp();
        debug_assert!(timestamp >= 0);

        Self::new(timestamp as u64)
    }
}

impl fmt::Display for TimestampSec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{timestamp}", timestamp = self.0)
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

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_display() {
        let ts = TimestampSec::new(1638257405);

        assert_eq!("1638257405", format!("{ts}"))
    }
}
