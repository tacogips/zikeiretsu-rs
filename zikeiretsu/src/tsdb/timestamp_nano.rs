use super::timestamp_sec::TimestampSec;

use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Deref, Sub};

pub const SEC_IN_NANOSEC: u64 = 1_000_000_000;
#[derive(PartialEq, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimestampNano(pub u64);
impl TimestampNano {
    pub fn new(inner: u64) -> Self {
        TimestampNano(inner)
    }

    pub fn now() -> Self {
        let timestamp = Utc::now().timestamp();

        debug_assert!(timestamp >= 0);
        let timestamp =
            (timestamp as u64 * SEC_IN_NANOSEC) + Utc::now().timestamp_subsec_nanos() as u64;

        Self::new(timestamp)
    }

    pub fn as_inner(&self) -> u64 {
        self.0
    }

    pub fn in_seconds(&self) -> u64 {
        self.0 / SEC_IN_NANOSEC
    }

    pub fn in_subsec_nano(&self) -> u32 {
        (self.0 % SEC_IN_NANOSEC) as u32
    }

    pub fn as_timestamp_sec(&self) -> TimestampSec {
        TimestampSec::new(self.in_seconds())
    }

    pub fn as_datetime(&self) -> DateTime<Utc> {
        let ndt = NaiveDateTime::from_timestamp(self.in_seconds() as i64, self.in_subsec_nano());
        DateTime::from_utc(ndt, Utc)
    }
}

impl<Tz: TimeZone> From<DateTime<Tz>> for TimestampNano {
    fn from(dt: DateTime<Tz>) -> Self {
        let v = dt.timestamp() as u64 * SEC_IN_NANOSEC + dt.timestamp_subsec_nanos() as u64;
        TimestampNano(v)
    }
}

impl fmt::Display for TimestampNano {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for TimestampNano {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Sub<TimestampNano> for TimestampNano {
    type Output = u64;
    fn sub(self, other: Self) -> Self::Output {
        *self - *other
    }
}

impl Sub<&TimestampNano> for &TimestampNano {
    type Output = u64;
    fn sub(self, other: &TimestampNano) -> Self::Output {
        **self - **other
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use chrono::DateTime;
    #[test]
    fn to_date_time() {
        let dt = DateTime::parse_from_rfc3339("2021-09-27T09:45:01.1749178Z").unwrap();
        let tsn: TimestampNano = dt.clone().into();
        let cdt = tsn.as_datetime();
        assert_eq!(cdt, dt);
    }
}
