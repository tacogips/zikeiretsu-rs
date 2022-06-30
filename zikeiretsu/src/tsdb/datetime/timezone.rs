use chrono::FixedOffset;
use once_cell::sync::Lazy;

pub static DEFAULT_TIMEZONE_AND_OFFSET: Lazy<TimeZoneAndOffset> =
    Lazy::new(TimeZoneAndOffset::default);

#[derive(Debug, PartialEq)]
pub struct TimeZoneAndOffset {
    pub tz: chrono_tz::Tz,
    pub offset: FixedOffset,
}
impl TimeZoneAndOffset {
    pub fn new(tz: chrono_tz::Tz, offset: FixedOffset) -> Self {
        Self { tz, offset }
    }
}

impl Default for TimeZoneAndOffset {
    fn default() -> Self {
        Self {
            tz: chrono_tz::UTC,
            offset: FixedOffset::west(0),
        }
    }
}
