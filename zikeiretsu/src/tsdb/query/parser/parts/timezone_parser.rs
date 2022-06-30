use super::clock_parser::time_sec_from_clock_str;
use crate::tsdb::query::parser::*;
use crate::tsdb::TimeZoneAndOffset;
use chrono::FixedOffset;
use once_cell::sync::OnceCell;
use pest::iterators::Pair;
use std::collections::HashMap;

// [+|-]01:00 => 0 as i32
fn timeoffset_sec_from_str(offset_str: &str) -> Result<i32> {
    let parsing_offset: &[u8] = offset_str.as_bytes();
    if offset_str == "00:00" {
        return Ok(0i32);
    }
    let is_nagative = match parsing_offset.first() {
        Some(b'+') => false,
        Some(b'-') => true,
        _ => return Err(ParserError::InvalidTimeOffset(offset_str.to_string())),
    };

    let sec = time_sec_from_clock_str(&offset_str[1..])?;

    if is_nagative {
        Ok(-sec)
    } else {
        Ok(sec)
    }
}

pub fn parse_timezone_name(pair: Pair<'_, Rule>) -> Result<&'static TimeZoneAndOffset> {
    if pair.as_rule() != Rule::TIMEZONE_NAME {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::TIMEZONE_NAME),
            format!("{:?}", pair.as_rule()),
        ));
    }

    timezone_zone().get(pair.as_str().trim()).map_or_else(
        || {
            Err(ParserError::InvalidTimeZone(
                pair.as_str().trim().to_string(),
            ))
        },
        Ok,
    )
}
static TIMEZONE_DEFS: OnceCell<HashMap<&'static str, TimeZoneAndOffset>> = OnceCell::new();

macro_rules! tz_abbrev_to_offset {
    ($(($abbrev:expr, $offset_str:expr)), *) => {{
        let mut m = HashMap::<&'static str, TimeZoneAndOffset>::new();
        $(
            m.insert($abbrev, TimeZoneAndOffset::new($abbrev.parse::<chrono_tz::Tz>().unwrap(), FixedOffset::east(timeoffset_sec_from_str($offset_str).unwrap())));
        )*
        m
    }};
}

fn timezone_zone() -> &'static HashMap<&'static str, TimeZoneAndOffset> {
    TIMEZONE_DEFS.get_or_init(|| {
        tz_abbrev_to_offset!(
            ("Etc/GMT+12", "-12:00"),
            ("Etc/GMT+11", "-11:00"),
            ("Pacific/Honolulu", "-10:00"),
            ("America/Anchorage", "-09:00"),
            ("America/Santa_Isabel", "-08:00"),
            ("America/Los_Angeles", "-08:00"),
            ("America/Chihuahua", "-07:00"),
            ("America/Phoenix", "-07:00"),
            ("America/Denver", "-07:00"),
            ("America/Guatemala", "-06:00"),
            ("America/Chicago", "-06:00"),
            ("America/Regina", "-06:00"),
            ("America/Mexico_City", "-06:00"),
            ("America/Bogota", "-05:00"),
            ("America/Indiana/Indianapolis", "-05:00"),
            ("America/New_York", "-05:00"),
            ("America/Caracas", "-04:30"),
            ("America/Halifax", "-04:00"),
            ("America/Asuncion", "-04:00"),
            ("America/La_Paz", "-04:00"),
            ("America/Cuiaba", "-04:00"),
            ("America/Santiago", "-04:00"),
            ("America/St_Johns", "-03:30"),
            ("America/Sao_Paulo", "-03:00"),
            ("America/Godthab", "-03:00"),
            ("America/Cayenne", "-03:00"),
            ("America/Argentina/Buenos_Aires", "-03:00"),
            ("America/Montevideo", "-03:00"),
            ("Etc/GMT+2", "-02:00"),
            ("Atlantic/Cape_Verde", "-01:00"),
            ("Atlantic/Azores", "-01:00"),
            ("Africa/Casablanca", "+00:00"),
            ("Atlantic/Reykjavik", "+00:00"),
            ("Europe/London", "+00:00"),
            ("Etc/GMT", "+00:00"),
            ("Europe/Berlin", "+01:00"),
            ("Europe/Paris", "+01:00"),
            ("Africa/Lagos", "+01:00"),
            ("Europe/Budapest", "+01:00"),
            ("Europe/Warsaw", "+01:00"),
            ("Africa/Windhoek", "+01:00"),
            ("Europe/Istanbul", "+02:00"),
            ("Europe/Kiev", "+02:00"),
            ("Africa/Cairo", "+02:00"),
            ("Asia/Damascus", "+02:00"),
            ("Asia/Amman", "+02:00"),
            ("Africa/Johannesburg", "+02:00"),
            ("Asia/Jerusalem", "+02:00"),
            ("Asia/Beirut", "+02:00"),
            ("Asia/Baghdad", "+03:00"),
            ("Europe/Minsk", "+03:00"),
            ("Asia/Riyadh", "+03:00"),
            ("Africa/Nairobi", "+03:00"),
            ("Asia/Tehran", "+03:30"),
            ("Europe/Moscow", "+04:00"),
            ("Asia/Tbilisi", "+04:00"),
            ("Asia/Yerevan", "+04:00"),
            ("Asia/Dubai", "+04:00"),
            ("Asia/Baku", "+04:00"),
            ("Indian/Mauritius", "+04:00"),
            ("Asia/Kabul", "+04:30"),
            ("Asia/Tashkent", "+05:00"),
            ("Asia/Karachi", "+05:00"),
            ("Asia/Colombo", "+05:30"),
            ("Asia/Kolkata", "+05:30"),
            ("Asia/Kathmandu", "+05:45"),
            ("Asia/Almaty", "+06:00"),
            ("Asia/Dhaka", "+06:00"),
            ("Asia/Yekaterinburg", "+06:00"),
            ("Asia/Yangon", "+06:30"),
            ("Asia/Bangkok", "+07:00"),
            ("Asia/Novosibirsk", "+07:00"),
            ("Asia/Krasnoyarsk", "+08:00"),
            ("Asia/Ulaanbaatar", "+08:00"),
            ("Asia/Shanghai", "+08:00"),
            ("Australia/Perth", "+08:00"),
            ("Asia/Singapore", "+08:00"),
            ("Asia/Taipei", "+08:00"),
            ("Asia/Irkutsk", "+09:00"),
            ("Asia/Seoul", "+09:00"),
            ("Asia/Tokyo", "+09:00"),
            ("Australia/Darwin", "+09:30"),
            ("Australia/Adelaide", "+09:30"),
            ("Australia/Hobart", "+10:00"),
            ("Asia/Yakutsk", "+10:00"),
            ("Australia/Brisbane", "+10:00"),
            ("Pacific/Port_Moresby", "+10:00"),
            ("Australia/Sydney", "+10:00"),
            ("Asia/Vladivostok", "+11:00"),
            ("Pacific/Guadalcanal", "+11:00"),
            ("Etc/GMT-12", "+12:00"),
            ("Pacific/Fiji", "+12:00"),
            ("Asia/Magadan", "+12:00"),
            ("Pacific/Auckland", "+12:00"),
            ("Pacific/Tongatapu", "+13:00"),
            ("Pacific/Apia", "+13:00")
        )
    })
}

#[cfg(test)]
mod test {

    use super::*;
    #[test]
    fn parse_timeoffset_sec_from_str_1() {
        let result = timeoffset_sec_from_str("+1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1 * 3600);

        let result = timeoffset_sec_from_str("-1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -1 * 3600);

        let result = timeoffset_sec_from_str("1");
        assert!(result.is_err());
    }

    #[test]
    fn parse_timeoffset_sec_from_str_2() {
        let result = timeoffset_sec_from_str("+2:00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2 * 3600);

        let result = timeoffset_sec_from_str("+12:00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 12 * 3600);

        let result = timeoffset_sec_from_str("+2:23");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2 * 3600 + 23 * 60);

        let result = timeoffset_sec_from_str("+02:00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2 * 3600);

        let result = timeoffset_sec_from_str("+02:23");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2 * 3600 + 23 * 60);

        let result = timeoffset_sec_from_str("-2:00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -2 * 3600);

        let result = timeoffset_sec_from_str("+2:00z");
        assert!(result.is_err());
    }

    #[test]
    fn parse_timeoffset_sec_from_str_3() {
        let result = timeoffset_sec_from_str("+2:00:12");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2 * 3600 + 12);

        let result = timeoffset_sec_from_str("+12:23:33");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 12 * 3600 + 23 * 60 + 33);

        let result = timeoffset_sec_from_str("-12:23:33");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -1 * (12 * 3600 + 23 * 60 + 33));

        let result = timeoffset_sec_from_str("+12:23:33z");
        assert!(result.is_err());
    }

    #[test]
    fn parse_timeoffset_sec_from_str_4() {
        let result = timeoffset_sec_from_str("-05:00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -5 * 3600);
    }
}
