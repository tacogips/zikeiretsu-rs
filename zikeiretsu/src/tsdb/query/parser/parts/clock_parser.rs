use crate::tsdb::query::parser::*;
use chrono::{FixedOffset, TimeZone};
use pest::{error::Error as PestError, iterators::Pair, Parser, ParserState};
use pest_derive::Parser;
use thiserror::Error;

// 00:00 => 0 as i32
pub(crate) fn time_sec_from_clock_str(clock_str: &str) -> Result<i32> {
    let mut parsing_offset: &[u8] = &clock_str.as_bytes();
    //parse hours
    let hour_num = if parsing_offset.is_empty() {
        return Err(QueryError::InvalidTimeOffset(clock_str.to_string()));
    } else if parsing_offset.len() > 2 {
        match (parsing_offset[0], parsing_offset[1]) {
            (hour_10 @ b'0'..=b'9', hour_1 @ b'0'..=b'9') => {
                parsing_offset = &parsing_offset[2..];
                i32::from(hour_10 - b'0') * 10 + i32::from(hour_1 - b'0')
            }

            (hour @ b'0'..=b'9', b':') => {
                parsing_offset = &parsing_offset[1..];
                i32::from(hour - b'0')
            }
            _ => return Err(QueryError::InvalidTimeOffset(clock_str.to_string())),
        }
    } else {
        match parsing_offset[0] {
            hour @ b'0'..=b'9' => {
                parsing_offset = &parsing_offset[1..];
                i32::from(hour - b'0')
            }
            _ => return Err(QueryError::InvalidTimeOffset(clock_str.to_string())),
        }
    };

    //parse minutes
    let minute_num = if let Some(&b':') = &parsing_offset.first() {
        parsing_offset = &parsing_offset[1..];
        if parsing_offset.len() < 2 {
            return Err(QueryError::InvalidTimeOffset(clock_str.to_string()));
        }
        match (parsing_offset[0], parsing_offset[1]) {
            (minute_10 @ b'0'..=b'5', minute_1 @ b'0'..=b'9') => {
                parsing_offset = &parsing_offset[2..];
                i32::from(minute_10 - b'0') * 10 + i32::from(minute_1 - b'0')
            }
            _ => return Err(QueryError::InvalidTimeOffset(clock_str.to_string())),
        }
    } else {
        0
    };

    //parse minutes
    let sec_num = if let Some(&b':') = &parsing_offset.first() {
        parsing_offset = &parsing_offset[1..];

        if parsing_offset.len() < 2 {
            return Err(QueryError::InvalidTimeOffset(clock_str.to_string()));
        }
        match (parsing_offset[0], parsing_offset[1]) {
            (secs_10 @ b'0'..=b'5', secs_1 @ b'0'..=b'9') => {
                parsing_offset = &parsing_offset[2..];
                i32::from(secs_10 - b'0') * 10 + i32::from(secs_1 - b'0')
            }
            _ => return Err(QueryError::InvalidTimeOffset(clock_str.to_string())),
        }
    } else {
        0
    };
    if !parsing_offset.is_empty() {
        return Err(QueryError::InvalidTimeOffset(clock_str.to_string()));
    }

    Ok(hour_num * 3600 + minute_num * 60 + sec_num)
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
}
