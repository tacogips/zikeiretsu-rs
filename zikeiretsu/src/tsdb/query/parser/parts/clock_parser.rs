use crate::tsdb::query::parser::*;

use super::is_space;
use chrono::FixedOffset;
use pest::iterators::Pair;

pub fn parse_clock_delta<'q>(pair: Pair<'q, Rule>) -> Result<FixedOffset> {
    if pair.as_rule() != Rule::CLOCK_DELTA {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::CLOCK_DELTA),
            format!("{:?}", pair.as_rule()),
        ));
    }
    let offset_sec = clock_delta_sec_from_str(pair.as_str())?;
    Ok(FixedOffset::east(offset_sec))
}

// [+|-]01:00 => 0 as i32
fn clock_delta_sec_from_str(clock_delta_str: &str) -> Result<i32> {
    let parsing_offset: &[u8] = &clock_delta_str.as_bytes();
    let is_nagative = match parsing_offset.first() {
        Some(b'+') => false,
        Some(b'-') => true,
        _ => return Err(ParserError::InvalidTimeOffset(clock_delta_str.to_string())),
    };

    let mut white_space_count = 0;
    for i in 1..parsing_offset.len() {
        if !is_space(parsing_offset[i]) {
            break;
        }
        white_space_count += 1;
    }

    let sec = time_sec_from_clock_str(&clock_delta_str[white_space_count + 1..])?;

    if is_nagative {
        Ok(-sec)
    } else {
        Ok(sec)
    }
}

// 00:00 => 0 as i32
pub(crate) fn time_sec_from_clock_str(clock_str: &str) -> Result<i32> {
    let mut parsing_offset: &[u8] = clock_str.as_bytes();
    //parse hours
    let hour_num = if parsing_offset.is_empty() {
        return Err(ParserError::InvalidTimeOffset(clock_str.to_string()));
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
            _ => return Err(ParserError::InvalidTimeOffset(clock_str.to_string())),
        }
    } else {
        match parsing_offset[0] {
            hour @ b'0'..=b'9' => {
                parsing_offset = &parsing_offset[1..];
                i32::from(hour - b'0')
            }
            _ => return Err(ParserError::InvalidTimeOffset(clock_str.to_string())),
        }
    };

    //parse minutes
    let minute_num = if let Some(&b':') = &parsing_offset.first() {
        parsing_offset = &parsing_offset[1..];
        if parsing_offset.len() < 2 {
            return Err(ParserError::InvalidTimeOffset(clock_str.to_string()));
        }
        match (parsing_offset[0], parsing_offset[1]) {
            (minute_10 @ b'0'..=b'5', minute_1 @ b'0'..=b'9') => {
                parsing_offset = &parsing_offset[2..];
                i32::from(minute_10 - b'0') * 10 + i32::from(minute_1 - b'0')
            }
            _ => return Err(ParserError::InvalidTimeOffset(clock_str.to_string())),
        }
    } else {
        0
    };

    //parse minutes
    let sec_num = if let Some(&b':') = &parsing_offset.first() {
        parsing_offset = &parsing_offset[1..];

        if parsing_offset.len() < 2 {
            return Err(ParserError::InvalidTimeOffset(clock_str.to_string()));
        }
        match (parsing_offset[0], parsing_offset[1]) {
            (secs_10 @ b'0'..=b'5', secs_1 @ b'0'..=b'9') => {
                parsing_offset = &parsing_offset[2..];
                i32::from(secs_10 - b'0') * 10 + i32::from(secs_1 - b'0')
            }
            _ => return Err(ParserError::InvalidTimeOffset(clock_str.to_string())),
        }
    } else {
        0
    };
    if !parsing_offset.is_empty() {
        return Err(ParserError::InvalidTimeOffset(clock_str.to_string()));
    }

    Ok(hour_num * 3600 + minute_num * 60 + sec_num)
}

#[cfg(test)]
mod test {

    use super::*;
    #[test]
    fn parse_clock_delta_sec_from_str_1() {
        let result = clock_delta_sec_from_str("+1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1 * 3600);

        let result = clock_delta_sec_from_str("-1");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -1 * 3600);

        let result = clock_delta_sec_from_str("1");
        assert!(result.is_err());
    }

    #[test]
    fn parse_clock_delta_sec_from_str_2() {
        let result = clock_delta_sec_from_str("+2:00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2 * 3600);

        let result = clock_delta_sec_from_str("+  12:00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 12 * 3600);

        let result = clock_delta_sec_from_str("+2:23");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2 * 3600 + 23 * 60);

        let result = clock_delta_sec_from_str("+02:00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2 * 3600);

        let result = clock_delta_sec_from_str("+02:23");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2 * 3600 + 23 * 60);

        let result = clock_delta_sec_from_str("-        2:00");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -2 * 3600);

        let result = clock_delta_sec_from_str("+2:00z");
        assert!(result.is_err());
    }
}
