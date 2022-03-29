use super::clock_parser::time_sec_from_clock_str;
use crate::tsdb::query::parser::*;
use chrono::FixedOffset;
use pest::{iterators::Pair, ParserState};

pub fn parse_timezone_offset<'q>(pair: Pair<'q, Rule>) -> Result<FixedOffset> {
    if pair.as_rule() != Rule::TIMEZONE_OFFSET_VAL {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::TIMEZONE_OFFSET_VAL),
            format!("{:?}", pair.as_rule()),
        ));
    }
    let offset_sec = timeoffset_sec_from_str(pair.as_str())?;
    Ok(FixedOffset::east(offset_sec))
}

// [+|-]01:00 => 0 as i32
fn timeoffset_sec_from_str(offfset_str: &str) -> Result<i32> {
    let parsing_offset: &[u8] = &offfset_str.as_bytes();
    let is_nagative = match parsing_offset.first() {
        Some(b'+') => false,
        Some(b'-') => true,
        _ => return Err(QueryError::InvalidTimeOffset(offfset_str.to_string())),
    };
    let sec = time_sec_from_clock_str(&offfset_str[1..])?;

    if is_nagative {
        Ok(sec * -1)
    } else {
        Ok(sec)
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
