use super::clock_parser::time_sec_from_clock_str;
use crate::tsdb::query::parser::*;
use chrono::FixedOffset;
use once_cell::sync::OnceCell;
use pest::iterators::Pair;
use std::collections::HashMap;

pub fn parse_timezone_offset<'q>(pair: Pair<'q, Rule>) -> Result<FixedOffset> {
    if pair.as_rule() != Rule::TIMEZONE_OFFSET_VAL {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::TIMEZONE_OFFSET_VAL),
            format!("{:?}", pair.as_rule()),
        ));
    }
    let offset_sec = timeoffset_sec_from_str(pair.as_str())?;
    Ok(FixedOffset::east(offset_sec))
}

// [+|-]01:00 => 0 as i32
fn timeoffset_sec_from_str(offset_str: &str) -> Result<i32> {
    let parsing_offset: &[u8] = &offset_str.as_bytes();
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

pub fn parse_timezone_name<'q>(pair: Pair<'q, Rule>) -> Result<FixedOffset> {
    if pair.as_rule() != Rule::TIMEZONE_NAME {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::TIMEZONE_NAME),
            format!("{:?}", pair.as_rule()),
        ));
    }

    timezone_abbevs()
        .get(pair.as_str().trim().to_uppercase().as_str())
        .map_or_else(
            || Err(ParserError::InvalidTimeZoneAbberv(pair.to_string())),
            |fixed_offset| Ok(*fixed_offset),
        )
}
static TIMEZONE_ABBREVS: OnceCell<HashMap<&'static str, FixedOffset>> = OnceCell::new();

macro_rules! tz_abbrev_to_offset {
    ($(($abbrev:expr, $offset_str:expr)), *) => {{
        let mut m = HashMap::<&'static str,FixedOffset>::new();
        $(
            m.insert($abbrev, FixedOffset::east(timeoffset_sec_from_str($offset_str).unwrap()));
        )*
        m
    }};
}

fn timezone_abbevs() -> &'static HashMap<&'static str, FixedOffset> {
    TIMEZONE_ABBREVS.get_or_init(|| {
        tz_abbrev_to_offset!(
            ("ACDT", "+10:30"),
            ("ACST", "+09:30"),
            ("ACT", "-05:00"),
            ("ACT", "+08:00"),
            ("ACWST", "+08:45"),
            ("ADT", "-03:00"),
            ("AEDT", "+11:00"),
            ("AEST", "+10:00"),
            ("AET", "+10:00"),
            ("AFT", "+04:30"),
            ("AKDT", "-08:00"),
            ("AKST", "-09:00"),
            ("ALMT", "+06:00"),
            ("AMST", "-03:00"),
            ("AMT", "-04:00"),
            ("AMT", "+04:00"),
            ("ANAT", "+12:00"),
            ("AQTT", "+05:00"),
            ("ART", "-03:00"),
            ("AST", "+03:00"),
            ("AST", "-04:00"),
            ("AWST", "+08:00"),
            ("AZOST", "00:00"),
            ("AZOT", "-01:00"),
            ("AZT", "+04:00"),
            ("BNT", "+08:00"),
            ("BIOT", "+06:00"),
            ("BIT", "-12:00"),
            ("BOT", "-04:00"),
            ("BRST", "-02:00"),
            ("BRT", "-03:00"),
            ("BST", "+06:00"),
            ("BST", "+11:00"),
            ("BST", "+01:00"),
            ("BTT", "+06:00"),
            ("CAT", "+02:00"),
            ("CCT", "+06:30"),
            ("CDT", "-05:00"),
            ("CDT", "-04:00"),
            ("CEST", "+02:00"),
            ("CET", "+01:00"),
            ("CHADT", "+13:45"),
            ("CHAST", "+12:45"),
            ("CHOT", "+08:00"),
            ("CHOST", "+09:00"),
            ("CHST", "+10:00"),
            ("CHUT", "+10:00"),
            ("CIST", "-08:00"),
            ("CKT", "-10:00"),
            ("CLST", "-03:00"),
            ("CLT", "-04:00"),
            ("COST", "-04:00"),
            ("COT", "-05:00"),
            ("CST", "-06:00"),
            ("CST", "+08:00"),
            ("CST", "-05:00"),
            ("CT", "-06:00"),
            ("CVT", "-01:00"),
            ("CWST", "+08:45"),
            ("CXT", "+07:00"),
            ("DAVT", "+07:00"),
            ("DDUT", "+10:00"),
            ("DFT", "+01:00"),
            ("EASST", "-05:00"),
            ("EAST", "-06:00"),
            ("EAT", "+03:00"),
            ("ECT", "-04:00"),
            ("ECT", "-05:00"),
            ("EDT", "-04:00"),
            ("EEST", "+03:00"),
            ("EET", "+02:00"),
            ("EGST", "00:00"),
            ("EGT", "-01:00"),
            ("EST", "-05:00"),
            ("ET", "-05:00"),
            ("FET", "+03:00"),
            ("FJT", "+12:00"),
            ("FKST", "-03:00"),
            ("FKT", "-04:00"),
            ("FNT", "-02:00"),
            ("GALT", "-06:00"),
            ("GAMT", "-09:00"),
            ("GET", "+04:00"),
            ("GFT", "-03:00"),
            ("GILT", "+12:00"),
            ("GIT", "-09:00"),
            ("GMT", "00:00"),
            ("GST", "-02:00"),
            ("GST", "+04:00"),
            ("GYT", "-04:00"),
            ("HDT", "-09:00"),
            ("HAEC", "+02:00"),
            ("HST", "-10:00"),
            ("HKT", "+08:00"),
            ("HMT", "+05:00"),
            ("HOVST", "+08:00"),
            ("HOVT", "+07:00"),
            ("ICT", "+07:00"),
            ("IDLW", "-12:00"),
            ("IDT", "+03:00"),
            ("IOT", "+03:00"),
            ("IRDT", "+04:30"),
            ("IRKT", "+08:00"),
            ("IRST", "+03:30"),
            ("IST", "+05:30"),
            ("IST", "+01:00"),
            ("IST", "+02:00"),
            ("JST", "+09:00"),
            ("KALT", "+02:00"),
            ("KGT", "+06:00"),
            ("KOST", "+11:00"),
            ("KRAT", "+07:00"),
            ("KST", "+09:00"),
            ("LHST", "+10:30"),
            ("LHST", "+11:00"),
            ("LINT", "+14:00"),
            ("MAGT", "+12:00"),
            ("MART", "-09:30"),
            ("MAWT", "+05:00"),
            ("MDT", "-06:00"),
            ("MET", "+01:00"),
            ("MEST", "+02:00"),
            ("MHT", "+12:00"),
            ("MIST", "+11:00"),
            ("MIT", "-09:30"),
            ("MMT", "+06:30"),
            ("MSK", "+03:00"),
            ("MST", "+08:00"),
            ("MST", "-07:00"),
            ("MUT", "+04:00"),
            ("MVT", "+05:00"),
            ("MYT", "+08:00"),
            ("NCT", "+11:00"),
            ("NDT", "-02:30"),
            ("NFT", "+11:00"),
            ("NOVT", "+07:00"),
            ("NPT", "+05:45"),
            ("NST", "-03:30"),
            ("NT", "-03:30"),
            ("NUT", "-11:00"),
            ("NZDT", "+13:00"),
            ("NZST", "+12:00"),
            ("OMST", "+06:00"),
            ("ORAT", "+05:00"),
            ("PDT", "-07:00"),
            ("PET", "-05:00"),
            ("PETT", "+12:00"),
            ("PGT", "+10:00"),
            ("PHOT", "+13:00"),
            ("PHT", "+08:00"),
            ("PHST", "+08:00"),
            ("PKT", "+05:00"),
            ("PMDT", "-02:00"),
            ("PMST", "-03:00"),
            ("PONT", "+11:00"),
            ("PST", "-08:00"),
            ("PWT", "+09:00"),
            ("PYST", "-03:00"),
            ("PYT", "-04:00"),
            ("RET", "+04:00"),
            ("ROTT", "-03:00"),
            ("SAKT", "+11:00"),
            ("SAMT", "+04:00"),
            ("SAST", "+02:00"),
            ("SBT", "+11:00"),
            ("SCT", "+04:00"),
            ("SDT", "-10:00"),
            ("SGT", "+08:00"),
            ("SLST", "+05:30"),
            ("SRET", "+11:00"),
            ("SRT", "-03:00"),
            ("SST", "-11:00"),
            ("SST", "+08:00"),
            ("SYOT", "+03:00"),
            ("TAHT", "-10:00"),
            ("THA", "+07:00"),
            ("TFT", "+05:00"),
            ("TJT", "+05:00"),
            ("TKT", "+13:00"),
            ("TLT", "+09:00"),
            ("TMT", "+05:00"),
            ("TRT", "+03:00"),
            ("TOT", "+13:00"),
            ("TVT", "+12:00"),
            ("ULAST", "+09:00"),
            ("ULAT", "+08:00"),
            ("UYST", "-02:00"),
            ("UYT", "-03:00"),
            ("UZT", "+05:00"),
            ("VET", "-04:00"),
            ("VLAT", "+10:00"),
            ("VOLT", "+04:00"),
            ("VOST", "+06:00"),
            ("VUT", "+11:00"),
            ("WAKT", "+12:00"),
            ("WAST", "+02:00"),
            ("WAT", "+01:00"),
            ("WEST", "+01:00"),
            ("WET", "00:00"),
            ("WIB", "+07:00"),
            ("WIT", "+09:00"),
            ("WITA", "+08:00"),
            ("WGST", "-02:00"),
            ("WGT", "-03:00"),
            ("WST", "+08:00"),
            ("YAKT", "+09:00"),
            ("YEKT", "+05:00")
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
