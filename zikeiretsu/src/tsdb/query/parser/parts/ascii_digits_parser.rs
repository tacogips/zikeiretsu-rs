use crate::tsdb::query::parser::*;
use pest::iterators::Pair;

pub fn parse_ascii_digits(pair: Pair<'_, Rule>) -> Result<u64> {
    if pair.as_rule() != Rule::ASCII_DIGITS {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::ASCII_DIGITS),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let val = pair.as_str().trim().parse::<u64>()?;
    Ok(val)
}
