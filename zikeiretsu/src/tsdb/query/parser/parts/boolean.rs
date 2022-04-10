use crate::tsdb::query::parser::*;
use pest::iterators::Pair;

pub fn parse_bool<'q>(pair: Pair<'q, Rule>) -> Result<bool> {
    if pair.as_rule() != Rule::BOOLEAN_VALUE {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::BOOLEAN_VALUE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    Ok(pair.as_str().to_uppercase() == "TRUE")
}
