use crate::tsdb::query::parser::*;

use pest::iterators::Pair;

#[derive(Debug, PartialEq)]
pub enum PosNeg {
    Positive,
    Negative,
}

impl PosNeg {
    pub fn is_nagative(&self) -> bool {
        *self == PosNeg::Negative
    }
}

pub fn parse_pos_neg<'q>(pair: Pair<'q, Rule>) -> Result<PosNeg> {
    if pair.as_rule() != Rule::POS_NEG {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::POS_NEG),
            format!("{:?}", pair.as_rule()),
        ));
    }

    match pair.as_str() {
        "+" => Ok(PosNeg::Positive),
        "-" => Ok(PosNeg::Negative),
        r => {
            return Err(ParserError::InvalidGrammer(format!(
                " pos/neg mark is neither '+' nor '-' : {r:?}"
            )))
        }
    }
}
