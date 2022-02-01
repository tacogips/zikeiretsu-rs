use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};

use rustyline::validate::{
    MatchingBracketValidator, ValidationContext, ValidationResult, Validator,
};
use rustyline::Result;
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};

lazy_static! {
    static ref MIDDLE_OF_MULTI_LINE_PATTEN: Regex =
        RegexBuilder::new(r".*\\[ \t]*$").build().unwrap();
}

#[derive(Completer, Helper, Highlighter, Hinter)]
pub struct InputValidator;
impl Validator for InputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> Result<ValidationResult> {
        Ok(validate_multiline(ctx.input()))
    }
}

fn validate_multiline(input: &str) -> ValidationResult {
    if MIDDLE_OF_MULTI_LINE_PATTEN.is_match(input) {
        ValidationResult::Incomplete
    } else {
        ValidationResult::Valid(None)
    }
}
