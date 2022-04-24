use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};

use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::Result;
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};

lazy_static! {
    static ref FINISH_LINE_PATTERN: Regex = RegexBuilder::new(r".*[ \t]*;[ \t]*$").build().unwrap();
}

#[derive(Completer, Helper, Highlighter, Hinter)]
pub struct MultiLineInputValidator;
impl Validator for MultiLineInputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> Result<ValidationResult> {
        Ok(validate_multiline(ctx.input()))
    }
}

fn validate_multiline(input: &str) -> ValidationResult {
    if FINISH_LINE_PATTERN.is_match(input) {
        ValidationResult::Valid(None)
    } else {
        ValidationResult::Incomplete
    }
}
