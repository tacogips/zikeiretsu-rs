use regex::{Regex, RegexBuilder};

use once_cell::sync::Lazy;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::Result;
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};

static FINISH_LINE_PATTERN: Lazy<Regex> =
    Lazy::new(|| RegexBuilder::new(r".*[ \t]*;[ \t]*$").build().unwrap());

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
