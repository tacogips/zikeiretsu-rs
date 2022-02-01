use super::query::*;
use thiserror::Error;

use crate::EngineError;
use rustyline::error::ReadlineError;
use rustyline::validate::{
    MatchingBracketValidator, ValidationContext, ValidationResult, Validator,
};
use rustyline::Result as RustylineResult;

use rustyline::Editor;
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};

#[derive(Error, Debug)]
pub enum ZikeiretsuReplError {
    #[error("repl read line error {0}")]
    ReadlineError(#[from] ReadlineError),

    #[error("engine error {0}")]
    EngineError(#[from] EngineError),
}

pub type Result<T> = std::result::Result<T, ZikeiretsuReplError>;

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator {
    brackets: MatchingBracketValidator,
}

impl Validator for InputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> RustylineResult<ValidationResult> {
        self.brackets.validate(ctx)
    }
}

pub fn start(_ctx: &mut QueryContext) -> Result<()> {
    let mut editor = Editor::new();
    let validator = InputValidator {
        brackets: MatchingBracketValidator::new(),
    };
    editor.set_helper(Some(validator));

    loop {
        let readline = editor.readline(">>");

        match readline {
            Ok(line) => {
                println!("input line [{line}]")
            }

            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                println!("bye");
                return Ok(());
            }
            Err(err) => return Err(ZikeiretsuReplError::from(err)),
        }
    }
}
