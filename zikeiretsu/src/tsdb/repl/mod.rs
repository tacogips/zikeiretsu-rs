mod validator;

use super::query::*;
use crate::EngineError;
use rustyline::error::ReadlineError;
use thiserror::Error;
use validator::*;

use rustyline::Editor;

#[derive(Error, Debug)]
pub enum ZikeiretsuReplError {
    #[error("repl read line error {0}")]
    ReadlineError(#[from] ReadlineError),

    #[error("engine error {0}")]
    EngineError(#[from] EngineError),
}

pub type Result<T> = std::result::Result<T, ZikeiretsuReplError>;
pub fn start(ctx: &QueryContext) -> Result<()> {
    let mut editor = Editor::new();
    editor.set_helper(Some(validator::InputValidator));

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

fn execute(ctx: &QueryContext, query_str: &str) {
    parse_query(query_str);
}
