mod validator;

use super::query::*;
use crate::tsdb::query::eval::execute_query;
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
pub async fn start(ctx: &mut DBContext) -> Result<()> {
    let mut editor = Editor::new();
    editor.set_helper(Some(validator::InputValidator));

    loop {
        let readline = editor.readline(">>");

        match readline {
            Ok(line) => {
                if let Err(e) = execute_query(&ctx, &line).await {
                    eprintln!("erorr: {e}")
                }
            }

            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                return Ok(());
            }
            Err(err) => return Err(ZikeiretsuReplError::from(err)),
        }
    }
}
