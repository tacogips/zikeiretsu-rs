mod validator;

use super::query::*;
use crate::tsdb::query::eval::execute_query;
use crate::EngineError;
use rustyline::error::ReadlineError;
use thiserror::Error;

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
    editor.set_helper(Some(validator::MultiLineInputValidator));

    loop {
        println!("");
        let readline = editor.readline("query>>");

        match readline {
            Ok(line) => {
                log::debug!("qeury:{}", line);
                if let Err(e) = execute_query(&ctx, &line).await {
                    eprintln!("query error: {e}")
                }
            }

            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                println!("bye");
                return Ok(());
            }
            Err(err) => return Err(ZikeiretsuReplError::from(err)),
        }
    }
}
