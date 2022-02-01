use thiserror::Error;

use crate::EngineError;
use rustyline::error::ReadlineError;
use rustyline::Editor;

#[derive(Error, Debug)]
pub enum ZikeiretsuReplError {
    #[error("repl read line error {0}")]
    ReadlineError(#[from] ReadlineError),

    #[error("engine error {0}")]
    EngineError(#[from] EngineError),
}

pub type Result<T> = std::result::Result<T, ZikeiretsuReplError>;
pub fn start() -> Result<()> {
    let editor = Editor::<()>::new();

    loop {
        let readline = editor.readline(">>");

        match readline {
            Ok(line) => {}
            Err(_) => {}
        }
    }
}
