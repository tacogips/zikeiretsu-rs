mod validator;

use super::engine::*;
use crate::tsdb::query::executor::execute_query;
use crate::EngineError;
use dirs::home_dir;
use rustyline::error::ReadlineError;
use rustyline::{Cmd, CompletionType, Config, EditMode, Editor, KeyEvent};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZikeiretsuReplError {
    #[error("repl read line error {0}")]
    ReadlineError(#[from] ReadlineError),

    #[error("engine error {0}")]
    EngineError(#[from] EngineError),

    #[error("failed to  get home dir ")]
    FailedToGetHomeDir,
    #[error("failed to create db dir {0}")]
    FailedToCreateHistoryDir(PathBuf),
}

pub type Result<T> = std::result::Result<T, ZikeiretsuReplError>;
pub async fn start(ctx: &mut DBContext) -> Result<()> {
    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Emacs)
        .build();

    let mut editor = Editor::with_config(config);
    editor.bind_sequence(KeyEvent::ctrl('p'), Cmd::PreviousHistory);
    editor.bind_sequence(KeyEvent::ctrl('n'), Cmd::NextHistory);

    editor.set_helper(Some(validator::MultiLineInputValidator));
    let history_file_path = repl_history_file()?;
    let _ = editor.load_history(history_file_path.as_path());

    loop {
        println!("");
        let readline = editor.readline("query> ");

        match readline {
            Ok(line) => {
                log::debug!("qeury:{}", line);
                editor.add_history_entry(line.as_str());
                if let Err(e) = execute_query(&ctx, &line).await {
                    eprintln!("query error: {e}")
                }
            }

            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                let _ = editor.save_history(history_file_path.as_path());
                println!("bye");
                return Ok(());
            }
            Err(err) => {
                let _ = editor.save_history(history_file_path.as_path());
                return Err(ZikeiretsuReplError::from(err));
            }
        }
    }
}

fn repl_history_file() -> Result<PathBuf> {
    let history_file_path = default_history_path()?;
    create_history_dir_if_not_exists(history_file_path.as_path())?;
    Ok(history_file_path)
}

pub fn create_history_dir_if_not_exists(db_path: &Path) -> Result<()> {
    let dir = db_path
        .parent()
        .ok_or_else(|| ZikeiretsuReplError::FailedToCreateHistoryDir(db_path.to_path_buf()))?;

    if !dir.exists() {
        fs::create_dir_all(dir)
            .map_err(|_| ZikeiretsuReplError::FailedToCreateHistoryDir(dir.to_path_buf()))?;
    }

    Ok(())
}

fn default_history_path() -> Result<PathBuf> {
    let mut dir = home_dir().ok_or(ZikeiretsuReplError::FailedToGetHomeDir)?;
    dir.push(".local/share/zikeiretsu/query_history");
    Ok(dir)
}
