use thiserror::Error;
mod with_storage;

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("failed to create lock file {0}")]
    FailedToGetLockfile(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, EngineError>;
