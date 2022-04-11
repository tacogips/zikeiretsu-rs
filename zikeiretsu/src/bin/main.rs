mod args;

use ::zikeiretsu::*;
use args::*;
use clap::Parser;
use dotenv::dotenv;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZikeiretsuBinError {
    #[error("engine error: {0}")]
    EngineError(#[from] EngineError),

    #[error("repl error: {0}")]
    ReplError(#[from] ZikeiretsuReplError),

    #[error("arg error: {0}")]
    ArgError(#[from] ArgsError),

    #[error("query eval error: {0}")]
    EvalError(#[from] EvalError),
}

pub type Result<T> = std::result::Result<T, ZikeiretsuBinError>;

#[tokio::main]
pub async fn main() -> Result<()> {
    let _ = dotenv();
    let args = Args::parse();
    args.setup()?;

    let mut ctx = args.as_db_context()?;
    match args.query {
        Some(query) => execute_query(&ctx, &query).await?,
        None => repl(&mut ctx).await?,
    }

    Ok(())
}

pub async fn repl(ctx: &mut DBContext) -> Result<()> {
    if let Err(e) = repl::start(ctx).await {
        eprintln!("repl error: {e}")
    }
    Ok(())
}
