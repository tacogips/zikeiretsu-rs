mod args;

use ::zikeiretsu::*;
use args::*;
use dotenv::dotenv;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZikeiretsuBinError {
    #[error("engine error: {0}")]
    EngineError(#[from] EngineError),

    #[error("repl error: {0}")]
    ReplError(#[from] ZikeiretsuReplError),
}

pub type Result<T> = std::result::Result<T, ZikeiretsuBinError>;

#[tokio::main]
pub async fn main() -> Result<()> {
    let _ = dotenv();

    let db_dir = "".to_string();
    let db_config = DBConfig::builder_with_cache().build();
    let mut ctx = DBContext::new(db_dir, db_config);

    repl(&mut ctx).await?;
    Ok(())
}

pub async fn repl(ctx: &mut DBContext) -> Result<()> {
    if let Err(e) = repl::start(&ctx).await {
        eprintln!("repl error: {e}")
    }
    Ok(())
}
