mod args;

use ::zikeiretsu::*;
use args::*;
use clap::Parser;
use dotenv::dotenv;
use std::io;

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

fn setup_log() {
    if std::env::var("RUST_LOG").is_ok() {
        let sub = tracing_subscriber::FmtSubscriber::builder()
            .with_writer(io::stderr)
            .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
            .finish();

        tracing::subscriber::set_global_default(sub).unwrap();
        tracing_log::LogTracer::init().unwrap();
    };
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let _ = dotenv();
    setup_log();

    let mut args = Args::parse();
    args.init()?;

    log::debug!("current_dir :{:?}", std::env::current_dir());

    log::debug!("args:{:?}", args);

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
