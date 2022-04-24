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

    #[error("query executor error: {0}")]
    ExecutorInterfaceError(#[from] ExecutorInterfaceError),

    #[error("arrow flight serve error: {0}")]
    ArrowFlightServeError(#[from] ArrowFlightServeError),

    #[error("arrow flight client error: {0}")]
    ArrowFlightClientError(#[from] ArrowFlightClientError),
}

pub type Result<T> = std::result::Result<T, ZikeiretsuBinError>;

fn setup_log(log_to_std_out: bool) {
    if std::env::var("RUST_LOG").is_ok() {
        if log_to_std_out {
            let sub = tracing_subscriber::FmtSubscriber::builder()
                .with_writer(io::stdout)
                .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
                .finish();
            tracing::subscriber::set_global_default(sub).unwrap();
        } else {
            let sub = tracing_subscriber::FmtSubscriber::builder()
                .with_writer(io::stderr)
                .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
                .finish();

            tracing::subscriber::set_global_default(sub).unwrap();
        };

        tracing_log::LogTracer::init().unwrap();
    };
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let _ = dotenv();

    let mut args = Args::parse();
    args.init(true)?;

    log::debug!("current_dir :{:?}", std::env::current_dir());
    let mut ctx = args.as_db_context()?;

    let mode = args.mode.unwrap_or(Mode::Adhoc);
    if mode == Mode::Server {
        setup_log(true);
        arrow_flight_server(ctx, args.host.as_deref(), args.port).await?;
    } else {
        setup_log(false);
        let mut executor_interface: Box<dyn ExecutorInterface> = if mode == Mode::Adhoc {
            Box::new(AdhocExecutorInterface)
        } else {
            Box::new(
                ArrowFlightClientInterface::new(args.https, args.host.as_deref(), args.port)
                    .await?,
            )
        };
        match args.query {
            Some(query) => executor_interface.execute_query(&ctx, &query).await?,
            None => repl(&mut ctx, executor_interface).await?,
        }
    };

    Ok(())
}

pub async fn repl(
    ctx: &mut DBContext,
    executor_interface: Box<dyn ExecutorInterface>,
) -> Result<()> {
    if let Err(e) = repl::start(ctx, executor_interface).await {
        eprintln!("repl error: {e}")
    }
    Ok(())
}
