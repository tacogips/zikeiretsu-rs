mod args;

mod fetch_metrics;
mod operation;

use ::zikeiretsu::*;
use args::*;
use operation::{output::OutputError, *};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZikeiretsuBinError {
    #[error("args error: {0}")]
    ArgsError(#[from] ArgsError),

    #[error("engine error: {0}")]
    EngineError(#[from] EngineError),

    #[error("output error: {0}")]
    OutputError(#[from] OutputError),

    #[error("repl error: {0}")]
    ReplError(#[from] ZikeiretsuReplError),
}

pub type Result<T> = std::result::Result<T, ZikeiretsuBinError>;

//#[tokio::main]
//pub async fn main() -> Result<()> {
//    //TODO(tacogips) impl client
//    let arg = parse_args_or_exits()?;
//    let operation = arg.to_operation()?;
//    match operation {
//        Operation::ListMetrics(list_metrics_condition) => {
//            list_metrics::execute(list_metrics_condition).await?;
//        }
//
//        Operation::FetchMetics(fetch_metrics_condition) => {
//            fetch_metrics::execute(fetch_metrics_condition).await?;
//        }
//
//        Operation::Describe(describe_database_condition) => {
//            describe_metrics::execute(describe_database_condition).await?;
//        }
//    }
//    Ok(())
//}

#[tokio::main]
pub async fn main() -> Result<()> {
    let db_dir = "".to_string();
    let search_setting = SearchSettings::builder_with_cache().build();
    let mut ctx = QueryContext::new(db_dir, search_setting);
    repl::start(&mut ctx)?;
    ////TODO(tacogips) impl client
    //let arg = parse_args_or_exits()?;
    //let operation = arg.to_operation()?;
    //match operation {
    //    Operation::ListMetrics(list_metrics_condition) => {
    //        list_metrics::execute(list_metrics_condition).await?;
    //    }

    //    Operation::FetchMetics(fetch_metrics_condition) => {
    //        fetch_metrics::execute(fetch_metrics_condition).await?;
    //    }

    //    Operation::Describe(describe_database_condition) => {
    //        describe_metrics::execute(describe_database_condition).await?;
    //    }
    //}
    //Ok(())
    //TODO(tacogips) for debugging
    //TODO(tacogips) for debugging

    //TODO(tacogips) for debugging

    //TODO(tacogips) for debugging
    //TODO(tacogips) for debugging

    Ok(())
}
