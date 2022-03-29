use ::zikeiretsu::*;
use argh::FromArgs;
use dotenv::Error as DotEnvError;
use std::env;
use thiserror::Error;

macro_rules! set_str_env_var_if_empty {
    ($receiver:expr,$env_key:expr) => {
        if $receiver.is_none() {
            if let Ok(v) = env::var($env_key) {
                $receiver = Some(v)
            }
        }
    };
}

#[derive(Error, Debug)]
pub enum ArgsError {
    #[error("{0} required")]
    MissingRequiredArg(String),

    #[error("environment variable {0} must be {1} but {2}")]
    InvalidEnvVar(String, String, String),

    #[error("invalid timestamp {0}")]
    InvalidTimestampFormat(#[from] chrono::ParseError),

    #[error("failed to load env file: {0}. cause:{1}")]
    FailedToLoadEnvFile(String, DotEnvError),

    #[error("invalid cloud type {0}")]
    InvalidCloudType(String),

    #[error("cloud type required")]
    NoCloudType,

    #[error("bucket required")]
    NoBucket,

    #[error("subpath required")]
    NoSubPath,
}

type Result<T> = std::result::Result<T, ArgsError>;

/// A Toy Timeseries DB 0.1.5
#[derive(FromArgs)]
pub struct Args {
    /// path to block files. it could be specify by environment variable `ZDB_DIR`
    #[argh(option, short = 'd')]
    db_dir: Option<String>,

    /// path to env file.
    #[argh(option, short = 'e')]
    env_file: Option<String>,

    /// type of cloud storage. only 'gcp' is available(aws nor azure are not yet).it could be specify by environment variable `ZDB_CLOUD_TYPE`
    #[argh(option, short = 'c')]
    cloud_type: Option<String>,

    /// bucket name of cloud storage. required if download datas from cloud storage. it could be specify by environment variable `ZDB_BUCKET`
    #[argh(option, short = 'b')]
    bucket: Option<String>,

    /// subpath of the block datas on cloud storage. it could be specify by environment variable `ZDB_CLOUD_SUBPATH`
    #[argh(option, short = 'p')]
    cloud_subpath: Option<String>,

    /// service account file path for GCP. it could be specify by environment variable
    /// `SERVICE_ACCOUNT` or `GOOGLE_APPLICATION_CREDENTIALS`
    #[argh(option, short = 'a')]
    service_account: Option<String>,

    ///download latest datas from cloud before fetch
    #[argh(switch, short = 'x')]
    sync_before_fetch: bool,

    #[argh(subcommand)]
    ope: Ope,
}

impl Args {
    fn fix_with_env_var(&mut self) -> Result<()> {
        set_str_env_var_if_empty!(self.db_dir, "ZDB_DIR");
        set_str_env_var_if_empty!(self.cloud_type, "ZDB_CLOUD_TYPE");
        set_str_env_var_if_empty!(self.bucket, "ZDB_BUCKET");
        set_str_env_var_if_empty!(self.cloud_subpath, "ZDB_CLOUD_SUBPATH");
        Ok(())
    }

    fn set_to_env_var(&mut self) -> Result<()> {
        if let Some(service_account) = self.service_account.as_ref() {
            env::set_var("SERVICE_ACCOUNT", service_account);
        }
        Ok(())
    }

    fn cloud_storage(&self) -> Result<CloudStorage> {
        match &self.cloud_type {
            Some(cloud_type) => {
                let bucket = if let Some(bucket) = self.bucket.as_ref() {
                    bucket
                } else {
                    return Err(ArgsError::NoBucket);
                };

                let subpath = if let Some(subpath) = self.cloud_subpath.as_ref() {
                    subpath
                } else {
                    return Err(ArgsError::NoSubPath);
                };
                match cloud_type.as_str() {
                    "gcp" => Ok(CloudStorage::Gcp(
                        Bucket(bucket.to_string()),
                        SubDir(subpath.to_string()),
                    )),
                    invalid_cloud_type @ _ => {
                        Err(ArgsError::InvalidCloudType(invalid_cloud_type.to_string()))
                    }
                }
            }
            _ => Err(ArgsError::NoCloudType),
        }
    }
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum Ope {
    ListMetrics(ListMetricsOpe),
    Fetch(FetchOpe),
    Describe(DescribeOpe),
}

#[derive(FromArgs, PartialEq, Debug)]
/// list all metrics
#[argh(subcommand, name = "list")]
struct ListMetricsOpe {
    /// destination path to ouput the result. default :stdout
    #[argh(option, short = 'o')]
    output: Option<String>,

    /// output format either [json|tsv]. default "tsv"
    #[argh(option, short = 'f')]
    format: Option<String>,
}

#[derive(FromArgs, PartialEq, Debug)]
/// describe about the database
#[argh(subcommand, name = "describe")]
struct DescribeOpe {
    /// destination path to ouput the result. default:stdout
    #[argh(option, short = 'o')]
    output: Option<String>,

    /// output format either [json|tsv]. default "tsv"
    #[argh(option, short = 'f')]
    format: Option<String>,
}

#[derive(FromArgs, PartialEq, Debug)]
/// fetch metrics data
#[argh(subcommand, name = "fetch")]
struct FetchOpe {
    /// metrics to search
    #[argh(option, short = 'm')]
    metrics: String,

    ///
    /// datetime filter. unix timstamp or rfc3339
    #[argh(option, short = 's')]
    since: Option<String>,

    /// datetime filter. unix timstamp or rfc3339
    #[argh(option, short = 'u')]
    until: Option<String>,

    /// destination path to ouput the result. default :stdout
    #[argh(option, short = 'o')]
    output: Option<String>,

    /// output format either [json|tsv]. default "tsv"
    #[argh(option, short = 'f')]
    format: Option<String>,
}

pub fn parse_args_or_exits() -> Result<Args> {
    let mut args: Args = argh::from_env();
    if let Some(env_file_path) = args.env_file.as_ref() {
        if let Err(e) = dotenv::from_path(&env_file_path) {
            return Err(ArgsError::FailedToLoadEnvFile(env_file_path.to_string(), e));
        }
    }

    args.fix_with_env_var()?;
    args.set_to_env_var()?;
    Ok(args)
}

fn convert_opt_output_format_or_default(output_format: Option<&String>) -> Result<OutputFormat> {
    match output_format {
        Some(format) => {
            let c = OutputFormat::from_str(format)?;
            Ok(c)
        }
        None => Ok(OutputFormat::default()),
    }
}

fn convert_opt_output_destination_or_default(
    output_destination: Option<&String>,
) -> Result<OutputDestination> {
    match output_destination {
        Some(format) => {
            let c = OutputDestination::from_str(format)?;
            Ok(c)
        }
        None => Ok(OutputDestination::default()),
    }
}
