use serde_json;
use std::fs::File;
use std::io::{stdout, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum OutputError {
    #[error("io error: {0} ")]
    IOError(#[from] std::io::Error),

    #[error("invalid output formt: {0} ")]
    InvalidOutputFormat(String),

    #[error("invalid output destination: {0} ")]
    InvalidOutputDestination(String),

    #[error("invalid json: {0} ")]
    SerdeJsonError(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, OutputError>;

pub struct OutputSetting {
    pub format: OutputFormat,
    pub destination: OutputDestination,
}

pub enum OutputFormat {
    Json,
    Tsv,
}

impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::Tsv
    }
}

impl FromStr for OutputFormat {
    type Err = OutputError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let r = match s {
            "json" => Self::Json,
            "tsv" => Self::Tsv,
            invalid_format @ _ => {
                return Err(OutputError::InvalidOutputFormat(invalid_format.to_string()))
            }
        };
        Ok(r)
    }
}

fn write_to_stdout<I: IntoIterator<Item = D>, D: std::fmt::Display>(datas: I) -> Result<()> {
    let out = stdout();
    let mut out = BufWriter::new(out.lock());

    for each in datas {
        writeln!(out, "{}", each)?;
    }
    Ok(())
}

fn write_to_file<'a, I: IntoIterator<Item = D>, D: std::fmt::Display>(
    p: &'a Path,
    datas: I,
) -> Result<()> {
    let dest = File::create(p)?;
    let mut dest = BufWriter::new(dest);

    for each_data in datas {
        dest.write(format!("{}", each_data).as_bytes())?;
    }

    dest.flush()?;
    Ok(())
}

pub enum OutputDestination {
    Stdout,
    File(PathBuf),
}

impl OutputDestination {
    pub fn write<I: IntoIterator<Item = D>, D: std::fmt::Display>(self, datas: I) -> Result<()> {
        match self {
            Self::Stdout => write_to_stdout(datas),
            Self::File(path) => write_to_file(path.as_ref(), datas),
        }
    }
}

impl Default for OutputDestination {
    fn default() -> Self {
        OutputDestination::Stdout
    }
}

impl FromStr for OutputDestination {
    type Err = OutputError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        if value == "stdout" {
            Ok(OutputDestination::Stdout)
        } else {
            let mut pathbuf = PathBuf::new();
            pathbuf.push(value);
            Ok(OutputDestination::File(pathbuf))
        }
    }
}
