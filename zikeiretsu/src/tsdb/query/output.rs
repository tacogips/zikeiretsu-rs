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

pub enum OutputFormat {
    Json,
    Tsv,
}

pub struct OutputSetting {
    pub format: OutputFormat,
    pub destination: OutputDestination,
}

fn write_to_stdout<I: IntoIterator<Item = D>, D: std::fmt::Display>(datas: I) -> Result<()> {
    let out = stdout();
    let mut out = BufWriter::new(out.lock());

    for each in datas {
        writeln!(out, "{data}", data = each)?;
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
        dest.write(format!("{data}", data = each_data).as_bytes())?;
    }

    dest.flush()?;
    Ok(())
}

pub enum OutputDestination {
    Stdout,
    File(PathBuf),
}
