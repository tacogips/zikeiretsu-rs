use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::{Display, Formatter, Result as FormatterResult};

#[derive(Hash, PartialEq, Eq, Clone, Debug, Deserialize, Serialize)]
pub struct Metrics(String);

impl Metrics {
    pub fn new<S: ToString>(s: S) -> Result<Self, String> {
        Ok(Self(s.to_string()))
    }
}

impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "{metrics}", metrics = self.0)
    }
}

impl TryFrom<&str> for Metrics {
    type Error = String;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Metrics::new(s.to_string())
    }
}

impl TryFrom<String> for Metrics {
    type Error = String;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Metrics::new(s)
    }
}
