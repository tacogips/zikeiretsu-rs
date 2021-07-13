use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FormatterResult};

#[derive(Hash, PartialEq, Eq, Clone, Debug, Deserialize, Serialize)]
pub struct Metrics(String);

impl Metrics {
    pub fn new<S: ToString>(s: S) -> Self {
        Self(s.to_string())
    }
}

impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Metrics {
    fn from(s: &str) -> Self {
        Metrics::new(s.to_string())
    }
}

impl From<String> for Metrics {
    fn from(s: String) -> Self {
        Metrics::new(s)
    }
}
