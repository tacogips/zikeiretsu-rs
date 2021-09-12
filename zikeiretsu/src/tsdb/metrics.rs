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
