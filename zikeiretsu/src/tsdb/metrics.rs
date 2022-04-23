use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::{Display, Formatter, Result as FormatterResult};

#[derive(Hash, PartialEq, Eq, Clone, Debug, Deserialize, Serialize)]
pub struct Metrics(String);

impl Metrics {
    pub fn new<S: ToString>(s: S) -> Result<Self, String> {
        let s = s.to_string();
        Self::validate(s.as_str())?;
        Ok(Self(s))
    }

    fn validate(s: &str) -> Result<(), String> {
        if s.starts_with('.') {
            return Err("metrics name can't starts with '.'".to_string());
        }
        Ok(())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn as_inner(&self) -> &String {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
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
