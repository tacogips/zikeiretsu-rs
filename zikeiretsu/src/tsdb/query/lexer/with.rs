use super::{LexerError, Result as LexerResult};
use crate::tsdb::query::parser::*;

use chrono::{DateTime, Duration, FixedOffset, ParseError as ChoronoParseError, TimeZone, Utc};
use std::collections::HashMap;

use crate::EngineError;

pub(crate) struct With<'q> {
    pub timezone: FixedOffset,
    pub output_format: OutputFormat,
    pub column_index_map: Option<HashMap<&'q str, usize>>,
}

impl<'q> Default for With<'q> {
    fn default() -> Self {
        let timezone: FixedOffset = FixedOffset::west(0);
        let output_format: OutputFormat = OutputFormat::Table;
        let column_index_map: Option<HashMap<&'q str, usize>> = None;

        Self {
            timezone,
            output_format,
            column_index_map,
        }
    }
}

pub(crate) fn interpret_with<'q>(with_clause: Option<WithClause<'q>>) -> LexerResult<With<'q>> {
    let mut with = With::default();

    // with
    if let Some(with_clause) = with_clause {
        // def columns
        if let Some(def_columns) = with_clause.def_columns {
            let mut column_index = HashMap::new();
            for (idx, column) in def_columns.iter().enumerate() {
                match column {
                    Column::Asterick => {
                        return Err(LexerError::InvalidColumnDefinition("".to_string()))
                    }
                    Column::ColumnName(column_name) => {
                        column_index.insert(column_name.as_str(), idx);
                    }
                }
            }
            with.column_index_map = Some(column_index)
        }
        // time zone
        if let Some(tz) = with_clause.def_timezone {
            with.timezone = tz
        }

        // output format
        if let Some(output) = with_clause.def_output {
            with.output_format = output
        }
    }
    Ok(with)
}
