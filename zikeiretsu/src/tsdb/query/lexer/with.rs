use super::{LexerError, Result as LexerResult};
use crate::tsdb::query::parser::clause::{OutputFormat, WithClause};
use crate::tsdb::query::parser::*;
use chrono::FixedOffset;
use std::collections::HashMap;
use std::path::PathBuf;

pub(crate) struct With<'q> {
    pub timezone: FixedOffset,
    pub output_format: OutputFormat,
    pub column_index_map: Option<HashMap<&'q str, usize>>,
    pub output_file_path: Option<PathBuf>,
}

impl<'q> Default for With<'q> {
    fn default() -> Self {
        let timezone: FixedOffset = FixedOffset::west(0);
        let output_format: OutputFormat = OutputFormat::DataFrame;

        Self {
            timezone,
            output_format,
            column_index_map: None,
            output_file_path: None,
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

        // output file path
        with.output_file_path = with_clause.def_output_file_path;

        // output format
        if let Some(output) = with_clause.def_output {
            with.output_format = output
        }
    }
    Ok(with)
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::FixedOffset;

    #[test]
    fn lex_with_1() {
        let with_clause = WithClause {
            def_columns: Some(vec![
                Column::ColumnName(ColumnName("c1")),
                Column::ColumnName(ColumnName("c2")),
                Column::ColumnName(ColumnName("c3")),
            ]),

            def_timezone: None,
            def_output: None,
            def_output_file_path: None,
            def_use_cache: true,
            def_sync_cloud: true,
        };

        let result = interpret_with(Some(with_clause)).unwrap();

        let mut column_map = HashMap::new();
        column_map.insert("c1", 0);
        column_map.insert("c2", 1);
        column_map.insert("c3", 2);
        assert_eq!(result.column_index_map, Some(column_map));
        assert_eq!(result.timezone, FixedOffset::east(0));
        assert_eq!(result.output_format, OutputFormat::DataFrame);
    }
}
