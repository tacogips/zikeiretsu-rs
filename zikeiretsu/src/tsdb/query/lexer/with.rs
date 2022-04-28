use super::{LexerError, Result as LexerResult};
use crate::tsdb::query::parser::clause::{OutputFormat, WithClause};
use crate::tsdb::query::parser::*;
use crate::tsdb::{CacheSetting, CloudStorageSetting};
use chrono::FixedOffset;
use std::collections::HashMap;
use std::path::PathBuf;

pub(crate) struct With<'q> {
    pub timezone: FixedOffset,
    pub database: Option<&'q str>,
    pub output_format: OutputFormat,
    pub format_datetime: bool,
    pub column_index_map: Option<HashMap<&'q str, usize>>,
    pub column_name_aliases: Option<Vec<String>>,
    pub output_file_path: Option<PathBuf>,
    pub cache_setting: CacheSetting,
    pub cloud_setting: CloudStorageSetting,
}

impl<'q> Default for With<'q> {
    fn default() -> Self {
        let timezone: FixedOffset = FixedOffset::west(0);
        let output_format: OutputFormat = OutputFormat::Table;

        Self {
            timezone,
            output_format,
            format_datetime: true,
            database: None,
            column_index_map: None,
            column_name_aliases: None,
            output_file_path: None,
            cache_setting: CacheSetting::default(),
            cloud_setting: CloudStorageSetting::default(),
        }
    }
}

pub(crate) fn interpret_with(with_clause: Option<WithClause<'_>>) -> LexerResult<With<'_>> {
    let mut with = With::default();

    // with
    if let Some(with_clause) = with_clause {
        // def columns
        if let Some(def_columns) = with_clause.def_columns {
            let mut column_index = HashMap::new();
            for (idx, column) in def_columns.iter().enumerate() {
                match column {
                    Column::Asterick => {
                        // never happened except bug.
                        return Err(LexerError::InvalidColumnDefinition(
                            "* is invali".to_string(),
                        ));
                    }
                    Column::ColumnName(column_name) => {
                        column_index.insert(column_name.as_str(), idx);
                    }
                }
            }
            with.column_index_map = Some(column_index);

            with.column_name_aliases = Some(
                def_columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>(),
            )
        }

        // database
        with.database = with_clause.def_database;

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

        // format datetiem
        with.format_datetime = with_clause.def_format_datetime;

        // cache setting
        if with_clause.def_use_cache {
            with.cache_setting = CacheSetting::both();
        }

        // cloud setting
        if with_clause.def_sync_cloud {
            with.cloud_setting = CloudStorageSetting::builder()
                .force_update_block_list(true)
                .build();
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

            def_database: None,
            def_timezone: None,
            def_format_datetime: true,
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
        assert_eq!(result.output_format, OutputFormat::Table);
    }
}
