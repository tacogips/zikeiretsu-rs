use super::super::boolean::parse_bool;
use crate::tsdb::query::parser::*;
use crate::tsdb::TimeZoneAndOffset;
use pest::iterators::Pair;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OutputFormat {
    Json,
    Table,
    Parquet,
    ParquetSnappy,
}

#[derive(Debug)]
pub struct WithClause<'q> {
    pub def_columns: Option<Vec<Column<'q>>>,
    pub def_database: Option<&'q str>,
    pub def_timezone: Option<&'static TimeZoneAndOffset>,
    pub def_output: Option<OutputFormat>,
    pub def_output_to_memory: bool,
    pub def_output_file_path: Option<PathBuf>,
    pub def_use_cache: bool,
    pub def_format_datetime: bool,
    pub def_force_sync_cloud: bool,
}

pub fn parse(pair: Pair<'_, Rule>) -> Result<WithClause<'_>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::WITH_CLAUSE {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::WITH_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut with_clause = WithClause {
        def_columns: None,
        def_database: None,
        def_timezone: None,
        def_output: None,
        def_output_to_memory: false,
        def_output_file_path: None,
        def_format_datetime: true,
        def_use_cache: true,
        def_force_sync_cloud: false,
    };
    for each in pair.into_inner() {
        if each.as_rule() == Rule::WITH_CLAUSE_DEFINES {
            for each_define in each.into_inner() {
                match each_define.as_rule() {
                    Rule::DEFINE_COLUMNS => {
                        for each_in_define_columns in each_define.into_inner() {
                            if each_in_define_columns.as_rule() == Rule::COLUMNS {
                                let columns = columns_parser::parse(each_in_define_columns, false)?;

                                with_clause.def_columns = Some(columns);
                            }
                        }
                    }

                    Rule::DEFINE_DATABASE => {
                        for each_in_define_database in each_define.into_inner() {
                            if each_in_define_database.as_rule() == Rule::DB_NAME {
                                with_clause.def_database = Some(each_in_define_database.as_str());
                            }
                        }
                    }

                    Rule::DEFINE_TZ => {
                        for each_in_define_tz in each_define.into_inner() {
                            if each_in_define_tz.as_rule() == Rule::TIMEZONE_NAME {
                                let timezone =
                                    timezone_parser::parse_timezone_name(each_in_define_tz)?;

                                with_clause.def_timezone = Some(timezone)
                            }
                        }
                    }

                    Rule::DEFINE_FORMAT => {
                        for each_in_define_tz in each_define.into_inner() {
                            match each_in_define_tz.as_rule() {
                                Rule::KW_JSON => with_clause.def_output = Some(OutputFormat::Json),
                                Rule::KW_TABLE => {
                                    with_clause.def_output = Some(OutputFormat::Table)
                                }

                                Rule::KW_PARQUET => {
                                    with_clause.def_output = Some(OutputFormat::Parquet)
                                }

                                Rule::KW_PARQUET_SNAPPY => {
                                    with_clause.def_output = Some(OutputFormat::ParquetSnappy)
                                }
                                _ => { /* do nothing */ }
                            }
                        }
                    }

                    Rule::DEFINE_OUTPUT_MEMORY => with_clause.def_output_to_memory = true,
                    Rule::DEFINE_OUTPUT_FILE => {
                        for each_in_output_file in each_define.into_inner() {
                            if each_in_output_file.as_rule() == Rule::FILE_PATH {
                                with_clause.def_output_file_path =
                                    Some(PathBuf::from(each_in_output_file.as_str()));
                            }
                        }
                    }

                    Rule::DEFINE_CACHE => {
                        for each_inner in each_define.into_inner() {
                            if each_inner.as_rule() == Rule::BOOLEAN_VALUE {
                                with_clause.def_use_cache = parse_bool(each_inner)?;
                            }
                        }
                    }

                    Rule::DEFINE_FORMAT_DATETIME => {
                        for each_inner in each_define.into_inner() {
                            if each_inner.as_rule() == Rule::BOOLEAN_VALUE {
                                with_clause.def_format_datetime = parse_bool(each_inner)?;
                            }
                        }
                    }

                    Rule::DEFINE_CLOUD => {
                        for each_inner in each_define.into_inner() {
                            if each_inner.as_rule() == Rule::BOOLEAN_VALUE {
                                with_clause.def_force_sync_cloud = parse_bool(each_inner)?;
                            }
                        }
                    }

                    _ => {
                        return Err(ParserError::InvalidGrammer(format!(
                            "invalid defines in with clause:{}",
                            each_define
                        )))
                    }
                }
            }
        }
    }

    Ok(with_clause)
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::FixedOffset;

    #[test]
    fn test_parse_with_1() {
        let query = r"with cols = [c1,c2, c3]            ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(
            result.def_columns,
            Some(vec![
                Column::ColumnName(ColumnName("c1")),
                Column::ColumnName(ColumnName("c2")),
                Column::ColumnName(ColumnName("c3"))
            ])
        );
        assert_eq!(result.def_timezone, None);
        assert_eq!(result.def_output, None);
        assert_eq!(result.def_output_file_path, None);
    }

    #[test]
    fn test_parse_with_2() {
        let query = r"with tz = Asia/Tokyo            ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(
            result.def_timezone,
            Some(&TimeZoneAndOffset::new(
                ("Asia/Tokyo").parse::<chrono_tz::Tz>().unwrap(),
                FixedOffset::east(9 * 3600)
            ))
        );
        assert_eq!(result.def_output, None);

        assert_eq!(result.def_output_file_path, None);
    }

    #[test]
    fn test_parse_with_3() {
        let query = r"with tz = America/Anchorage           ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(
            result.def_timezone,
            Some(&TimeZoneAndOffset::new(
                ("America/Anchorage").parse::<chrono_tz::Tz>().unwrap(),
                FixedOffset::east(-9 * 3600)
            ))
        );

        assert_eq!(result.def_output, None);
        assert_eq!(result.def_output_file_path, None);
    }

    #[test]
    fn test_parse_with_4() {
        let query = r"with format = json            ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, None);
        assert_eq!(result.def_output, Some(OutputFormat::Json));
        assert_eq!(result.def_output_file_path, None);
    }

    #[test]
    fn test_parse_with_5() {
        let query = r"with output_file =   '/some/thing.json'          ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, None);
        assert_eq!(result.def_output, None);
        assert_eq!(
            result.def_output_file_path,
            Some(PathBuf::from("/some/thing.json"))
        );
    }

    #[test]
    fn test_parse_with_6() {
        let query = r"with tz = America/Anchorage           ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(
            result.def_timezone,
            Some(&TimeZoneAndOffset::new(
                ("America/Anchorage").parse::<chrono_tz::Tz>().unwrap(),
                FixedOffset::east(-9 * 3600)
            ))
        );

        assert_eq!(result.def_output, None);
        assert_eq!(result.def_output_file_path, None);
    }

    #[test]
    fn test_parse_with_7() {
        let query = r"with format =   json          ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, None);
        assert_eq!(result.def_output, Some(OutputFormat::Json));
        assert_eq!(result.def_output_file_path, None,);
    }

    #[test]
    fn test_parse_with_8() {
        let query = r"with format =  table           ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, None);
        assert_eq!(result.def_output, Some(OutputFormat::Table));
        assert_eq!(result.def_output_file_path, None,);
    }

    #[test]
    fn test_parse_with_9() {
        let query = r"with format =   parquet          ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, None);
        assert_eq!(result.def_output, Some(OutputFormat::Parquet));
        assert_eq!(result.def_output_file_path, None,);
    }
}
