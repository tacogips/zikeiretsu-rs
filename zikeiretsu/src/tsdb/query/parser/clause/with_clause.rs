use super::super::boolean::parse_bool;
use crate::tsdb::query::parser::*;
use pest::iterators::Pair;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Json,
    DataFrame,
}

#[derive(Debug)]
pub struct WithClause<'q> {
    pub def_columns: Option<Vec<Column<'q>>>,
    pub def_timezone: Option<FixedOffset>,
    pub def_output: Option<OutputFormat>,
    pub def_output_file_path: Option<PathBuf>,
    pub def_use_cache: bool,
    pub def_sync_cloud: bool,
}

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<WithClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::WITH_CLAUSE {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::WITH_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut with_clause = WithClause {
        def_columns: None,
        def_timezone: None,
        def_output: None,
        def_output_file_path: None,
        def_use_cache: true,
        def_sync_cloud: true,
    };
    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::WITH_CLAUSE_DEFINES => {
                for each_define in each.into_inner() {
                    match each_define.as_rule() {
                        Rule::DEFINE_COLUMNS => {
                            for each_in_define_columns in each_define.into_inner() {
                                if each_in_define_columns.as_rule() == Rule::COLUMNS {
                                    let columns =
                                        columns_parser::parse(each_in_define_columns, false)?;

                                    with_clause.def_columns = Some(columns);
                                }
                            }
                        }

                        Rule::DEFINE_TZ => {
                            for each_in_define_tz in each_define.into_inner() {
                                if each_in_define_tz.as_rule() == Rule::TIMEZONE_OFFSET_VAL {
                                    let timezone =
                                        timezone_parser::parse_timezone_offset(each_in_define_tz)?;

                                    with_clause.def_timezone = Some(timezone)
                                } else if each_in_define_tz.as_rule() == Rule::TIMEZONE_NAME {
                                    let timezone =
                                        timezone_parser::parse_timezone_name(each_in_define_tz)?;

                                    with_clause.def_timezone = Some(timezone)
                                }
                            }
                        }

                        Rule::DEFINE_FORMAT => {
                            for each_in_define_tz in each_define.into_inner() {
                                match each_in_define_tz.as_rule() {
                                    Rule::KW_JSON => {
                                        with_clause.def_output = Some(OutputFormat::Json)
                                    }
                                    Rule::KW_DATAFRAME => {
                                        with_clause.def_output = Some(OutputFormat::DataFrame)
                                    }
                                    _ => { /* do nothing */ }
                                }
                            }
                        }

                        Rule::DEFINE_OUTPUT_FILE => {
                            for each_in_output_file in each_define.into_inner() {
                                match each_in_output_file.as_rule() {
                                    Rule::FILE_PATH => {
                                        with_clause.def_output_file_path =
                                            Some(PathBuf::from(each_in_output_file.as_str()));
                                    }
                                    _ => { /* do nothing */ }
                                }
                            }
                        }

                        Rule::DEFINE_CACHE => {
                            for each_inner in each_define.into_inner() {
                                match each_inner.as_rule() {
                                    Rule::BOOLEAN_VALUE => {
                                        with_clause.def_use_cache = parse_bool(each_inner)?;
                                    }
                                    _ => { /* do nothing */ }
                                }
                            }
                        }

                        Rule::DEFINE_CLOUD => {
                            for each_inner in each_define.into_inner() {
                                match each_inner.as_rule() {
                                    Rule::BOOLEAN_VALUE => {
                                        with_clause.def_sync_cloud = parse_bool(each_inner)?;
                                    }
                                    _ => { /* do nothing */ }
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

            _ => { /* do nothing */ }
        }
    }

    Ok(with_clause)
}

#[cfg(test)]
mod test {
    use super::*;

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
        let query = r"with tz = +9:00            ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, Some(FixedOffset::east(9 * 3600)));
        assert_eq!(result.def_output, None);

        assert_eq!(result.def_output_file_path, None);
    }

    #[test]
    fn test_parse_with_3() {
        let query = r"with tz = -9:00            ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, Some(FixedOffset::east(-9 * 3600)));
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
        let query = r"with tz = JST           ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, Some(FixedOffset::east(9 * 3600)));
        assert_eq!(result.def_output, None);
        assert_eq!(result.def_output_file_path, None);
    }
}
