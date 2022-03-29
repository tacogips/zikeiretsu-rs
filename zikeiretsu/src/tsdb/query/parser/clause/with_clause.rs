use crate::tsdb::query::parser::*;
use pest::iterators::Pair;

#[derive(Debug, PartialEq)]
pub enum OutputFormat {
    Json,
    Table,
}

#[derive(Debug)]
pub struct WithClause<'q> {
    pub def_columns: Option<Vec<Column<'q>>>,
    pub def_timezone: Option<FixedOffset>,
    pub def_output: Option<OutputFormat>,
}

pub fn parse<'q>(pair: Pair<'q, Rule>) -> Result<WithClause<'q>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::WITH_CLAUSE {
        return Err(QueryError::UnexpectedPair(
            format!("{:?}", Rule::WITH_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut with_clause = WithClause {
        def_columns: None,
        def_timezone: None,
        def_output: None,
    };
    for each in pair.into_inner() {
        //TODO(tacogips) for debugging
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
                                }
                            }
                        }

                        Rule::DEFINE_FORMAT => {
                            for each_in_define_tz in each_define.into_inner() {
                                match each_in_define_tz.as_rule() {
                                    Rule::KW_JSON => {
                                        with_clause.def_output = Some(OutputFormat::Json)
                                    }
                                    Rule::KW_TABLE => {
                                        with_clause.def_output = Some(OutputFormat::Table)
                                    }
                                    _ => { /* do nothing */ }
                                }
                            }
                        }
                        _ => {
                            return Err(QueryError::InvalidGrammer(format!(
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
    }

    #[test]
    fn test_parse_with_2() {
        let query = r"with tz = +9:00            ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, Some(FixedOffset::east(9 * 3600)));
        assert_eq!(result.def_output, None);
    }

    #[test]
    fn test_parse_with_3() {
        let query = r"with tz = -9:00            ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, Some(FixedOffset::east(-9 * 3600)));
        assert_eq!(result.def_output, None);
    }

    #[test]
    fn test_parse_with_4() {
        let query = r"with format = json            ";
        let mut pairs = QueryGrammer::parse(Rule::WITH_CLAUSE, query).unwrap();
        let result = parse(pairs.next().unwrap()).unwrap();
        assert_eq!(result.def_columns, None);
        assert_eq!(result.def_timezone, None);
        assert_eq!(result.def_output, Some(OutputFormat::Json));
    }
}
