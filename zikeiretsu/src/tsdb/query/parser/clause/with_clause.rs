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
        match each.as_rule() {
            Rule::DEFINE_COLUMNS => {
                for each_in_define_columns in each.into_inner() {
                    if each_in_define_columns.as_rule() == Rule::COLUMNS {
                        let columns = columns_parser::parse(each_in_define_columns, false)?;
                        with_clause.def_columns = Some(columns)
                    }
                }
            }

            Rule::DEFINE_TZ => {
                for each_in_define_tz in each.into_inner() {
                    if each_in_define_tz.as_rule() == Rule::TIMEZONE_OFFSET_VAL {
                        let timezone = timezone_parser::parse_timezone_offset(each_in_define_tz)?;

                        with_clause.def_timezone = Some(timezone)
                    }
                }
            }

            Rule::DEFINE_FORMAT => {
                for each_in_define_tz in each.into_inner() {
                    match each_in_define_tz.as_rule() {
                        Rule::KW_JSON => with_clause.def_output = Some(OutputFormat::Json),
                        Rule::KW_TABLE => with_clause.def_output = Some(OutputFormat::Table),
                        _ => { /* do nothing */ }
                    }
                }
            }

            _ => {}
        }
    }

    Ok(with_clause)
}
