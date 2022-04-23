use pest::iterators::Pair;

use crate::tsdb::query::parser::*;

#[derive(Debug, PartialEq)]
pub struct SelectClause<'q> {
    pub select_columns: Vec<Column<'q>>,
}

pub fn parse(pair: Pair<'_, Rule>) -> Result<SelectClause<'_>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::SELECT_CLAUSE {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::SELECT_CLAUSE),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut select_columns: Option<Vec<Column<'_>>> = None;
    for each in pair.into_inner() {
        match each.as_rule() {
            Rule::COLUMNS => {
                let columns = columns_parser::parse(each, false)?;
                select_columns = Some(columns)
            }
            Rule::KW_ASTERISK => select_columns = Some(vec![Column::Asterick]),
            _ => {}
        }
    }

    // if it might be a bug if the result could not pass validation below.
    match select_columns {
        Some(cols) if cols.is_empty() => {
            Err(ParserError::EmptyColumns("select clause".to_string()))
        }
        None => Err(ParserError::EmptyColumns("select clause".to_string())),
        Some(select_columns) => Ok(SelectClause { select_columns }),
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use pest::*;

    #[test]
    fn parse_select_1() {
        let select_clause = r"select ts,some
            ";

        let pairs = QueryGrammer::parse(Rule::SELECT_CLAUSE, select_clause);

        assert!(pairs.is_ok());
        let parsed = parse(pairs.unwrap().next().unwrap());
        assert_eq!(
            SelectClause {
                select_columns: vec![
                    Column::ColumnName(ColumnName("ts")),
                    Column::ColumnName(ColumnName("some"))
                ]
            },
            parsed.unwrap()
        )
    }
}
