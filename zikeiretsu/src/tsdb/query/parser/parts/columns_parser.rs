use once_cell::sync::OnceCell;
use pest::iterators::Pair;
use std::collections::HashSet;

use crate::tsdb::query::parser::*;

pub fn parse<'q>(pair: Pair<'q, Rule>, allow_asterisk: bool) -> Result<Vec<Column<'q>>> {
    #[cfg(debug_assertions)]
    if pair.as_rule() != Rule::COLUMNS {
        return Err(ParserError::UnexpectedPair(
            format!("{:?}", Rule::COLUMNS),
            format!("{:?}", pair.as_rule()),
        ));
    }

    let mut columns = Vec::<Column<'q>>::new();
    for each_pair_in_columns in pair.into_inner() {
        if each_pair_in_columns.as_rule() == Rule::COLUMN_NAME {
            let column_str = each_pair_in_columns.as_str();
            if column_str == "*" {
                if allow_asterisk {
                    columns.push(Column::Asterick)
                } else {
                    return Err(ParserError::InvalidColumnName(column_str.to_string()));
                }
            } else {
                validate_column_name(column_str)?;
                columns.push(Column::ColumnName(ColumnName(
                    each_pair_in_columns.as_str(),
                )))
            }
        }
    }
    Ok(columns)
}

static INVALID_COLUMN_NAME: OnceCell<HashSet<&'static str>> = OnceCell::new();

pub fn invalid_colum_names() -> &'static HashSet<&'static str> {
    INVALID_COLUMN_NAME.get_or_init(|| {
        let mut s = HashSet::new();
        s.insert("SELECT");
        s.insert("FROM");
        s.insert("WITH");
        s.insert("WHERE");
        s.insert("AND");
        s.insert("OR");
        s.insert("DESC");
        s.insert("ASC");
        s.insert("OFFSET");
        s.insert("LIMIT");
        s.insert("COLS");
        s.insert("TZ");
        s
    })
}

fn validate_column_name(column_name: &str) -> Result<()> {
    if invalid_colum_names().contains(column_name.to_uppercase().as_str()) {
        return Err(ParserError::InvalidColumnName(column_name.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_columns_1() {
        let query = r"c1,c2, c3";
        let mut pairs = QueryGrammer::parse(Rule::COLUMNS, query).unwrap();
        let result = parse(pairs.next().unwrap(), false).unwrap();
        assert_eq!(
            result,
            vec![
                Column::ColumnName(ColumnName("c1")),
                Column::ColumnName(ColumnName("c2")),
                Column::ColumnName(ColumnName("c3"))
            ]
        );
    }
}
