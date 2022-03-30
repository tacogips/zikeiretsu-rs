use super::{LexerError, Result as LexerResult};
use crate::tsdb::query::parser::*;
use std::collections::HashMap;

pub(crate) fn interpret_field_selector<'q>(
    column_index_map: Option<&HashMap<&'q str, usize>>,
    select: Option<&SelectClause<'q>>,
) -> LexerResult<Option<Vec<usize>>> {
    // select columns
    match select {
        None => Err(LexerError::NoSelect),
        Some(select) => {
            if select
                .select_columns
                .iter()
                .find(|each| *each == &Column::Asterick)
                .is_some()
            {
                Ok(None)
            } else {
                let mut field_selectors = Vec::<usize>::new();
                match column_index_map {
                    None => {
                        return Err(LexerError::NoColumnDef(format!(
                            "columns :{}",
                            select
                                .select_columns
                                .iter()
                                .map(|e| e.to_string())
                                .collect::<Vec<String>>()
                                .join(",")
                        )))
                    }
                    Some(column_index_map) => {
                        for column in select.select_columns.iter() {
                            if let Column::ColumnName(column_name) = column {
                                match column_index_map.get(column_name.as_str()) {
                                    Some(column_idx) => field_selectors.push(*column_idx),
                                    None => {
                                        return Err(LexerError::NoColumnDef(format!(
                                            "{}",
                                            column_name.as_str()
                                        )))
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Some(field_selectors))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn lex_select_1() {
        let mut column_map = HashMap::new();
        column_map.insert("c1", 0);
        column_map.insert("c2", 1);
        column_map.insert("c3", 2);

        let select = SelectClause {
            select_columns: vec![
                Column::ColumnName(ColumnName("c2")),
                Column::ColumnName(ColumnName("c1")),
                Column::ColumnName(ColumnName("c3")),
            ],
        };

        let result = interpret_field_selector(Some(&column_map), Some(&select)).unwrap();

        assert_eq!(result, Some(vec![1, 0, 2]));
    }
    #[test]
    fn lex_select_2() {
        let mut column_map = HashMap::new();
        column_map.insert("c1", 0);
        column_map.insert("c2", 1);
        column_map.insert("c3", 2);

        let select = SelectClause {
            select_columns: vec![Column::Asterick],
        };

        let result = interpret_field_selector(Some(&column_map), Some(&select)).unwrap();

        assert_eq!(result, None);
    }

    #[test]
    fn lex_select_err_1() {
        let mut column_map = HashMap::new();
        column_map.insert("c1", 0);
        column_map.insert("c2", 1);
        column_map.insert("c3", 2);

        assert!(interpret_field_selector(Some(&column_map), None).is_err());
    }
}
