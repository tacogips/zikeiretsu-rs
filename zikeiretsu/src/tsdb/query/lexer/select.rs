use super::{LexerError, Result as LexerResult};

use crate::tsdb::datapoint::DatapointSearchCondition;
use crate::tsdb::datetime::DatetimeAccuracy;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::query::parser::*;
use chrono::{DateTime, Duration, FixedOffset, ParseError as ChoronoParseError, TimeZone, Utc};
use either::Either;
use std::collections::HashMap;

use crate::EngineError;
use thiserror::Error;

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
