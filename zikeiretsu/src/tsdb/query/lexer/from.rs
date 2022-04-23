use super::{LexerError, Result as LexerResult};
use crate::tsdb::metrics::Metrics;
use crate::tsdb::query::parser::clause::FromClause;
use either::Either;

pub(crate) enum BuildinMetrics {
    ListMetrics,
    DescribeMetrics,
    DescribeBlockList,
}

impl BuildinMetrics {
    fn from(metrics: &str) -> Option<Self> {
        match metrics {
            ".metrics" => Some(Self::ListMetrics),
            ".describe" => Some(Self::DescribeMetrics),
            ".block_list" => Some(Self::DescribeBlockList),
            _ => None,
        }
    }
}

pub(crate) fn parse_from(
    from_clause: Option<&FromClause<'_>>,
) -> LexerResult<Either<Metrics, BuildinMetrics>> {
    match from_clause {
        None => Err(LexerError::NoFrom),
        Some(metrics) => match BuildinMetrics::from(metrics.from) {
            Some(build_in_query) => Ok(Either::Right(build_in_query)),
            None => {
                let metrics =
                    Metrics::new(metrics.from.to_string()).map_err(LexerError::InvalidMetrics)?;
                Ok(Either::Left(metrics))
            }
        },
    }
}
