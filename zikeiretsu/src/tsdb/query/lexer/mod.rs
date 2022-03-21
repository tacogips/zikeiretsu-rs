use crate::tsdb::datapoint::DatapointSearchCondition;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::query::parser::ParsedQuery;

use crate::EngineError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LexerError {
    #[error("engine error :{0}")]
    EngineError(#[from] EngineError),
}

pub type Result<T> = std::result::Result<T, LexerError>;

pub enum Query {
    ListMetrics,
    Metrics(QueryContext),
}

pub struct QueryContext {
    metrics: Metrics,
    field_selectors: Option<Vec<usize>>,
    search_condotion: DatapointSearchCondition,
    limit: Option<usize>,
    offset: Option<usize>,
}

pub fn interpret(parsed_query: ParsedQuery) -> Result<Query> {
    unimplemented!()
}

//#[derive(Debug)]
//pub struct ParsedQuery<'q> {
//    pub with: Option<WithClause<'q>>,
//    pub select: Option<SelectClause<'q>>,
//    pub from: Option<FromClause<'q>>,
//    pub r#where: Option<WhereClause<'q>>,
//    pub order_or_limit: Option<OrderOrLimitClause<'q>>,
//}
