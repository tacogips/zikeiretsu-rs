mod timezone;

use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "tsdb/query/query.pest"]
pub struct QueryParser {}

pub struct Query<'q> {
    pub def_columns: Option<Vec<&'q str>>,
    pub def_timezone: Option<&'q str>,
    pub select_columns: Option<Vec<&'q str>>,
    pub select_metrics: &'q str,
}

pub enum TsFilter<'q> {
    In(TsFilterValue<'q>, TsFilterValue<'q>),
    Gte(TsFilterValue<'q>),
    Gt(TsFilterValue<'q>),
    Lte(TsFilterValue<'q>),
    Lt(TsFilterValue<'q>),
    Equal(TsFilterValue<'q>),
}

pub enum TsFilterValue<'a> {
    DateString(&'a str),
    Function,
}

pub fn parse_query(query: &str) {
    QueryParser::parse(Rule::QUERY, query);
    unimplemented!()
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn parse_timezone_offset_val() {
        let pairs = QueryParser::parse(Rule::TIMEZONE_OFFSET_VAL, "+1");

        for each in pairs.unwrap() {
            if each.as_rule() == Rule::TIMEZONE_OFFSET_VAL {
                //TODO(tacogips) for debugging
                println!("==== {:?}", each.as_rule());
            }
        }

        assert!(false);
    }
}
