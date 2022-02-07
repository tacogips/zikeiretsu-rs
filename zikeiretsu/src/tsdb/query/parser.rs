use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "tsdb/query/query.pest"]
pub struct QueryParser {}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn parse_test() {
        let succ = QueryParser::parse(
            Rule::file,
            "65279,1179403647,1463895090\n\t12,4
            ",
        );
        println!("{:?}", succ);
        assert!(succ.is_ok())
    }
}
