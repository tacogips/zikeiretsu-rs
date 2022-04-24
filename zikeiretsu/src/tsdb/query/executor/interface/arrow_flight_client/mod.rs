use super::*;
pub struct ArrowFlightClientInterface;

impl ArrowFlightClientInterface {}

#[async_trait]
impl ExecutorInterface for ArrowFlightClientInterface {
    async fn execute_query(&self, ctx: &DBContext, query: &str) -> Result<()> {
        match execute_query(ctx, query).await {
            Err(e) => {
                eprintln!("{}", e);
            }
            Ok(result) => {
                if let Some(records) = result.records {
                    output_records(records, result.output_condition).await?
                } else {
                    println!("[empty]")
                }
            }
        }
        Ok(())
    }
}
