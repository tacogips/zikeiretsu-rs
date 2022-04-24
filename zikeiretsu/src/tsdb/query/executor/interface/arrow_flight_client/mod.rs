use super::*;
use crate::query::OutputCondition;
use arrow::{datatypes::Schema, error::ArrowError};
use arrow_flight::{
    flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch, FlightData,
    Ticket,
};
use std::sync::Arc;
use tokio_stream::StreamExt;
//use futures::StreamExt;
use tonic::transport::Channel;

pub struct ArrowFlightClientInterface {
    client: FlightServiceClient<Channel>,
}

impl ArrowFlightClientInterface {
    pub async fn new(
        https: bool,
        host: Option<&str>,
        port: Option<usize>,
    ) -> ArrowFlightResult<Self> {
        let url_schema = if https { "https" } else { "http" };
        let addr = format!(
            "{url_schema}://{}:{}",
            host.unwrap_or("0.0.0.0"),
            port.unwrap_or(51033)
        )
        .parse()
        .map_err(|e| ArrowFlightClientError::AddressParseError(format!("{e}")))?;

        let channel = Channel::builder(addr).connect().await?;

        let client = FlightServiceClient::new(channel);
        Ok(Self { client })
    }
}

#[async_trait]
impl ExecutorInterface for ArrowFlightClientInterface {
    async fn execute_query(&mut self, _ctx: &DBContext, query: &str) -> Result<()> {
        let ticket = Ticket {
            ticket: query.as_bytes().to_vec(),
        };

        match self.client.do_get(ticket).await {
            Ok(response_stream) => {
                let mut stream = response_stream.into_inner();
                let mut flight_datas = Vec::<FlightData>::new();
                while let Some(result) = stream.next().await {
                    match result {
                        Err(status) => eprintln!("{}", status),
                        Ok(flight_data) => flight_datas.push(flight_data),
                    }
                }

                if let Err(e) = print_flight_data_query_result(flight_datas).await {
                    eprintln!("{}", e);
                }
            }
            Err(status) => {
                eprintln!("{}", status);
            }
        }

        Ok(())
    }
}

pub async fn print_flight_data_query_result(flight_data: Vec<FlightData>) -> ArrowFlightResult<()> {
    if flight_data.len() != 2 {
        return Err(ArrowFlightClientError::InvalidStreamFlightDataNum(
            flight_data.len(),
        ));
    }

    let schema_data = flight_data.first().unwrap(); //TODO handler errr
    let schema = Schema::try_from(schema_data)?;

    let record_batch_data = flight_data.last().unwrap(); //TODO handler errror
    let output_condition: OutputCondition =
        serde_json::from_slice(&record_batch_data.app_metadata)?;
    let record_batch = flight_data_to_arrow_batch(record_batch_data, Arc::new(schema), &[])?;

    output_records(record_batch, output_condition).await?;
    Ok(())
}

pub type ArrowFlightResult<T> = std::result::Result<T, ArrowFlightClientError>;
#[derive(Error, Debug)]
pub enum ArrowFlightClientError {
    #[error("address parse error: {0}")]
    AddressParseError(String),

    #[error("tonic transport error error: {0}")]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error("json parse error: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("arrow error: {0}")]
    ArrowError(#[from] ArrowError),

    #[error("arrow error: {0}")]
    ExecuteError(#[from] ExecuteError),

    #[error("invalid steamed flight data num {0}")]
    InvalidStreamFlightDataNum(usize),
}
