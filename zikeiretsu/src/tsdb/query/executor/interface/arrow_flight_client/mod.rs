use super::*;
use crate::query::OutputCondition;
use arrow::{datatypes::Schema, error::ArrowError};
use arrow_flight::{
    flight_service_client::FlightServiceClient,
    flight_service_server::FlightService,
    flight_service_server::FlightServiceServer,
    utils::{flight_data_from_arrow_batch, flight_data_to_arrow_batch},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::stream::Stream;
use std::sync::Arc;
use tokio_stream::StreamExt;
//use futures::StreamExt;
use tonic::transport::Channel;

pub struct ArrowFlightClientInterface {
    client: FlightServiceClient<Channel>,
}

impl ArrowFlightClientInterface {
    pub async fn new(host: Option<&str>, port: Option<usize>) -> ArrowFlightResult<Self> {
        let addr = format!("{}:{}", host.unwrap_or("0.0.0.0"), port.unwrap_or(51033))
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
                if let Some(result) = stream.next().await {
                    match result {
                        Err(status) => eprintln!("{}", status),
                        Ok(flight_data) => {
                            if let Err(e) = print_flight_data_query_result(flight_data).await {
                                eprintln!("{}", e);
                            }
                        }
                    }
                }
            }
            Err(status) => {
                eprintln!("{}", status);
            }
        }

        Ok(())
    }
}

pub async fn print_flight_data_query_result(flight_data: FlightData) -> ArrowFlightResult<()> {
    let output_condition: OutputCondition = serde_json::from_slice(&flight_data.app_metadata)?;
    let schema = Schema::try_from(&flight_data)?;
    let record_batch = flight_data_to_arrow_batch(&flight_data, Arc::new(schema), &[])?;

    output_records(record_batch, output_condition).await?;
    Ok(())

    //data: &FlightData,
    //schema: SchemaRef,
    //dictionaries_by_field: &[Option<ArrayRef>],

    //pub struct FlightData {
    //    ///
    //    /// The descriptor of the data. This is only relevant when a client is
    //    /// starting a new DoPut stream.
    //    #[prost(message, optional, tag = "1")]
    //    pub flight_descriptor: ::core::option::Option<FlightDescriptor>,
    //    ///
    //    /// Header for message data as described in Message.fbs::Message.
    //    #[prost(bytes = "vec", tag = "2")]
    //    pub data_header: ::prost::alloc::vec::Vec<u8>,
    //    ///
    //    /// Application-defined metadata.
    //    #[prost(bytes = "vec", tag = "3")]
    //    pub app_metadata: ::prost::alloc::vec::Vec<u8>,
    //    ///
    //    /// The actual batch of Arrow data. Preferably handled with minimal-copies
    //    /// coming last in the definition to help with sidecar patterns (it is
    //    /// expected that some implementations will fetch this field off the wire
    //    /// with specialized code to avoid extra memory copies).
    //    #[prost(bytes = "vec", tag = "1000")]
    //    pub data_body: ::prost::alloc::vec::Vec<u8>,
    //}
    //
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
}
