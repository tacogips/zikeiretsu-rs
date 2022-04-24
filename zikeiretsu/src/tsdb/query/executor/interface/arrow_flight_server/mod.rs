mod do_get_handler;

use crate::tsdb::engine::DBContext;
use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

use futures::prelude::stream::{BoxStream, Stream};
use std::pin::Pin;
use thiserror::Error;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct FlightZikeiretsuService(pub DBContext);
pub type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
#[tonic::async_trait]
impl FlightService for FlightZikeiretsuService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = DoGetStream;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        do_get_handler::handle(&self.0, request).await
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

pub async fn server(db_config: DBContext, host: &str, port: Option<usize>) -> ServeResult<()> {
    let addr = format!("{}:{}", host, port.unwrap_or(51033))
        .parse()
        .map_err(|e| ServeError::AddressParseError(format!("{e}")))?;
    let service = FlightZikeiretsuService(db_config);
    let svc = FlightServiceServer::new(service);

    log::info!("zikeiretsu arrow flight server listening at [{}]", addr);
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}

pub type ServeResult<T> = std::result::Result<T, ServeError>;
#[derive(Error, Debug)]
pub enum ServeError {
    #[error("address parse error: {0}")]
    AddressParseError(String),

    #[error("tonic transport error error: {0}")]
    TonicTransportError(#[from] tonic::transport::Error),
}
