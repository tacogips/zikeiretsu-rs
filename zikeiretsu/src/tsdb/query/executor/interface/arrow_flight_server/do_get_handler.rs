use super::DoGetStream;
use futures::Stream;
use std::pin::Pin;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

pub async fn handle(request: Request<Ticket>) -> Result<Response<DoGetStream>, Status> {
    Err(Status::unimplemented("Not yet implemented"))
}
