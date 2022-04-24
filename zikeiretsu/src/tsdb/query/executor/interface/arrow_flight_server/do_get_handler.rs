use super::DoGetStream;
use crate::tsdb::engine::DBContext;
use futures::Stream;
use std::pin::Pin;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::{execute_query, output::*, ExecuteError};

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

pub async fn handle(
    ctx: &DBContext,
    request: Request<Ticket>,
) -> Result<Response<DoGetStream>, Status> {
    let ticket = request.into_inner();
    let query = ticket.to_string();
    let result = execute_query(ctx, &query).await;
    Err(Status::unimplemented("Not yet implemented"))
}
