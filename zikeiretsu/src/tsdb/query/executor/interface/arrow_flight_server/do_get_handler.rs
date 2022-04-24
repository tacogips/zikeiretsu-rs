use super::DoGetStream;
use crate::tsdb::engine::DBContext;
use async_stream::stream;
use serde_json;
use tonic::{Request, Response, Status};

use crate::execute_query;

use arrow_flight::{utils::flight_data_from_arrow_batch, FlightData, SchemaAsIpc, Ticket};

use arrow::ipc::writer::IpcWriteOptions;

//pub type DoGetStream =
//    Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
//
pub async fn handle(
    ctx: &DBContext,
    request: Request<Ticket>,
) -> Result<Response<DoGetStream>, Status> {
    let ticket = request.into_inner();

    let query = String::from_utf8(ticket.ticket).unwrap();
    let result = execute_query(ctx, &query).await;
    match result {
        Err(e) => Err(Status::invalid_argument(format!("invalid argument :{e}"))),

        Ok(result) => {
            if let Some(records) = result.records {
                let output_condition =
                    serde_json::to_string(&result.output_condition).map_err(|e| {
                        Status::unknown(format!("failed to serialize the output condition :{e}"))
                    })?;

                let write_option = IpcWriteOptions::default();

                let schema_data: FlightData =
                    SchemaAsIpc::new(&records.schema(), &write_option).into();

                let (_dictionaries, mut fligh_batch) =
                    flight_data_from_arrow_batch(&records, &write_option);

                fligh_batch.app_metadata = output_condition.into_bytes();

                Ok(Response::new(Box::pin(stream! {
                    yield Ok(schema_data);
                    yield Ok(fligh_batch)
                })))
            } else {
                Err(Status::not_found("no data found"))
            }
        }
    }
}
