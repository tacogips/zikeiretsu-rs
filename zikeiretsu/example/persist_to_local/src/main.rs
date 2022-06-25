use chrono::DateTime;
use serde::Deserialize;
use std::io;
use std::path::PathBuf;
use tempdir::TempDir;
use zikeiretsu::*;

const PRICES_DATA: &[u8] = include_bytes!("resources/prices.json");

#[derive(Deserialize, Debug)]
struct Trade {
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub exec_date: String,
}

impl Trade {
    fn into_datapoint(self) -> DataPoint {
        let ts = DateTime::parse_from_rfc3339(&self.exec_date).unwrap();
        DataPoint {
            timestamp_nano: ts.into(),
            field_values: self.field_values(),
        }
    }

    fn field_values(&self) -> Vec<FieldValue> {
        let v = vec![
            FieldValue::Bool(self.side == "BUY"),
            FieldValue::Float64(self.price),
            FieldValue::Float64(self.size),
        ];
        v
    }
}

async fn write_datas(temp_db_dir: &PathBuf) {
    let prices: Vec<Trade> = serde_json::from_slice(PRICES_DATA).unwrap();
    let prices: Vec<DataPoint> = prices.into_iter().map(|e| e.into_datapoint()).collect();

    // field type , [buy_side == bool, price == float64, size == float64]
    let fields = vec![FieldType::Bool, FieldType::Float64, FieldType::Float64];
    let persistence = Persistence::Storage(temp_db_dir.as_path().to_path_buf(), None);

    let wr = Engine::writable_store_builder("trades".try_into().unwrap(), fields.clone())
        .persistence(persistence)
        //give the store specific sort function
        .sorter(|lhs: &DataPoint, rhs: &DataPoint| {
            if lhs.timestamp_nano == rhs.timestamp_nano {
                match (lhs.get_field(0), rhs.get_field(0)) {
                    (Some(lhs_buy_sell), Some(rhs_buy_sell)) => {
                        match (lhs_buy_sell.as_bool(), rhs_buy_sell.as_bool()) {
                            // "BUY" first
                            (Ok(lhs_is_buy), Ok(rhs_is_buy)) => match (lhs_is_buy, rhs_is_buy) {
                                (true, true) => std::cmp::Ordering::Equal,
                                (false, false) => std::cmp::Ordering::Equal,

                                (false, true) => std::cmp::Ordering::Less,
                                (true, false) => std::cmp::Ordering::Greater,
                            },
                            _ => std::cmp::Ordering::Equal,
                        }
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            } else {
                lhs.timestamp_nano.cmp(&rhs.timestamp_nano)
            }
        })
        .build();
    wr.lock().await.push_multi(prices).await.unwrap();
    // persist all datapoints
    let condition = PersistCondition::new(DatapointSearchCondition::all(), true);
    wr.lock().await.persist(condition).await.unwrap();
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    let sub = tracing_subscriber::FmtSubscriber::builder()
        .with_writer(io::stderr)
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(sub).unwrap();
    tracing_log::LogTracer::init().unwrap();

    // database dir path is {data_dir}/{database_name}
    let temp_data_dir = TempDir::new("zikeretsu_local_example").unwrap();

    let mut db_dir: PathBuf = temp_data_dir.path().into();
    db_dir.push("test_db");
    write_datas(&db_dir).await;

    let db_context = DBContext::new(
        temp_data_dir.into_path(),
        None,
        vec![Database::new("test_db".to_string(), None)],
    );

    let query = r#"
    with
        cols = [is_buy,price,size],
        format = table

    select *

    from trades
    where ts  in ('2021-09-27 09:42',+3 minute)
        "#;
    AdhocExecutorInterface
        .execute_query(&db_context, &query)
        .await
        .unwrap();
}
