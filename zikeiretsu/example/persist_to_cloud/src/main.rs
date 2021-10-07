use chrono::DateTime;

use dotenv::dotenv;
use env_logger::{Builder, Target};
use log::LevelFilter;
use serde::Deserialize;
use std::env;
use tempdir::TempDir;
use uuid::Uuid;
use zikeiretsu::*;

const PRICES_DATA: &[u8] = include_bytes!("resources/prices.json");

#[derive(Deserialize, Debug)]
struct Price {
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub exec_date: String,
}

impl Price {
    fn to_rec(self) -> PriceRec {
        let ts = DateTime::parse_from_rfc3339(&self.exec_date).unwrap();

        PriceRec {
            ts: ts.into(),
            is_buy: self.side == "BUY",
            price: self.price,
            size: self.size,
        }
    }
}

#[derive(Debug)]
struct PriceRec {
    ts: TimestampNano,
    is_buy: bool,
    price: f64,
    size: f64,
}

impl PriceRec {
    fn into_datapoint(self) -> DataPoint {
        DataPoint {
            timestamp_nano: self.ts,
            field_values: self.field_values(),
        }
    }

    fn field_values(&self) -> Vec<FieldValue> {
        let v = vec![
            FieldValue::Bool(self.is_buy),
            FieldValue::Float64(self.price),
            FieldValue::Float64(self.size),
        ];
        v
    }
}

async fn persist_to_cloud(cloud_dir: &str) {
    let prices: Vec<Price> = serde_json::from_slice(PRICES_DATA).unwrap();
    let prices: Vec<DataPoint> = prices
        .into_iter()
        .map(|e| e.to_rec().into_datapoint())
        .collect();

    let fields = vec![FieldType::Bool, FieldType::Float64, FieldType::Float64];
    let temp_db_dir = TempDir::new("zikeretsu_local_example_write").unwrap();

    let bucket = env::var("BUCKET").unwrap();
    let cloud_storage = CloudStorage::new_gcp(&bucket, Some(cloud_dir));
    let cloud_storage_setting = CloudStorageSetting::builder(cloud_storage).build();

    let persistence = Persistence::Storage(
        temp_db_dir.path().to_path_buf(),
        Some(cloud_storage_setting),
    );

    let wr = Zikeiretsu::writable_store_builder("price", fields.clone())
        .persistence(persistence)
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

    let condition = PersistCondition {
        datapoint_search_condition: DatapointSearchCondition::all(),
        clear_after_persisted: true,
    };
    wr.lock().await.persist(condition).await.unwrap();
}

async fn load_from_cloud(cloud_dir: &str) {
    let temp_db_dir = TempDir::new("zikeretsu_local_example_readonly").unwrap();
    let bucket = env::var("BUCKET").unwrap();
    let cloud_storage = CloudStorage::new_gcp(&bucket, Some(cloud_dir));
    let cloud_storage_setting = CloudStorageSetting::builder(cloud_storage).build();
    let search_condition = DatapointSearchCondition::all();
    let search_setting = SearchSettings::builder_with_no_cache()
        .cloud_storage_setting(cloud_storage_setting)
        .build();
    let read_store = Zikeiretsu::readonly_store(
        temp_db_dir.path(),
        "price",
        &search_condition,
        &search_setting,
    )
    .await
    .unwrap();
    let searcher = read_store.searcher();
    let result = searcher.search(&search_condition).await;
    assert!(result.is_some());
    let result = result.unwrap();
    assert_eq!(result.len(), 86);
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let mut logger_builder = Builder::new();
    logger_builder.target(Target::Stdout);
    logger_builder.filter_level(LevelFilter::Debug);
    logger_builder.init();

    let my_uuid = Uuid::new_v4();
    let cloud_dir = format!("zdb_{}", my_uuid.to_string());
    persist_to_cloud(&cloud_dir).await;
    load_from_cloud(&cloud_dir).await;
}
