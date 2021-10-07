use chrono::DateTime;
use serde::Deserialize;
use tempdir::TempDir;
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

#[tokio::main]
async fn main() {
    let prices: Vec<Price> = serde_json::from_slice(PRICES_DATA).unwrap();
    let prices: Vec<DataPoint> = prices
        .into_iter()
        .map(|e| e.to_rec().into_datapoint())
        .collect();

    let fields = vec![FieldType::Bool, FieldType::Float64, FieldType::Float64];
    let temp_db_dir = TempDir::new("zikeretsu_local_example").unwrap();
    let persistence = Persistence::Storage(temp_db_dir.path().to_path_buf(), None);

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
    let expected = {
        let mut wr_lock = wr.lock().await;
        let datapoints = wr_lock.datapoints().await.unwrap();
        let searcher = DatapointSearcher::new(&datapoints);

        let dt = DateTime::parse_from_rfc3339("2021-09-27T09:45:01.1749178Z").unwrap();
        let cond = DatapointSearchCondition::since(dt.into());
        let found = searcher.search(&cond).await.unwrap();

        println!("found len:{}", found.len());
        for each in found {
            println!("{}: {:?}", each.timestamp_nano.as_datetime(), each);
        }
        found.to_vec()
    };

    {
        let cond = DatapointSearchCondition::all();

        wr.lock()
            .await
            .persist(PersistCondition {
                datapoint_search_condition: cond,
                clear_after_persisted: false,
            })
            .await
            .unwrap()
    };

    // readonly store store
    let condition = DatapointSearchCondition::all();
    let setting = SearchSettings::builder_with_no_cache().build();
    let read_only_store = Zikeiretsu::readonly_store(temp_db_dir, "price", &condition, &setting)
        .await
        .unwrap();
    let searcher = read_only_store.searcher();

    let dt = DateTime::parse_from_rfc3339("2021-09-27T09:45:01.1749178Z").unwrap();
    let cond = DatapointSearchCondition::since(dt.into());
    let datapoints = searcher.search(&cond).await;

    assert_eq!(datapoints.unwrap().to_vec(), expected);
}
