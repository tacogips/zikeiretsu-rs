use super::{storage_api, DatapointSorter, WritableStore};
use crate::tsdb::datapoint::*;
use crate::tsdb::metrics::Metrics;
use crate::tsdb::store::writable_store::Result;
use crate::tsdb::timestamp_nano::TimestampNano;
use crate::tsdb::CloudStorage;
use chrono::{DateTime, Duration, Utc};
use log;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::{task, time};

#[derive(Clone)]
pub struct PersistCondition {
    pub datapoint_search_condition: DatapointsRange,
    pub remove_from_store_after_persisted: bool,
}
impl PersistCondition {
    pub fn new(
        datapoint_search_condition: DatapointsRange,
        remove_from_store_after_persisted: bool,
    ) -> Self {
        Self {
            datapoint_search_condition,
            remove_from_store_after_persisted,
        }
    }
}

#[derive(Clone)]
pub enum Persistence {
    OnMemory,
    Storage(
        PathBuf,
        Option<(CloudStorage, storage_api::CloudStorageSetting)>,
    ),
}

pub struct PeriodicallyPeristenceShutdown {
    shutdown_tx: mpsc::Sender<DateTime<Utc>>,
    join_handle: task::JoinHandle<()>,
}

impl PeriodicallyPeristenceShutdown {
    pub async fn shutdown_and_wait(self) -> Result<()> {
        self.shutdown_tx.send(chrono::Utc::now()).await?;
        self.join_handle.await?;
        Ok(())
    }
}

pub fn start_periodically_persistence<S: DatapointSorter + 'static>(
    store: Arc<Mutex<WritableStore<S>>>,
    interval_duration: Duration,
    remove_from_store_after_persisted: bool,
) -> PeriodicallyPeristenceShutdown {
    let (persistence_tx, mut persistence_rx) = mpsc::channel::<(DateTime<Utc>, Metrics)>(1);
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<chrono::DateTime<chrono::Utc>>(1);
    let interval_duration = interval_duration.to_std().unwrap();
    task::spawn(async move {
        loop {
            let waiting_shutdown = time::timeout(interval_duration, shutdown_rx.recv()).await;
            if waiting_shutdown.is_ok() {
                log::info!("breaking the periodicaly persistence loop");

                let datapoint_search_condition = DatapointsRange::new(
                    None,
                    Some(TimestampNano::now() + Duration::nanoseconds(1)),
                );
                let condition = PersistCondition {
                    datapoint_search_condition,
                    remove_from_store_after_persisted,
                };
                let mut mutex_store = store.lock().await;

                if let Err(e) = persistence_tx
                    .send((chrono::Utc::now(), mutex_store.metrics.clone()))
                    .await
                {
                    log::error!("periodicaly persistence failed:{e}");
                }

                if let Err(e) = &mutex_store.persist(condition).await {
                    log::error!("store persisted error:{e}");
                    //TODO(tacogips) the process should be interrupted ?
                }
                if let Err(e) = mutex_store.scavange_on_shutdown().await {
                    log::error!(" scavenge on shutdown failed: {e}");
                }

                break;
            }

            let datapoint_search_condition =
                DatapointsRange::new(None, Some(TimestampNano::now() + Duration::nanoseconds(1)));
            let condition = PersistCondition {
                datapoint_search_condition,
                remove_from_store_after_persisted,
            };
            let mut mutex_store = store.lock().await;

            if let Err(e) = persistence_tx
                .send((chrono::Utc::now(), mutex_store.metrics.clone()))
                .await
            {
                log::error!("periodicaly persistence failed:{e}");
            }

            log::debug!("start periodically persistent ");
            if let Err(e) = &mutex_store.persist(condition).await {
                log::error!("store persisted error:{e}");
                //TODO(tacogips) the process should be interrupted ?
            }
        }
    });

    // TODO(tacogips) need this??
    // persist
    let join_handle = task::spawn(async move {
        while let Some((dt, metrics)) = persistence_rx.recv().await {
            log::info!("persistent, metrics:{metrics} at: {dt} UTC")
        }
    });

    PeriodicallyPeristenceShutdown {
        shutdown_tx,
        join_handle,
    }
}
