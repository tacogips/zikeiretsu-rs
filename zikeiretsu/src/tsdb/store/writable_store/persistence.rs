use super::{storage_api, DatapointSorter, WritableStore};
use crate::tsdb::datapoint::*;
use crate::tsdb::store::writable_store::Result;
use crate::tsdb::timestamp_nano::TimestampNano;
use chrono::{DateTime, Utc};
use log;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::{task, time};

#[derive(Clone)]
pub struct PersistCondition {
    pub datapoint_search_condition: DatapointSearchCondition,
    pub clear_after_persisted: bool,
}

#[derive(Clone)]
pub enum Persistence {
    OnMemory,
    Storage(PathBuf, Option<storage_api::CloudStorageSetting>),
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
    clear_after_persisted: bool,
) -> PeriodicallyPeristenceShutdown {
    let (persistence_tx, mut persistence_rx) = mpsc::channel::<DateTime<Utc>>(1);
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<chrono::DateTime<chrono::Utc>>(1);
    task::spawn(async move {
        loop {
            let waiting_shutdown =
                time::timeout(interval_duration.clone(), shutdown_rx.recv()).await;
            if !waiting_shutdown.is_err() {
                log::info!("breaking the periodicaly persistence loop");

                let datapoint_search_condition =
                    DatapointSearchCondition::new(None, Some(TimestampNano::now()));
                let condition = PersistCondition {
                    datapoint_search_condition,
                    clear_after_persisted,
                };
                let mut mutext_store = store.lock().await;
                if let Err(e) = &mutext_store.persist(condition).await {
                    log::error!("store persisted error:{}", e);
                    //TODO(tacogips) the process should be interrupted ?
                }

                break;
            }
            if let Err(e) = persistence_tx.send(chrono::Utc::now()).await {
                log::error!("periodicaly persistence failed:{}", e);
            }

            let datapoint_search_condition =
                DatapointSearchCondition::new(None, Some(TimestampNano::now()));
            let condition = PersistCondition {
                datapoint_search_condition,
                clear_after_persisted,
            };
            let mut mutext_store = store.lock().await;

            log::debug!("start periodically persistent ");
            if let Err(e) = &mutext_store.persist(condition).await {
                log::error!("store persisted error:{}", e);
                //TODO(tacogips) the process should be interrupted ?
            }
        }
    });

    // TODO(tacogips) what's this doing?
    // persist
    let join_handle =
        task::spawn(async move { while let Some(_dt) = persistence_rx.recv().await {} });

    PeriodicallyPeristenceShutdown {
        shutdown_tx,
        join_handle,
    }
}
