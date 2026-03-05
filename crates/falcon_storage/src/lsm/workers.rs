use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use super::engine::LsmEngine;
use crate::resource_isolation::ResourceIsolator;

/// Background worker handles for flush and compaction.
pub struct BgWorkers {
    shutdown: Arc<AtomicBool>,
    flush_notify: Arc<Notify>,
    compaction_notify: Arc<Notify>,
    flush_handle: Option<tokio::task::JoinHandle<()>>,
    compaction_handle: Option<tokio::task::JoinHandle<()>>,
}

impl BgWorkers {
    /// Spawn background flush and compaction workers for the given engine.
    pub fn spawn(
        engine: Arc<LsmEngine>,
        flush_interval: Duration,
        compaction_interval: Duration,
        isolator: Arc<ResourceIsolator>,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let flush_notify = Arc::new(Notify::new());
        let compaction_notify = Arc::new(Notify::new());

        let flush_handle = {
            let engine = engine.clone();
            let shutdown = shutdown.clone();
            let notify = flush_notify.clone();
            let compaction_notify = compaction_notify.clone();
            let isolator = isolator.clone();
            tokio::spawn(async move {
                flush_loop(engine, shutdown, notify, compaction_notify, flush_interval, isolator).await;
            })
        };

        let compaction_handle = {
            let engine = engine.clone();
            let shutdown = shutdown.clone();
            let notify = compaction_notify.clone();
            let isolator = isolator.clone();
            tokio::spawn(async move {
                compaction_loop(engine, shutdown, notify, compaction_interval, isolator).await;
            })
        };

        Self {
            shutdown,
            flush_notify,
            compaction_notify,
            flush_handle: Some(flush_handle),
            compaction_handle: Some(compaction_handle),
        }
    }

    /// Wake up the flush worker immediately (e.g. when memtable is full).
    pub fn notify_flush(&self) {
        self.flush_notify.notify_one();
    }

    /// Wake up the compaction worker immediately.
    pub fn notify_compaction(&self) {
        self.compaction_notify.notify_one();
    }

    /// Signal shutdown and wait for workers to finish.
    pub async fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.flush_notify.notify_one();
        self.compaction_notify.notify_one();
        if let Some(h) = self.flush_handle.take() {
            let _ = h.await;
        }
        if let Some(h) = self.compaction_handle.take() {
            let _ = h.await;
        }
    }
}

async fn flush_loop(
    engine: Arc<LsmEngine>,
    shutdown: Arc<AtomicBool>,
    notify: Arc<Notify>,
    compaction_notify: Arc<Notify>,
    interval: Duration,
    isolator: Arc<ResourceIsolator>,
) {
    loop {
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(interval) => {}
        }
        if shutdown.load(Ordering::SeqCst) {
            let eng = engine.clone();
            let _ = tokio::task::spawn_blocking(move || eng.flush()).await;
            break;
        }

        // Acquire CPU slot for background flush
        let guard = isolator.cpu.try_acquire();
        if guard.is_none() {
            tracing::trace!("bg flush skipped: CPU slots exhausted");
            continue;
        }

        let eng = engine.clone();
        let iso = isolator.clone();
        let result = tokio::task::spawn_blocking(move || {
            let r = eng.flush();
            // Charge estimated flush I/O (memtable size estimate)
            iso.io.consume_blocking(eng.stats().memtable_bytes);
            r
        }).await;
        drop(guard);
        match result {
            Ok(Ok(())) => {
                compaction_notify.notify_one();
            }
            Ok(Err(e)) => {
                tracing::warn!("bg flush error: {}", e);
            }
            Err(e) => {
                tracing::error!("bg flush panicked: {}", e);
            }
        }
    }
}

async fn compaction_loop(
    engine: Arc<LsmEngine>,
    shutdown: Arc<AtomicBool>,
    notify: Arc<Notify>,
    interval: Duration,
    isolator: Arc<ResourceIsolator>,
) {
    loop {
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(interval) => {}
        }
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        let guard = isolator.cpu.try_acquire();
        if guard.is_none() {
            tracing::trace!("bg compaction skipped: CPU slots exhausted");
            continue;
        }

        let eng = engine.clone();
        let result = tokio::task::spawn_blocking(move || eng.maybe_trigger_compaction()).await;
        drop(guard);
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!("bg compaction error: {}", e);
            }
            Err(e) => {
                tracing::error!("bg compaction panicked: {}", e);
            }
        }
    }
}
