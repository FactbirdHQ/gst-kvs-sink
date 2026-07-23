//! Mock MediaUploader with a controllable gate on `put_fragment` and scripted
//! session-reset behaviour, for exercising the upload worker.

use gstrskvssink::advanced::{Fragment, KvsError, MediaUploader};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::debug;

/// Uploader whose `put_fragment` blocks until the gate is opened, letting tests
/// stall the upload worker deterministically. Session expiry is test-settable,
/// and `reset_session` can be scripted to fail N times before succeeding; a
/// successful reset clears the expired flag, mirroring `KvsClient` semantics.
#[derive(Clone)]
pub struct GatedUploader {
    gate: Arc<watch::Sender<bool>>,
    uploaded: Arc<Mutex<Vec<Fragment>>>,
    put_calls: Arc<AtomicUsize>,
    reset_calls: Arc<AtomicUsize>,
    reset_failures_remaining: Arc<AtomicUsize>,
    expired: Arc<AtomicBool>,
}

#[allow(dead_code)]
impl GatedUploader {
    pub fn new(gate_open: bool) -> Self {
        Self {
            gate: Arc::new(watch::channel(gate_open).0),
            uploaded: Arc::new(Mutex::new(Vec::new())),
            put_calls: Arc::new(AtomicUsize::new(0)),
            reset_calls: Arc::new(AtomicUsize::new(0)),
            reset_failures_remaining: Arc::new(AtomicUsize::new(0)),
            expired: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn open_gate(&self) {
        let _ = self.gate.send(true);
    }

    pub fn set_expired(&self, expired: bool) {
        self.expired.store(expired, Ordering::SeqCst);
    }

    pub fn session_expired(&self) -> bool {
        self.expired.load(Ordering::SeqCst)
    }

    /// Make the next `count` calls to `reset_session` fail.
    pub fn fail_next_resets(&self, count: usize) {
        self.reset_failures_remaining.store(count, Ordering::SeqCst);
    }

    pub fn uploaded_count(&self) -> usize {
        self.uploaded.lock().unwrap().len()
    }

    pub fn put_calls(&self) -> usize {
        self.put_calls.load(Ordering::SeqCst)
    }

    pub fn reset_calls(&self) -> usize {
        self.reset_calls.load(Ordering::SeqCst)
    }

    /// Poll `pred` until it holds or `timeout` elapses; returns whether it held.
    pub fn wait_until(&self, timeout: Duration, pred: impl Fn(&Self) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pred(self) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        pred(self)
    }
}

impl MediaUploader for GatedUploader {
    fn initialize(
        &self,
        _stream_name: &str,
        _region: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn put_fragment<'a>(
        &'a self,
        fragment: &'a Fragment,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + 'a>> {
        Box::pin(async move {
            self.put_calls.fetch_add(1, Ordering::SeqCst);

            let mut gate_rx = self.gate.subscribe();
            while !*gate_rx.borrow() {
                if gate_rx.changed().await.is_err() {
                    break;
                }
            }

            debug!(
                "GatedUploader: uploaded fragment ({} bytes)",
                fragment.total_size()
            );
            self.uploaded.lock().unwrap().push(fragment.clone());
            Ok(())
        })
    }

    fn is_session_expired(&self) -> bool {
        self.expired.load(Ordering::SeqCst)
    }

    fn reset_session(&self) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>> {
        Box::pin(async move {
            self.reset_calls.fetch_add(1, Ordering::SeqCst);

            let failures = &self.reset_failures_remaining;
            if failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                debug!("GatedUploader: simulating reset_session failure");
                return Err(KvsError::Connection("simulated reset failure".to_string()));
            }

            // Like KvsClient, the session only stops reporting expired once a
            // reset actually succeeds.
            self.expired.store(false, Ordering::SeqCst);
            debug!("GatedUploader: reset_session succeeded");
            Ok(())
        })
    }
}
