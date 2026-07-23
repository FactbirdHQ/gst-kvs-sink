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
///
/// Cloning shares all state: the sink consumes one clone by value while the
/// test keeps another to drive the gate and observe counters.
#[derive(Clone)]
pub struct GatedUploader {
    inner: Arc<GatedInner>,
}

struct GatedInner {
    gate: watch::Sender<bool>,
    uploaded: Mutex<Vec<Fragment>>,
    put_calls: AtomicUsize,
    reset_calls: AtomicUsize,
    reset_failures_remaining: AtomicUsize,
    expired: AtomicBool,
}

#[allow(dead_code)]
impl GatedUploader {
    pub fn new(gate_open: bool) -> Self {
        Self {
            inner: Arc::new(GatedInner {
                gate: watch::channel(gate_open).0,
                uploaded: Mutex::new(Vec::new()),
                put_calls: AtomicUsize::new(0),
                reset_calls: AtomicUsize::new(0),
                reset_failures_remaining: AtomicUsize::new(0),
                expired: AtomicBool::new(false),
            }),
        }
    }

    pub fn open_gate(&self) {
        let _ = self.inner.gate.send(true);
    }

    pub fn set_expired(&self, expired: bool) {
        self.inner.expired.store(expired, Ordering::SeqCst);
    }

    pub fn session_expired(&self) -> bool {
        self.inner.expired.load(Ordering::SeqCst)
    }

    /// Make the next `count` calls to `reset_session` fail.
    pub fn fail_next_resets(&self, count: usize) {
        self.inner
            .reset_failures_remaining
            .store(count, Ordering::SeqCst);
    }

    pub fn uploaded_count(&self) -> usize {
        self.inner.uploaded.lock().unwrap().len()
    }

    pub fn put_calls(&self) -> usize {
        self.inner.put_calls.load(Ordering::SeqCst)
    }

    pub fn reset_calls(&self) -> usize {
        self.inner.reset_calls.load(Ordering::SeqCst)
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
            self.inner.put_calls.fetch_add(1, Ordering::SeqCst);

            let mut gate_rx = self.inner.gate.subscribe();
            while !*gate_rx.borrow() {
                if gate_rx.changed().await.is_err() {
                    break;
                }
            }

            debug!(
                "GatedUploader: uploaded fragment ({} bytes)",
                fragment.total_size()
            );
            self.inner.uploaded.lock().unwrap().push(fragment.clone());
            Ok(())
        })
    }

    fn is_session_expired(&self) -> bool {
        self.session_expired()
    }

    fn reset_session(&self) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>> {
        Box::pin(async move {
            self.inner.reset_calls.fetch_add(1, Ordering::SeqCst);

            if self
                .inner
                .reset_failures_remaining
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
            self.inner.expired.store(false, Ordering::SeqCst);
            debug!("GatedUploader: reset_session succeeded");
            Ok(())
        })
    }
}
