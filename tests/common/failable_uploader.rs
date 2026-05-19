//! Mock MediaUploader for testing network failure scenarios

use anyhow::Result;
use gstrskvssink::advanced::{Fragment, KvsError, MediaUploader};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::debug;

/// Configuration for simulating upload failures
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum FailureConfig {
    /// Always succeed
    AlwaysSucceed,
    /// Fail N times, then succeed forever
    FailTimes(usize),
    /// Custom pattern: true = succeed, false = fail
    FailPattern(Vec<bool>),
}

/// Type of error to simulate
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub enum ErrorType {
    /// Network-related errors (triggers recovery)
    NetworkError,
    /// Non-network errors (should NOT trigger recovery)
    PermissionDenied,
    InvalidCredentials,
    StreamNotFound,
}

impl ErrorType {
    pub fn to_error(&self) -> KvsError {
        match self {
            ErrorType::NetworkError => {
                KvsError::Network("Connection refused: network unreachable".to_string())
            }
            ErrorType::PermissionDenied => {
                KvsError::AwsError("Permission denied: insufficient privileges".to_string())
            }
            ErrorType::InvalidCredentials => {
                KvsError::AwsError("Invalid AWS credentials".to_string())
            }
            ErrorType::StreamNotFound => {
                KvsError::AwsError("Stream not found: test-stream".to_string())
            }
        }
    }
}

/// Mock uploader that can simulate various failure scenarios
pub struct FailableMediaUploader {
    failure_config: FailureConfig,
    error_type: ErrorType,
    upload_count: Arc<AtomicUsize>,
    upload_times: Arc<Mutex<Vec<Instant>>>,
    fragments: Arc<Mutex<Vec<Fragment>>>,
    initialized: Arc<Mutex<bool>>,
}

impl FailableMediaUploader {
    pub fn new() -> Self {
        Self {
            failure_config: FailureConfig::AlwaysSucceed,
            error_type: ErrorType::NetworkError,
            upload_count: Arc::new(AtomicUsize::new(0)),
            upload_times: Arc::new(Mutex::new(Vec::new())),
            fragments: Arc::new(Mutex::new(Vec::new())),
            initialized: Arc::new(Mutex::new(false)),
        }
    }

    /// Configure to fail N times, then succeed
    #[allow(dead_code)]
    pub fn fail_times(mut self, count: usize) -> Self {
        self.failure_config = FailureConfig::FailTimes(count);
        self
    }

    /// Configure with custom failure pattern
    #[allow(dead_code)]
    pub fn fail_pattern(mut self, pattern: Vec<bool>) -> Self {
        self.failure_config = FailureConfig::FailPattern(pattern);
        self
    }

    /// Set the error type to simulate
    #[allow(dead_code)]
    pub fn with_error_type(mut self, error_type: ErrorType) -> Self {
        self.error_type = error_type;
        self
    }

    /// Get the number of upload attempts made
    #[allow(dead_code)]
    pub fn upload_count(&self) -> usize {
        self.upload_count.load(Ordering::SeqCst)
    }

    /// Get the timestamps of all upload attempts
    #[allow(dead_code)]
    pub fn upload_times(&self) -> Vec<Instant> {
        self.upload_times.lock().unwrap().clone()
    }

    /// Get all successfully uploaded fragments
    #[allow(dead_code)]
    pub fn captured_fragments(&self) -> Vec<Fragment> {
        self.fragments.lock().unwrap().clone()
    }

    /// Check if the uploader should fail for this attempt
    fn should_fail(&self, attempt: usize) -> bool {
        match &self.failure_config {
            FailureConfig::AlwaysSucceed => false,
            FailureConfig::FailTimes(count) => attempt < *count,
            FailureConfig::FailPattern(pattern) => {
                if attempt < pattern.len() {
                    !pattern[attempt] // Pattern has true=succeed, we return true=fail
                } else {
                    false // Succeed after pattern exhausted
                }
            }
        }
    }
}

impl Default for FailableMediaUploader {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for FailableMediaUploader {
    fn clone(&self) -> Self {
        Self {
            failure_config: self.failure_config.clone(),
            error_type: self.error_type.clone(),
            upload_count: Arc::clone(&self.upload_count),
            upload_times: Arc::clone(&self.upload_times),
            fragments: Arc::clone(&self.fragments),
            initialized: Arc::clone(&self.initialized),
        }
    }
}

impl MediaUploader for FailableMediaUploader {
    fn initialize(
        &self,
        _stream_name: &str,
        _region: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>> {
        Box::pin(async move {
            debug!("FailableMediaUploader initialized");
            *self.initialized.lock().unwrap() = true;
            Ok(())
        })
    }

    fn put_fragment<'a>(
        &'a self,
        fragment: &'a Fragment,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + 'a>> {
        Box::pin(async move {
            let attempt = self.upload_count.fetch_add(1, Ordering::SeqCst);
            self.upload_times.lock().unwrap().push(Instant::now());

            debug!(
                "FailableMediaUploader: Upload attempt {} for fragment {:?}",
                attempt, fragment.fragment_number
            );

            if self.should_fail(attempt) {
                let error = self.error_type.to_error();
                debug!(
                    "FailableMediaUploader: Simulating failure (attempt {}): {:?}",
                    attempt, error
                );
                return Err(error);
            }

            // Success - store the fragment
            debug!(
                "FailableMediaUploader: Successfully uploaded fragment {:?} (attempt {})",
                fragment.fragment_number, attempt
            );
            self.fragments.lock().unwrap().push(fragment.clone());
            Ok(())
        })
    }

    fn is_session_expired(&self) -> bool {
        // Test uploader doesn't have session expiration
        false
    }

    fn reset_session(&self) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>> {
        Box::pin(async move {
            // Test uploader doesn't need session resets
            debug!("FailableMediaUploader: reset_session called (no-op for tests)");
            Ok(())
        })
    }
}
