use crate::fragment::Fragment;
use crate::kvs_client::KvsError;
use std::future::Future;
use std::pin::Pin;

/// Trait for uploading media fragments to a streaming service
///
/// This abstraction allows for both production (KVS) and test implementations.
/// The `put_fragment` method handles the full upload protocol internally,
/// including any acknowledgments, and only returns when the fragment is
/// persisted successfully or has failed.
pub trait MediaUploader: Send + Sync + 'static {
    /// Initialize the uploader with stream settings
    /// Called once during sink.start() with access to all properties
    fn initialize(
        &self,
        stream_name: &str,
        region: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>>;

    /// Upload a media fragment, await until Persisted or Failed
    ///
    /// This method handles the full KVS protocol internally:
    /// - Sends fragment to KVS
    /// - Processes acknowledgments (Buffering, Received, Persisted)
    /// - Logs intermediate states for observability
    /// - Returns Ok(()) when Persisted, Err() on failure or timeout
    ///
    /// Takes &Fragment (borrow) to avoid unnecessary clones.
    fn put_fragment<'a>(
        &'a self,
        fragment: &'a Fragment,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + 'a>>;

    /// Check if KVS session is expired (synchronous for convenience)
    /// AWS KVS sessions expire after 45 minutes
    fn is_session_expired(&self) -> bool;

    /// Reset the KVS session (new producer timestamp, reconnect)
    /// Called proactively when session duration limit is reached
    fn reset_session(&self) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>>;
}
