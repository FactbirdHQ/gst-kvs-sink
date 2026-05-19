use crate::fragment::Fragment;
use crate::media_uploader::MediaUploader;
use anyhow::{Context, Result};
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{
    SignableBody, SignableRequest, SigningInstructions, SigningSettings, sign,
};
use aws_sigv4::sign::v4::SigningParams;
use aws_smithy_runtime_api::client::identity::Identity;
use backoff::{ExponentialBackoff, backoff::Backoff};
use reqwest::Client;
use serde::Deserialize;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

const DEFAULT_RETENTION_HOURS: i32 = 730; // 30 days
const FRAGMENT_CHANNEL_SIZE: usize = 128;
const ACK_CHANNEL_SIZE: usize = 100;
const STALE_CONNECTION_TIMEOUT_SECS: u64 = 30;
const MAX_UNACKED_FRAGMENTS_WARNING: usize = 50;
const FRAGMENT_STATUS_RETENTION: usize = 150;
const FRAGMENT_STATUS_TIMEOUT_SECS: u64 = 120;

// ============================================================================
// Type Definitions
// ============================================================================

/// KVS-specific error types
#[derive(Debug, thiserror::Error)]
pub enum KvsError {
    #[error("Network error: {0}")]
    Network(String),

    #[error("AWS error: {0}")]
    AwsError(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

/// KVS Acknowledgment types as per AWS documentation
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum AckEventType {
    Buffering,
    Received,
    Persisted,
    Error,
    Idle,
}

/// KVS Acknowledgment structure
/// Note: Despite AWS docs saying "AckEventType", actual field is "EventType"
#[derive(Debug, Clone, Deserialize)]
struct KvsAcknowledgment {
    #[serde(rename = "EventType")]
    event_type: AckEventType,
    #[serde(rename = "FragmentTimecode")]
    #[allow(dead_code)]
    fragment_timecode: Option<u64>,
    #[serde(rename = "FragmentNumber")]
    fragment_number: Option<String>,
    #[serde(rename = "ErrorId")]
    error_id: Option<u32>,
    #[serde(rename = "ErrorCode")]
    error_code: Option<String>,
}

#[derive(Debug, Clone)]
struct FragmentAckStatus {
    fragment_number: String,
    sent_time: Instant,
    buffering_ack: bool,
    received_ack: bool,
    persisted_ack: bool,
    error: Option<String>,
}

/// Public KVS client for uploading video fragments
pub struct KvsClient {
    inner: Arc<Mutex<Option<KvsConnection>>>,
}

/// Internal KVS connection state for persistent streaming
struct KvsConnection {
    stream_name: String,
    region: String,
    producer_start_timestamp: String,

    // Session management (AWS KVS has 45-minute session limit)
    session_start: Instant,
    max_session_duration: Duration,

    // Fragment numbering and header tracking (KVS protocol requirements)
    next_fragment_num: u64,
    first_fragment_of_connection: bool,

    fragment_sender: Option<mpsc::Sender<bytes::Bytes>>,
    connection_monitor: Option<oneshot::Receiver<()>>,
    ack_receiver: Option<mpsc::Receiver<KvsAcknowledgment>>,
    fragment_status: Vec<FragmentAckStatus>,
    last_ack_time: Instant,
    last_idle_time: Option<Instant>,
}

// ============================================================================
// KvsAcknowledgment Implementation
// ============================================================================

impl KvsAcknowledgment {
    fn get_error_description(&self) -> Option<String> {
        match (self.error_id, self.error_code.as_deref()) {
            (Some(4000), _) => {
                Some("STREAM_READ_ERROR - Error reading the data stream".to_string())
            }
            (Some(4001), _) => {
                Some("MAX_FRAGMENT_SIZE_REACHED - Fragment size exceeds 50 MB limit".to_string())
            }
            (Some(4002), _) => {
                Some("MAX_FRAGMENT_DURATION_REACHED - Fragment duration exceeds limit".to_string())
            }
            (Some(4003), _) => Some(
                "MAX_CONNECTION_DURATION_REACHED - Connection duration exceeds limit".to_string(),
            ),
            (Some(4004), _) => {
                Some("FRAGMENT_TIMECODE_LESSER_THAN_PREVIOUS - Fragments out of order".to_string())
            }
            (Some(4006), _) => {
                Some("INVALID_MKV_DATA - Failed to parse input as valid MKV".to_string())
            }
            (Some(4007), _) => {
                Some("INVALID_PRODUCER_TIMESTAMP - Invalid producer timestamp".to_string())
            }
            (Some(4008), _) => Some("STREAM_NOT_ACTIVE - Stream no longer exists".to_string()),
            (Some(4009), _) => Some(
                "FRAGMENT_METADATA_LIMIT_REACHED - Fragment metadata limit reached".to_string(),
            ),
            (Some(4010), _) => {
                Some("TRACK_NUMBER_MISMATCH - Track number mismatch in MKV".to_string())
            }
            (Some(4011), _) => {
                Some("FRAMES_MISSING_FOR_TRACK - Fragment missing frames for track".to_string())
            }
            (Some(5000), _) => Some("INTERNAL_ERROR - Internal service error".to_string()),
            (Some(5001), _) => Some("ARCHIVAL_ERROR - Failed to persist fragments".to_string()),
            (Some(id), Some(code)) => Some(format!("Error {id}: {code}")),
            (Some(id), None) => Some(format!("Error {id}")),
            _ => None,
        }
    }
}

// ============================================================================
// KvsClient Implementation
// ============================================================================

impl KvsClient {
    /// Create an uninitialized KVS client
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }
}

// ============================================================================
// KvsConnection Implementation
// ============================================================================

impl KvsConnection {
    /// Create new connection (does not establish network connection)
    fn new(stream_name: &str, region: &str) -> Self {
        let producer_start_timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .to_string();

        debug!(
            "Producer start timestamp (ABSOLUTE mode): {}",
            producer_start_timestamp
        );

        Self {
            stream_name: stream_name.to_string(),
            region: region.to_string(),
            producer_start_timestamp,
            session_start: Instant::now(), // Track session age
            max_session_duration: Duration::from_secs(40 * 60), // 40 minutes (AWS limit is 45)
            next_fragment_num: 1,          // Always start at 1
            first_fragment_of_connection: true, // First fragment needs headers
            fragment_sender: None,
            connection_monitor: None,
            ack_receiver: None,
            fragment_status: Vec::new(),
            last_ack_time: Instant::now(),
            last_idle_time: None,
        }
    }

    /// Establish connection and return response future
    async fn connect(
        &mut self,
    ) -> Result<impl Future<Output = Result<reqwest::Response, reqwest::Error>> + use<>> {
        // Load AWS config and credentials
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_types::region::Region::new(self.region.clone()))
            .credentials_provider(CredentialsProviderChain::default_provider().await)
            .load()
            .await;
        let credentials_provider = config.credentials_provider().unwrap();

        // Get data endpoint and create stream if needed
        let data_endpoint =
            Self::get_or_create_stream(&self.stream_name, &self.region, &credentials_provider)
                .await?;
        let url = format!("{data_endpoint}/putMedia");

        debug!("Connecting to KVS PutMedia endpoint: {}", url);

        // Sign the request
        let headers = [
            ("x-amzn-stream-name", self.stream_name.as_str()),
            (
                "x-amzn-producer-start-timestamp",
                self.producer_start_timestamp.as_str(),
            ),
            ("x-amzn-fragment-timecode-type", "ABSOLUTE"),
        ];

        let signing_instructions = Self::sign_request(
            &url,
            &self.region,
            &headers,
            SignableBody::Bytes(&[]),
            &credentials_provider,
        )
        .await?;

        // Build request with signed headers
        let client = Client::builder()
            .http1_only()
            .build()
            .context("Failed to create HTTP client")?;

        let mut request_headers = reqwest::header::HeaderMap::new();
        request_headers.insert("transfer-encoding", "chunked".parse().unwrap());

        for (name, value) in signing_instructions
            .headers()
            .chain(headers.iter().copied())
        {
            request_headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
                reqwest::header::HeaderValue::from_str(value)?,
            );
        }

        // Create streaming body with channel
        let (tx, rx) = mpsc::channel::<bytes::Bytes>(FRAGMENT_CHANNEL_SIZE);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok::<_, std::io::Error>);
        let body = reqwest::Body::wrap_stream(stream);

        self.fragment_sender = Some(tx);

        info!("Persistent streaming connection setup complete");
        Ok(client.post(&url).headers(request_headers).body(body).send())
    }

    /// Send fragment over persistent connection
    /// Assigns fragment number and decides whether to include headers
    async fn send_fragment(&mut self, fragment: &Fragment) -> Result<()> {
        self.process_acknowledgments().await;
        self.check_connection_health()?;

        // Assign fragment number
        let fragment_num = self.next_fragment_num;

        // Decide whether to include headers (first fragment of connection)
        let should_include_headers = self.first_fragment_of_connection;

        let header_bytes = if should_include_headers {
            fragment.header_data.len()
        } else {
            0
        };

        debug!(
            "Sending fragment {} ({} bytes = {} header + {} cluster) over persistent connection",
            fragment_num,
            header_bytes + fragment.cluster_data.len(),
            header_bytes,
            fragment.cluster_data.len()
        );

        // Track for acknowledgments
        self.fragment_status.push(FragmentAckStatus {
            fragment_number: fragment_num.to_string(),
            sent_time: Instant::now(),
            buffering_ack: false,
            received_ack: false,
            persisted_ack: false,
            error: None,
        });

        let sender = self
            .fragment_sender
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No active streaming connection"))?;

        // Send headers first (if first fragment of connection)
        if should_include_headers {
            sender
                .send(fragment.header_data.clone())
                .await
                .map_err(|_| anyhow::anyhow!("Failed to send headers - channel closed"))?;
        }

        // Always send cluster data
        sender
            .send(fragment.cluster_data.clone())
            .await
            .map_err(|_| anyhow::anyhow!("Failed to send cluster - channel closed"))?;

        debug!(
            "Fragment {} sent to KVS connection (headers: {})",
            fragment_num,
            if should_include_headers { "yes" } else { "no" }
        );

        // Update state
        self.next_fragment_num += 1;
        self.first_fragment_of_connection = false;

        Ok(())
    }

    /// Check if KVS session has expired (AWS limit is 45 minutes, we use 40 for margin)
    fn is_session_expired(&self) -> bool {
        self.session_start.elapsed() > self.max_session_duration
    }

    /// Reset KVS session with new producer timestamp and reconnect
    /// This is called proactively when approaching the 45-minute AWS session limit
    async fn reset_session_internal(&mut self) -> Result<()> {
        info!("Resetting KVS session - updating producer timestamp and reconnecting");

        // Update producer start timestamp to NOW
        self.producer_start_timestamp = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .to_string();

        // Reset session tracking
        self.session_start = Instant::now();
        self.next_fragment_num = 1;
        self.first_fragment_of_connection = true;
        self.fragment_status.clear();

        info!(
            "New producer start timestamp: {}",
            self.producer_start_timestamp
        );

        // Reconnect with new session
        self.reconnect_with_backoff().await?;

        Ok(())
    }

    /// Reconnect with exponential backoff
    async fn reconnect_with_backoff(&mut self) -> Result<()> {
        let mut backoff = ExponentialBackoff {
            initial_interval: std::time::Duration::from_millis(500),
            max_interval: std::time::Duration::from_secs(30),
            max_elapsed_time: Some(std::time::Duration::from_secs(300)),
            ..Default::default()
        };

        loop {
            match self.reconnect_once().await {
                Ok(()) => {
                    info!("Reconnection successful");
                    return Ok(());
                }
                Err(e) => {
                    warn!("Reconnection attempt failed: {}", e);
                    match backoff.next_backoff() {
                        Some(duration) => {
                            info!("Waiting {:?} before next reconnection attempt", duration);
                            tokio::time::sleep(duration).await;
                        }
                        None => {
                            return Err(anyhow::anyhow!(
                                "Failed to reconnect after exhausting retries: {}",
                                e
                            ));
                        }
                    }
                }
            }
        }
    }

    // ========================================================================
    // Private Helper Methods
    // ========================================================================

    async fn reconnect_once(&mut self) -> Result<()> {
        debug!("Attempting KVS reconnection...");

        // Clean up existing state
        self.fragment_sender = None;
        self.connection_monitor = None;
        self.ack_receiver = None;

        let unacked: Vec<_> = self
            .fragment_status
            .iter()
            .filter(|s| !s.persisted_ack && s.error.is_none())
            .map(|s| s.fragment_number.clone())
            .collect();

        if !unacked.is_empty() {
            warn!(
                "Reconnecting with {} unacknowledged fragments: {:?}",
                unacked.len(),
                unacked
            );
        }

        self.fragment_status.clear();
        self.last_ack_time = Instant::now();
        self.last_idle_time = None;

        // CRITICAL: Reset first_fragment flag (next fragment needs headers)
        self.first_fragment_of_connection = true;
        // NOTE: next_fragment_num continues counting (same session)

        info!(
            "Reconnecting to KVS with same producer start timestamp: {} (next fragment: {})",
            self.producer_start_timestamp, self.next_fragment_num
        );

        // Re-establish connection
        let response_fut = self.connect().await?;

        // Create channels for monitoring
        let (monitor_tx, monitor_rx) = oneshot::channel();
        self.connection_monitor = Some(monitor_rx);

        let (ack_tx, ack_rx) = mpsc::channel(ACK_CHANNEL_SIZE);
        self.ack_receiver = Some(ack_rx);

        tokio::spawn(Self::handle_response(response_fut, ack_tx, monitor_tx));

        Ok(())
    }

    fn check_connection_health(&mut self) -> Result<()> {
        // Check connection monitor
        if let Some(monitor) = &mut self.connection_monitor {
            match monitor.try_recv() {
                Ok(_) | Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(anyhow::anyhow!("Connection closed by server"));
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
        }

        // Check for stale connection
        let elapsed = self.last_ack_time.elapsed().as_secs();
        if elapsed > STALE_CONNECTION_TIMEOUT_SECS {
            if let Some(last_idle) = self.last_idle_time {
                if last_idle.elapsed().as_secs() > STALE_CONNECTION_TIMEOUT_SECS {
                    return Err(anyhow::anyhow!(
                        "Connection appears stale - no acknowledgments for {} seconds",
                        elapsed
                    ));
                }
            } else {
                return Err(anyhow::anyhow!(
                    "Connection appears stale - no acknowledgments for {} seconds",
                    elapsed
                ));
            }
        }

        // Check for too many unacknowledged fragments
        let pending_count = self
            .fragment_status
            .iter()
            .filter(|s| !s.persisted_ack && s.error.is_none())
            .count();

        if pending_count > MAX_UNACKED_FRAGMENTS_WARNING {
            warn!(
                "High number of unacknowledged fragments: {}. KVS may be experiencing issues.",
                pending_count
            );
        }

        Ok(())
    }

    async fn process_acknowledgments(&mut self) {
        let Some(receiver) = &mut self.ack_receiver else {
            return;
        };

        while let Ok(ack) = receiver.try_recv() {
            self.last_ack_time = Instant::now();

            match &ack.event_type {
                AckEventType::Idle => {
                    self.last_idle_time = Some(Instant::now());
                    debug!("Connection is idle but alive");
                }
                _ if ack.fragment_number.is_some() => {
                    let kvs_frag_num = ack.fragment_number.as_ref().unwrap();
                    let oldest_pending =
                        self.fragment_status
                            .iter_mut()
                            .find(|s| match &ack.event_type {
                                AckEventType::Buffering => !s.buffering_ack,
                                AckEventType::Received => s.buffering_ack && !s.received_ack,
                                AckEventType::Persisted => s.received_ack && !s.persisted_ack,
                                AckEventType::Error => s.error.is_none(),
                                _ => false,
                            });

                    if let Some(status) = oldest_pending {
                        match &ack.event_type {
                            AckEventType::Buffering => status.buffering_ack = true,
                            AckEventType::Received => status.received_ack = true,
                            AckEventType::Persisted => {
                                status.persisted_ack = true;
                                debug!(
                                    "Fragment {} (KVS: {}) fully acknowledged",
                                    status.fragment_number, kvs_frag_num
                                );
                            }
                            AckEventType::Error => {
                                status.error = ack.get_error_description();
                                error!(
                                    "Fragment {} (KVS: {}) error: {:?}",
                                    status.fragment_number, kvs_frag_num, status.error
                                );
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        // Clean up old acknowledged fragments
        if self.fragment_status.len() > FRAGMENT_STATUS_RETENTION {
            self.fragment_status.retain(|s| {
                !s.persisted_ack && s.sent_time.elapsed().as_secs() <= FRAGMENT_STATUS_TIMEOUT_SECS
            });
        }
    }

    // ========================================================================
    // Static Helper Methods
    // ========================================================================

    async fn get_or_create_stream(
        stream_name: &str,
        region: &str,
        credentials_provider: &aws_credential_types::provider::SharedCredentialsProvider,
    ) -> Result<String> {
        debug!(
            "Fetching data endpoint for stream: {} in region: {}",
            stream_name, region
        );

        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_types::region::Region::new(region.to_string()))
            .credentials_provider(credentials_provider.clone())
            .load()
            .await;

        let kvs_client = aws_sdk_kinesisvideo::Client::new(&config);

        // Check if stream exists, create if not
        if kvs_client
            .describe_stream()
            .stream_name(stream_name)
            .send()
            .await
            .is_err()
        {
            debug!("Stream {} does not exist, creating it", stream_name);
            kvs_client
                .create_stream()
                .stream_name(stream_name)
                .media_type("video/h264")
                .data_retention_in_hours(DEFAULT_RETENTION_HOURS)
                .send()
                .await
                .context("Failed to create stream")?;
            debug!("Stream {} created successfully", stream_name);
        }

        // Get data endpoint with retry
        let backoff_config = ExponentialBackoff {
            initial_interval: std::time::Duration::from_millis(200),
            max_interval: std::time::Duration::from_secs(10),
            max_elapsed_time: Some(std::time::Duration::from_secs(60)),
            ..Default::default()
        };

        let resp = backoff::future::retry(backoff_config, || async {
            kvs_client
                .get_data_endpoint()
                .stream_name(stream_name)
                .api_name(aws_sdk_kinesisvideo::types::ApiName::PutMedia)
                .send()
                .await
                .map_err(|e| {
                    warn!(
                        "Failed to refresh data endpoint: {}, will retry with backoff",
                        e
                    );
                    backoff::Error::transient(e)
                })
        })
        .await
        .context("Failed to refresh data endpoint after retries")?;

        resp.data_endpoint()
            .context("No data endpoint returned")
            .map(|s| s.to_string())
    }

    async fn sign_request(
        url: &str,
        region: &str,
        headers: &[(&str, &str)],
        body: SignableBody<'_>,
        credentials_provider: &aws_credential_types::provider::SharedCredentialsProvider,
    ) -> Result<SigningInstructions> {
        let aws_credentials = credentials_provider
            .provide_credentials()
            .await
            .context("Failed to get AWS credentials")?;

        let identity = Identity::from(aws_credentials);
        let signing_params = SigningParams::builder()
            .identity(&identity)
            .region(region)
            .name("kinesisvideo")
            .time(SystemTime::now())
            .settings(SigningSettings::default())
            .build()?
            .into();

        let signable_request = SignableRequest::new("POST", url, headers.iter().copied(), body)?;
        let (signing_instructions, _) = sign(signable_request, &signing_params)?.into_parts();

        Ok(signing_instructions)
    }

    /// Process acknowledgment stream buffer and extract complete JSON objects
    fn process_ack_buffer<F>(buffer: &mut String, mut handler: F) -> bool
    where
        F: FnMut(KvsAcknowledgment) -> bool,
    {
        while let Some(newline_pos) = buffer.find('\n') {
            let json_str = buffer.drain(..=newline_pos).collect::<String>();
            let json_str = json_str.trim();

            if !json_str.is_empty() {
                match serde_json::from_str::<KvsAcknowledgment>(json_str) {
                    Ok(ack) => {
                        if !handler(ack) {
                            return false;
                        }
                    }
                    Err(e) => warn!(
                        "Failed to parse KVS acknowledgment: {} (raw: {})",
                        e, json_str
                    ),
                }
            }
        }
        true
    }

    /// Handle response stream for acknowledgments (spawned in background)
    async fn handle_response(
        response_fut: impl Future<Output = Result<reqwest::Response, reqwest::Error>>,
        ack_tx: mpsc::Sender<KvsAcknowledgment>,
        monitor_tx: oneshot::Sender<()>,
    ) {
        match response_fut.await {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("PutMedia request failed: {}", response.status());
                    let _ = monitor_tx.send(());
                    return;
                }

                debug!("PutMedia streaming connection established successfully");

                let mut stream = response.bytes_stream();
                let mut buffer = String::new();

                while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
                    match chunk {
                        Ok(bytes) => {
                            if let Ok(text) = std::str::from_utf8(&bytes) {
                                buffer.push_str(text);

                                Self::process_ack_buffer(&mut buffer, |ack| {
                                    // Log acknowledgment
                                    match &ack.event_type {
                                        AckEventType::Buffering => {
                                            if let Some(frag_num) = &ack.fragment_number {
                                                debug!("Fragment {} BUFFERING", frag_num);
                                            }
                                        }
                                        AckEventType::Received => {
                                            if let Some(frag_num) = &ack.fragment_number {
                                                debug!("Fragment {} RECEIVED", frag_num);
                                            }
                                        }
                                        AckEventType::Persisted => {
                                            if let Some(frag_num) = &ack.fragment_number {
                                                debug!("Fragment {} PERSISTED", frag_num);
                                            }
                                        }
                                        AckEventType::Error => {
                                            let error_desc = ack
                                                .get_error_description()
                                                .unwrap_or_else(|| "Unknown error".to_string());
                                            error!(
                                                "KVS Error for fragment {:?}: {}",
                                                ack.fragment_number, error_desc
                                            );
                                        }
                                        AckEventType::Idle => {
                                            debug!("KVS Idle acknowledgment received")
                                        }
                                    }

                                    // Send through channel
                                    if let Err(e) = ack_tx.try_send(ack) {
                                        // Channel closed is expected during connection transitions
                                        // (e.g., when switching from historical upload to live streaming)
                                        if matches!(e, mpsc::error::TrySendError::Closed(_)) {
                                            debug!(
                                                "Acknowledgment channel closed (connection transition)"
                                            );
                                        } else {
                                            warn!(
                                                "Failed to send acknowledgment to handler: {}",
                                                e
                                            );
                                        }
                                    }

                                    true
                                });
                            }
                        }
                        Err(e) => {
                            error!("Error reading KVS response: {}", e);
                            break;
                        }
                    }
                }
                info!("KVS response stream ended - connection closed by server");
            }
            Err(e) => error!("Failed to establish streaming connection: {}", e),
        }

        let _ = monitor_tx.send(());
    }
}

// ============================================================================
// MediaUploader Trait Implementation
// ============================================================================
impl MediaUploader for KvsClient {
    fn initialize(
        &self,
        stream_name: &str,
        region: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>> {
        let stream_name = stream_name.to_string();
        let region = region.to_string();

        Box::pin(async move {
            debug!(
                "Initializing KVS client for stream: {} in region: {}",
                stream_name, region
            );

            let mut connection = KvsConnection::new(&stream_name, &region);
            let response_fut = connection.connect().await?;

            // Create channels for monitoring
            let (monitor_tx, monitor_rx) = oneshot::channel();
            connection.connection_monitor = Some(monitor_rx);

            let (ack_tx, ack_rx) = mpsc::channel(ACK_CHANNEL_SIZE);
            connection.ack_receiver = Some(ack_rx);

            *self.inner.lock().await = Some(connection);

            tokio::spawn(KvsConnection::handle_response(
                response_fut,
                ack_tx,
                monitor_tx,
            ));

            info!("KVS client initialized successfully");
            Ok(())
        })
    }

    fn put_fragment<'a>(
        &'a self,
        fragment: &'a Fragment,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + 'a>> {
        Box::pin(async move {
            let mut connection_opt = self.inner.lock().await;
            let connection = connection_opt.as_mut().ok_or_else(|| {
                KvsError::Internal(anyhow::anyhow!(
                    "KVS client not initialized - call initialize() first"
                ))
            })?;

            match connection.send_fragment(fragment).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    warn!("Fragment send failed: {}, attempting reconnection", e);
                    connection.reconnect_with_backoff().await?;
                    connection.send_fragment(fragment).await.map_err(|e| {
                        KvsError::Connection(format!(
                            "Failed to send fragment after reconnection: {e}"
                        ))
                    })
                }
            }
        })
    }

    fn is_session_expired(&self) -> bool {
        // Non-blocking check (synchronous)
        // Try to acquire lock without waiting - if we can't get the lock, assume not expired
        if let Ok(connection_opt) = self.inner.try_lock()
            && let Some(connection) = connection_opt.as_ref()
        {
            return connection.is_session_expired();
        }
        false
    }

    fn reset_session(&self) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>> {
        Box::pin(async move {
            let mut connection_opt = self.inner.lock().await;
            let connection = connection_opt
                .as_mut()
                .ok_or_else(|| KvsError::Internal(anyhow::anyhow!("KVS client not initialized")))?;

            connection
                .reset_session_internal()
                .await
                .map_err(|e| KvsError::Connection(format!("Session reset failed: {e}")))?;

            Ok(())
        })
    }
}
