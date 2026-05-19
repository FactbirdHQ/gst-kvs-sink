use anyhow::Result;
use gstreamer::glib;
use gstreamer::glib::clone::Downgrade;
use gstreamer::prelude::{ElementExt, StaticType, ToValue};
use gstreamer::subclass::prelude::*;
use gstreamer_base::subclass::prelude::*;
use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info};

use super::properties::{Properties, Settings};
use crate::buffered_upload_manager::{BufferManagerEvent, BufferStats, BufferedUploadManager};
use crate::kvs_client::KvsClient;
use crate::media_uploader::MediaUploader;
use crate::mkv_writer::MkvWriter;

/// Network recovery retry intervals
const INITIAL_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// Upload mode controlling fragment handling behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadMode {
    /// Immediate mode: live streaming with 2s clusters, direct upload to KVS
    Immediate,
    /// BufferOnly mode: buffering with 15s clusters, disk storage only
    BufferOnly,
}

pub struct KvsSink {
    settings: Mutex<Settings>,
    state: Mutex<State>,
    /// Media uploader wrapped in Arc for sharing with async tasks
    /// RwLock allows rare writes during initialization, frequent reads during operation
    media_uploader: RwLock<Arc<AsyncMutex<Box<dyn MediaUploader>>>>,
}

struct State {
    mkv_writer: Option<MkvWriter>,
    current_fragment_start_pts: Option<gstreamer::ClockTime>,
    /// True when we need to wait for a keyframe to start a new fragment
    /// (either at stream start or after MKV writer reset)
    waiting_for_keyframe: bool,
    /// Collect frames for current fragment (per-fragment approach)
    current_fragment_frames: Vec<gstreamer::Buffer>,
    /// Buffered upload manager (wrapped for async task sharing)
    buffer_manager: Option<Arc<AsyncMutex<BufferedUploadManager>>>,
    /// Current upload mode (controls fragment duration and upload behavior)
    current_mode: UploadMode,
    /// Event receiver for buffer manager notifications
    buffer_event_rx: Option<tokio::sync::mpsc::UnboundedReceiver<BufferManagerEvent>>,
    /// Background recovery task handle
    recovery_task: Option<tokio::task::JoinHandle<()>>,
    /// Count of consecutive network failures (for exponential backoff)
    consecutive_failures: u32,
    /// Timestamp of last recovery attempt (to prevent duplicate tasks)
    last_recovery_attempt: Option<Instant>,
    /// Flag to trigger session reset at next keyframe (set by mode switch)
    pending_session_reset: bool,
    /// Pending mode switch to execute at next keyframe boundary (prevents frame loss)
    pending_mode_switch: Option<UploadMode>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            mkv_writer: None,
            current_fragment_start_pts: None,
            waiting_for_keyframe: false,
            current_fragment_frames: Vec::new(),
            buffer_manager: None,
            current_mode: UploadMode::BufferOnly, // Safe default
            buffer_event_rx: None,
            recovery_task: None,
            consecutive_failures: 0,
            last_recovery_attempt: None,
            pending_session_reset: false,
            pending_mode_switch: None,
        }
    }
}

impl KvsSink {
    /// Get or create a Tokio runtime handle for async operations
    fn ensure_runtime(&self) -> Handle {
        // Always prefer external runtime if available (async context)
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return handle;
        }

        // Fall back to static runtime for cdylib case
        use std::sync::OnceLock;
        static CDYLIB_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

        let runtime = CDYLIB_RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_name("kvs-sink-cdylib")
                .enable_all()
                .build()
                .expect("Failed to create Tokio runtime for cdylib")
        });

        runtime.handle().clone()
    }
}

impl KvsSink {
    pub fn new(uploader: impl MediaUploader + 'static) -> Self {
        Self {
            settings: Mutex::new(Settings::default()),
            state: Mutex::new(State::default()),
            media_uploader: RwLock::new(Arc::new(AsyncMutex::new(Box::new(uploader)))),
        }
    }

    /// Replace the media uploader (for dependency injection during initialization)
    /// This should only be called during construction, never after the element is in use
    pub fn set_uploader(&self, uploader: impl MediaUploader + 'static) {
        let new_uploader = Arc::new(AsyncMutex::new(Box::new(uploader) as Box<dyn MediaUploader>));
        *self.media_uploader.write().unwrap() = new_uploader;
    }

    /// Trigger upload of all buffered fragments (blocking upload, then conditionally switch mode)
    ///
    /// Mode is determined by BufferedUploadManager based on time range semantics
    pub async fn trigger_upload(&self) -> Result<(), gstreamer::ErrorMessage> {
        let buffer_mgr = match self.get_buffer_manager() {
            Some(mgr) => mgr,
            None => {
                gstreamer::warning!(
                    CAT,
                    imp = self,
                    "Buffer manager not initialized, cannot trigger upload"
                );
                return Ok(());
            }
        };

        gstreamer::info!(
            CAT,
            imp = self,
            "Trigger upload: uploading all buffered data"
        );

        // Trigger upload (BLOCKING - uploads all historical data in chronological order)
        // During this time, new fragments buffer to a NEW mega-cluster file
        // BufferedUploadManager will notify us of the target mode via event
        buffer_mgr
            .lock()
            .await
            .trigger_upload_all(None, None)
            .await
            .map_err(|e| {
                gstreamer::error_msg!(
                    gstreamer::ResourceError::Write,
                    ["Failed to trigger upload: {}", e]
                )
            })?;

        gstreamer::info!(
            CAT,
            imp = self,
            "Upload trigger completed, processing events"
        );

        // Process buffer manager events (will conditionally switch mode based on event)
        self.process_buffer_events().await;

        Ok(())
    }

    /// Get buffer statistics
    pub fn buffer_stats(&self) -> Option<BufferStats> {
        let state = self.state.lock().unwrap();
        state.buffer_manager.as_ref().and_then(|mgr| {
            // Use try_lock since we're in sync context
            mgr.try_lock().ok().map(|m| m.stats())
        })
    }
}

impl Default for KvsSink {
    fn default() -> Self {
        Self::new(KvsClient::new())
    }
}

#[glib::object_subclass]
impl ObjectSubclass for KvsSink {
    const NAME: &'static str = "GstRsKvsSink";
    type Type = super::KvsSink;
    type ParentType = gstreamer_base::BaseSink;
}

impl ObjectImpl for KvsSink {
    fn properties() -> &'static [glib::ParamSpec] {
        Properties::properties()
    }

    fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
        let mut settings = self.settings.lock().unwrap();
        Properties::set_property(&mut settings, value, pspec);
    }

    fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        let settings = self.settings.lock().unwrap();
        Properties::get_property(&settings, pspec)
    }

    fn signals() -> &'static [glib::subclass::Signal] {
        static SIGNALS: Lazy<Vec<glib::subclass::Signal>> = Lazy::new(|| {
            vec![glib::subclass::Signal::builder("trigger-upload")
                .param_types([
                    Option::<String>::static_type(), // start timestamp
                    Option::<String>::static_type(), // end timestamp
                ])
                .return_type::<bool>()
                .action()
                .class_handler(|args| {
                    // Extract the element instance (args[0] is the object)
                    let element = match args[0].get::<super::KvsSink>() {
                        Ok(e) => e,
                        Err(_) => return Some(false.to_value()),
                    };
                    let imp = element.imp();

                    // Extract start/end timestamp arguments
                    let start = args
                        .get(1)
                        .and_then(|arg| arg.get::<Option<String>>().ok().flatten());

                    let end = args
                        .get(2)
                        .and_then(|arg| arg.get::<Option<String>>().ok().flatten());

                    gstreamer::info!(
                        CAT,
                        imp = imp,
                        "Received trigger-upload action (start={:?}, end={:?})",
                        start,
                        end
                    );

                    // STEP 1: SYNCHRONOUS VALIDATION
                    // Check if buffer_manager is available
                    let buffer_manager = match imp.get_buffer_manager() {
                        Some(bm) => bm,
                        None => {
                            gstreamer::warning!(
                                CAT,
                                imp = imp,
                                "Trigger rejected: buffer_manager not initialized (sink not ready)"
                            );
                            return Some(false.to_value());
                        }
                    };

                    // Parse timestamps if provided
                    let start_time = start.and_then(|s| {
                        chrono::DateTime::parse_from_rfc3339(&s)
                            .ok()
                            .map(|dt| dt.with_timezone(&chrono::Utc))
                    });

                    let end_time = end.and_then(|s| {
                        chrono::DateTime::parse_from_rfc3339(&s)
                            .ok()
                            .map(|dt| dt.with_timezone(&chrono::Utc))
                    });

                    // STEP 2: SPAWN ASYNC TASK
                    let element_weak = element.downgrade();
                    imp.ensure_runtime().spawn(async move {
                        let element = match element_weak.upgrade() {
                            Some(e) => e,
                            None => {
                                // Element was dropped, nothing to do
                                return;
                            }
                        };

                        let imp = element.imp();

                        gstreamer::info!(
                            CAT,
                            imp = imp,
                            "Starting upload trigger task (start={:?}, end={:?})",
                            start_time,
                            end_time
                        );

                        // Convert chrono DateTime to SystemTime
                        let start_systime = start_time.map(std::time::SystemTime::from);
                        let end_systime = end_time.map(std::time::SystemTime::from);

                        // Trigger the upload with time range
                        match buffer_manager
                            .lock()
                            .await
                            .trigger_upload_all(start_systime, end_systime)
                            .await
                        {
                            Ok(()) => {
                                gstreamer::info!(
                                    CAT,
                                    imp = imp,
                                    "Upload trigger completed successfully"
                                );

                                // Process buffer manager events to switch mode if needed
                                imp.process_buffer_events().await;

                                // Post success message to bus
                                let _ = element.post_message(
                                    gstreamer::message::Application::builder(
                                        gstreamer::Structure::builder("upload-trigger-success")
                                            .field("start", start_time.map(|t| t.to_rfc3339()))
                                            .field("end", end_time.map(|t| t.to_rfc3339()))
                                            .build(),
                                    )
                                    .build(),
                                );
                            }
                            Err(e) => {
                                gstreamer::error!(CAT, imp = imp, "Upload trigger failed: {}", e);

                                // Post error message to bus
                                let _ = element.post_message(
                                    gstreamer::message::Application::builder(
                                        gstreamer::Structure::builder("upload-trigger-error")
                                            .field("error", e.to_string())
                                            .field("start", start_time.map(|t| t.to_rfc3339()))
                                            .field("end", end_time.map(|t| t.to_rfc3339()))
                                            .build(),
                                    )
                                    .build(),
                                );
                            }
                        }
                    });

                    // STEP 3: RETURN TRUE (accepted)
                    gstreamer::info!(CAT, imp = imp, "Trigger accepted, upload task spawned");
                    Some(true.to_value())
                })
                .build()]
        });
        SIGNALS.as_ref()
    }

    fn constructed(&self) {
        self.parent_constructed();
    }
}

impl GstObjectImpl for KvsSink {}

impl ElementImpl for KvsSink {
    fn metadata() -> Option<&'static gstreamer::subclass::ElementMetadata> {
        static ELEMENT_METADATA: Lazy<gstreamer::subclass::ElementMetadata> = Lazy::new(|| {
            gstreamer::subclass::ElementMetadata::new(
                "KVS Sink",
                "Sink/Network/Video",
                "Streams H.264 video to AWS Kinesis Video Streams via fragmented MKV PutMedia",
                env!("CARGO_PKG_AUTHORS"),
            )
        });
        Some(&*ELEMENT_METADATA)
    }

    fn change_state(
        &self,
        transition: gstreamer::StateChange,
    ) -> Result<gstreamer::StateChangeSuccess, gstreamer::StateChangeError> {
        if transition == gstreamer::StateChange::ReadyToPaused {
            let buffer_directory_set = {
                let settings = self.settings.lock().unwrap();
                settings
                    .buffer_directory
                    .as_deref()
                    .map(|s| !s.is_empty())
                    .unwrap_or(false)
            };
            if !buffer_directory_set {
                gstreamer::element_imp_error!(
                    self,
                    gstreamer::ResourceError::Settings,
                    ["buffer-directory property is required and was not set"]
                );
                return Err(gstreamer::StateChangeError);
            }
        }
        self.parent_change_state(transition)
    }

    fn pad_templates() -> &'static [gstreamer::PadTemplate] {
        static PAD_TEMPLATES: Lazy<Vec<gstreamer::PadTemplate>> = Lazy::new(|| {
            let caps = gstreamer::Caps::builder("video/x-h264")
                .field("stream-format", "avc")
                .field("alignment", "au")
                .build();

            let sink_pad_template = gstreamer::PadTemplate::new(
                "sink",
                gstreamer::PadDirection::Sink,
                gstreamer::PadPresence::Always,
                &caps,
            )
            .unwrap();

            vec![sink_pad_template]
        });

        PAD_TEMPLATES.as_ref()
    }
}

impl BaseSinkImpl for KvsSink {
    fn start(&self) -> Result<(), gstreamer::ErrorMessage> {
        gstreamer::info!(CAT, imp = self, "Starting KVS sink");
        let settings = self.settings.lock().unwrap();
        let mut state = self.state.lock().unwrap();

        if settings.stream_name.is_empty() {
            return Err(gstreamer::error_msg!(
                gstreamer::ResourceError::Settings,
                ["Stream name not set"]
            ));
        }

        // Initialize the injected media uploader
        tokio::task::block_in_place(|| {
            self.ensure_runtime().block_on(async {
                {
                    let uploader_arc = Arc::clone(&self.media_uploader.read().unwrap());
                    let uploader = uploader_arc.lock().await;
                    uploader
                        .initialize(&settings.stream_name, &settings.region)
                        .await
                }
            })
        })
        .map_err(|e| {
            gstreamer::error_msg!(
                gstreamer::ResourceError::OpenWrite,
                ["Failed to initialize media uploader: {}", e]
            )
        })?;

        // Initialize MKV writer
        let mkv_writer = MkvWriter::new();

        // Determine initial upload mode from settings
        let initial_mode = if settings.mode == "continuous" {
            UploadMode::Immediate
        } else {
            UploadMode::BufferOnly
        };

        // Create event channel for buffer manager notifications
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

        // Initialize buffered upload manager. buffer_directory is required and
        // is verified during the Ready->Paused state-change guard.
        let buffer_dir = settings
            .buffer_directory
            .as_deref()
            .map(std::path::PathBuf::from)
            .ok_or_else(|| {
                gstreamer::error_msg!(
                    gstreamer::ResourceError::Settings,
                    ["buffer-directory property is required"]
                )
            })?
            .join(&settings.stream_name);

        let uploader_arc = Arc::clone(&self.media_uploader.read().unwrap());
        let mut buffer_manager = BufferedUploadManager::new(
            settings.stream_name.clone(),
            settings.region.clone(),
            buffer_dir,
            std::time::Duration::from_secs(settings.max_buffer_hours * 3600),
            uploader_arc,
        )
        .map_err(|e| {
            gstreamer::error_msg!(
                gstreamer::ResourceError::OpenWrite,
                ["Failed to create buffered upload manager: {}", e]
            )
        })?;

        // Set event channel for upload completion notifications
        buffer_manager.set_event_channel(event_tx);

        state.mkv_writer = Some(mkv_writer);
        state.buffer_manager = Some(Arc::new(AsyncMutex::new(buffer_manager)));
        state.waiting_for_keyframe = true;
        state.current_mode = initial_mode;
        state.buffer_event_rx = Some(event_rx);

        // Release locks before calling update_fragment_duration
        drop(state);
        drop(settings);

        // Set initial fragment duration based on mode
        if let Err(e) = self.update_fragment_duration(initial_mode) {
            gstreamer::warning!(
                CAT,
                imp = self,
                "Failed to set initial fragment duration: {:?}",
                e
            );
        }

        let settings = self.settings.lock().unwrap();
        gstreamer::info!(
            CAT,
            imp = self,
            "Started KVS sink for stream: {} in {:?} mode",
            settings.stream_name,
            initial_mode
        );
        Ok(())
    }

    fn stop(&self) -> Result<(), gstreamer::ErrorMessage> {
        // Cancel recovery task if running
        if self.cancel_recovery_task() {
            gstreamer::info!(CAT, imp = self, "Cancelled recovery task on stop");
        }

        let mut state = self.state.lock().unwrap();

        // Note: Pending fragments are flushed in the EOS event handler before stop is called

        *state = State::default();

        gstreamer::info!(CAT, imp = self, "Stopped KVS sink");
        Ok(())
    }

    fn render(
        &self,
        buffer: &gstreamer::Buffer,
    ) -> Result<gstreamer::FlowSuccess, gstreamer::FlowError> {
        // Block on async operations using the cached runtime
        self.ensure_runtime()
            .block_on(async { self.render_async(buffer).await })
    }

    fn set_caps(&self, caps: &gstreamer::Caps) -> Result<(), gstreamer::LoggableError> {
        gstreamer::info!(CAT, imp = self, "Setting caps: {}", caps);

        // Extract video properties once from caps
        let (width, height, codec_data) = if let Some(s) = caps.structure(0) {
            let width = s.get::<i32>("width").unwrap_or(1920) as u32;
            let height = s.get::<i32>("height").unwrap_or(1080) as u32;

            // Extract codec_data (SPS/PPS for H.264)
            let codec_data = s
                .get::<gstreamer::Buffer>("codec_data")
                .ok()
                .and_then(|buf| buf.map_readable().ok().map(|map| map.as_slice().to_vec()));

            (width, height, codec_data)
        } else {
            gstreamer::warning!(CAT, imp = self, "No structure in caps, using defaults");
            (1920, 1080, None)
        };

        let mut state = self.state.lock().unwrap();

        // Update MKV writer with codec metadata
        if let Some(writer) = state.mkv_writer.as_mut() {
            writer.set_codec_metadata(width, height, codec_data.clone());
        } else {
            gstreamer::warning!(
                CAT,
                imp = self,
                "MKV writer not yet initialized, caps will be set later"
            );
        }

        self.parent_set_caps(caps)
    }

    fn event(&self, event: gstreamer::Event) -> bool {
        use gstreamer::EventView;

        match event.view() {
            EventView::Eos(_) => {
                gstreamer::info!(CAT, imp = self, "Received EOS, flushing pending fragment");

                // Flush any pending fragment before EOS completes
                let settings = self.settings.lock().unwrap().clone();
                drop(settings); // Release lock before async operations

                let result = self
                    .ensure_runtime()
                    .block_on(async { self.finalize_and_send_current_fragment().await });

                if let Err(e) = result {
                    gstreamer::warning!(
                        CAT,
                        imp = self,
                        "Failed to flush pending fragment on EOS: {:?}",
                        e
                    );
                }

                // Pass EOS event upstream
                self.parent_event(event)
            }
            _ => {
                // Pass all other events to parent
                self.parent_event(event)
            }
        }
    }
}

impl KvsSink {
    async fn render_async(
        &self,
        buffer: &gstreamer::Buffer,
    ) -> Result<gstreamer::FlowSuccess, gstreamer::FlowError> {
        let settings = self.settings.lock().unwrap().clone();
        let is_keyframe = !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT);

        // Wait for keyframe to start streaming
        {
            let mut state = self.state.lock().unwrap();
            if state.waiting_for_keyframe {
                if !is_keyframe {
                    return Ok(gstreamer::FlowSuccess::Ok); // Drop non-keyframes
                }
                // Got keyframe - start first fragment
                state.current_fragment_start_pts = buffer.pts();
                state.waiting_for_keyframe = false;
                state.current_fragment_frames.clear();
                debug!("Starting fragment 1 with keyframe");
            }
        }

        // Check if we should finalize current fragment and start new one
        // Also check for pending mode switch, session expiry, or pending reset (at keyframe boundaries)
        let (should_fragment, pending_new_mode, needs_reset) = {
            let state = self.state.lock().unwrap();
            let should_frag =
                is_keyframe && self.should_fragment_on_duration(&state, buffer, &settings);
            let sess_expired = self.is_kvs_session_expired();
            let pending_reset = state.pending_session_reset;
            let pending_mode = state.pending_mode_switch;
            (should_frag, pending_mode, sess_expired || pending_reset)
        };

        if should_fragment {
            // Finalize and send current fragment FIRST (using current mode, before switching)
            self.finalize_and_send_current_fragment().await?;

            // Handle pending mode switch at this keyframe boundary (no frame loss!)
            if let Some(new_mode) = pending_new_mode {
                gstreamer::info!(
                    CAT,
                    imp = self,
                    "Executing pending mode switch to {:?} at keyframe boundary",
                    new_mode
                );

                // Update mode and clear pending flag
                {
                    let mut state = self.state.lock().unwrap();
                    state.current_mode = new_mode;
                    state.pending_mode_switch = None;
                }

                // Update fragment duration for new mode
                if let Err(e) = self.update_fragment_duration(new_mode) {
                    gstreamer::warning!(
                        CAT,
                        imp = self,
                        "Failed to update fragment duration: {:?}",
                        e
                    );
                }

                // If switching to Immediate mode, reset session (already at keyframe boundary!)
                if new_mode == UploadMode::Immediate {
                    gstreamer::info!(
                        CAT,
                        imp = self,
                        "Resetting session for Immediate mode transition"
                    );
                    self.reset_session_at_keyframe(
                        buffer.pts().unwrap_or(gstreamer::ClockTime::ZERO),
                    )
                    .await?;
                }
            } else if needs_reset {
                // Session expired or reset pending (not from mode switch)
                gstreamer::info!(
                    CAT,
                    imp = self,
                    "Resetting session at keyframe boundary (proactive - no frame loss)"
                );
                self.reset_session_at_keyframe(buffer.pts().unwrap_or(gstreamer::ClockTime::ZERO))
                    .await?;

                // Clear the pending reset flag
                {
                    let mut state = self.state.lock().unwrap();
                    state.pending_session_reset = false;
                }
            }

            // Start new fragment with this keyframe
            {
                let mut state = self.state.lock().unwrap();
                state.current_fragment_start_pts = buffer.pts();
                state.current_fragment_frames.clear();
                debug!("Starting fragment with keyframe");
            }
        }

        // Add frame to current fragment
        {
            let mut state = self.state.lock().unwrap();
            state.current_fragment_frames.push(buffer.clone());
        }

        Ok(gstreamer::FlowSuccess::Ok)
    }

    /// Get buffer manager reference (cloned Arc for async usage)
    fn get_buffer_manager(&self) -> Option<Arc<AsyncMutex<BufferedUploadManager>>> {
        let state = self.state.lock().unwrap();
        state.buffer_manager.clone()
    }

    /// Cancel recovery task if running and return true if a task was cancelled
    fn cancel_recovery_task(&self) -> bool {
        let task_to_abort = {
            let mut state = self.state.lock().unwrap();
            state.recovery_task.take()
        };

        if let Some(task) = task_to_abort {
            task.abort();
            true
        } else {
            false
        }
    }

    /// Check if KVS session is expired (proactive check from kvs_client)
    fn is_kvs_session_expired(&self) -> bool {
        // Clone the Arc to avoid lifetime issues
        let uploader_arc = self.media_uploader.read().unwrap().clone();

        // Synchronous check - no async needed!
        // Try non-blocking lock, if can't acquire assume not expired
        // Save result to variable to drop guard before returning
        uploader_arc
            .try_lock()
            .map(|kvs| kvs.is_session_expired())
            .unwrap_or(false)
    }

    /// Reset KVS session at keyframe boundary (proactive approach - no frame loss)
    async fn reset_session_at_keyframe(
        &self,
        current_pts: gstreamer::ClockTime,
    ) -> Result<(), gstreamer::FlowError> {
        gstreamer::info!(
            CAT,
            imp = self,
            "Resetting KVS session at keyframe boundary (proactive - no frame loss)"
        );

        // Reset MKV writer first (updates session_start_wall_time)
        {
            let mut state = self.state.lock().unwrap();
            if let Some(writer) = state.mkv_writer.as_mut() {
                writer.reset_for_new_session(current_pts);
            }
        }

        // Reset KVS session (new producer timestamp, reconnect)
        let uploader_arc = self.media_uploader.read().unwrap().clone();
        uploader_arc
            .lock()
            .await
            .reset_session()
            .await
            .map_err(|e| {
                gstreamer::error!(CAT, imp = self, "Failed to reset KVS session: {}", e);
                gstreamer::FlowError::Error
            })?;

        gstreamer::info!(CAT, imp = self, "KVS session reset complete");
        Ok(())
    }

    /// Check if we should fragment based on duration
    fn should_fragment_on_duration(
        &self,
        state: &State,
        buffer: &gstreamer::Buffer,
        settings: &Settings,
    ) -> bool {
        match (state.current_fragment_start_pts, buffer.pts()) {
            (Some(fragment_start), Some(current_pts)) => {
                current_pts.saturating_sub(fragment_start) >= settings.fragment_duration
            }
            _ => false,
        }
    }

    /// Finalize current fragment and send it (upload or buffer based on current mode)
    async fn finalize_and_send_current_fragment(&self) -> Result<(), gstreamer::FlowError> {
        let (frames, current_mode) = {
            let state = self.state.lock().unwrap();
            if state.current_fragment_frames.is_empty() {
                return Ok(()); // No frames to send
            }
            (state.current_fragment_frames.clone(), state.current_mode)
        };

        // Generate fragment using per-fragment approach
        let fragment = {
            let mut state = self.state.lock().unwrap();
            let writer = state.mkv_writer.as_mut().ok_or_else(|| {
                gstreamer::error!(CAT, imp = self, "MKV writer not initialized");
                gstreamer::FlowError::Error
            })?;

            let frame_refs: Vec<&gstreamer::Buffer> = frames.iter().collect();
            debug!(
                "Finalizing fragment with {} frames (mode: {:?})",
                frame_refs.len(),
                current_mode
            );
            writer.finalize_fragment(&frame_refs).map_err(|e| {
                gstreamer::error!(CAT, imp = self, "Failed to finalize fragment: {}", e);
                gstreamer::FlowError::Error
            })?
        };

        let fragment_size = fragment.total_size();

        // Get buffer manager reference
        let buffer_mgr = self.get_buffer_manager().ok_or_else(|| {
            gstreamer::error!(CAT, imp = self, "Buffer manager not initialized");
            gstreamer::FlowError::Error
        })?;

        // Handle fragment based on current mode
        match current_mode {
            UploadMode::Immediate => {
                // Try direct upload
                let uploader_arc = self.media_uploader.read().unwrap().clone();
                let uploader = uploader_arc.lock().await;
                match uploader.put_fragment(&fragment).await {
                    Ok(()) => {
                        debug!("Fragment uploaded directly ({} bytes)", fragment_size);
                        Ok(())
                    }
                    Err(e) if Self::is_network_error_str(&e.to_string()) => {
                        // Network failure - switch to BufferOnly mode
                        gstreamer::warning!(
                            CAT,
                            imp = self,
                            "Network failure detected: {} - switching to BufferOnly mode",
                            e
                        );

                        self.handle_network_failure().await?;

                        // Buffer this fragment
                        buffer_mgr
                            .lock()
                            .await
                            .push_fragment(fragment)
                            .await
                            .map_err(|e| {
                                gstreamer::error!(
                                    CAT,
                                    imp = self,
                                    "Failed to push fragment to buffer: {}",
                                    e
                                );
                                gstreamer::FlowError::Error
                            })?;

                        info!(
                            "Buffered fragment ({} bytes) due to network failure",
                            fragment_size
                        );
                        Ok(())
                    }
                    Err(e) => {
                        gstreamer::error!(CAT, imp = self, "Failed to upload fragment: {}", e);
                        Err(gstreamer::FlowError::Error)
                    }
                }
            }
            UploadMode::BufferOnly => {
                // Always buffer
                buffer_mgr
                    .lock()
                    .await
                    .push_fragment(fragment)
                    .await
                    .map_err(|e| {
                        gstreamer::error!(
                            CAT,
                            imp = self,
                            "Failed to push fragment to buffer: {}",
                            e
                        );
                        gstreamer::FlowError::Error
                    })?;

                debug!(
                    "Buffered fragment ({} bytes) in BufferOnly mode",
                    fragment_size
                );
                Ok(())
            }
        }
    }

    /// Check if error string indicates a network-related issue
    fn is_network_error_str(error_str: &str) -> bool {
        let error_lower = error_str.to_lowercase();
        error_lower.contains("connection")
            || error_lower.contains("network")
            || error_lower.contains("timeout")
            || error_lower.contains("dns")
            || error_lower.contains("resolve")
    }

    /// Handle network failure by switching to BufferOnly mode and scheduling recovery attempts
    async fn handle_network_failure(&self) -> Result<(), gstreamer::FlowError> {
        gstreamer::info!(
            CAT,
            imp = self,
            "Switching to BufferOnly mode due to network failure"
        );

        // Check if recovery task is already running
        {
            let state = self.state.lock().unwrap();
            if state.recovery_task.is_some() {
                gstreamer::debug!(
                    CAT,
                    imp = self,
                    "Recovery task already running, skipping spawn"
                );
                return Ok(());
            }
        }

        // Update state to BufferOnly mode
        {
            let mut state = self.state.lock().unwrap();
            state.current_mode = UploadMode::BufferOnly;
            state.consecutive_failures += 1;
            state.last_recovery_attempt = Some(Instant::now());
        }

        // Update fragment duration for buffering
        if let Err(e) = self.update_fragment_duration(UploadMode::BufferOnly) {
            gstreamer::warning!(
                CAT,
                imp = self,
                "Failed to update fragment duration: {:?}",
                e
            );
        }

        // Get buffer manager reference for recovery task
        let buffer_mgr = match self.get_buffer_manager() {
            Some(mgr) => mgr,
            None => {
                gstreamer::warning!(
                    CAT,
                    imp = self,
                    "Cannot schedule recovery: buffer manager not initialized"
                );
                return Ok(());
            }
        };

        // Spawn recovery task
        let element_weak = self.obj().downgrade();

        let task_handle = self.ensure_runtime().spawn(async move {
            gstreamer::info!(CAT, "Network recovery task started");

            loop {
                // Calculate backoff delay based on consecutive failures
                let delay = {
                    let element = match element_weak.upgrade() {
                        Some(e) => e,
                        None => {
                            gstreamer::debug!(CAT, "Element dropped, stopping recovery task");
                            return;
                        }
                    };
                    let imp = element.imp();
                    let state = imp.state.lock().unwrap();
                    let failures = state.consecutive_failures;

                    // Exponential backoff: min(INITIAL * 2^failures, MAX)
                    let backoff_secs =
                        INITIAL_RETRY_INTERVAL.as_secs() * (2u64.pow(failures.saturating_sub(1)));
                    let delay = Duration::from_secs(backoff_secs.min(MAX_RETRY_INTERVAL.as_secs()));

                    gstreamer::info!(
                        CAT,
                        "Recovery attempt scheduled in {}s (failures: {})",
                        delay.as_secs(),
                        failures
                    );
                    delay
                };

                tokio::time::sleep(delay).await;

                let element = match element_weak.upgrade() {
                    Some(e) => e,
                    None => {
                        gstreamer::debug!(
                            CAT,
                            "Element dropped during sleep, stopping recovery task"
                        );
                        return;
                    }
                };
                let imp = element.imp();

                gstreamer::info!(CAT, "Attempting network recovery...");

                // Attempt recovery by triggering upload
                match buffer_mgr.lock().await.trigger_upload_all(None, None).await {
                    Ok(()) => {
                        gstreamer::info!(CAT, "Network recovery successful!");

                        // Reset failure counter and clear recovery task handle
                        {
                            let mut state = imp.state.lock().unwrap();
                            state.consecutive_failures = 0;
                            state.recovery_task = None;
                        }

                        // Process buffer events to switch to Immediate mode
                        imp.process_buffer_events().await;

                        gstreamer::info!(CAT, "Recovery task completed successfully");
                        return; // Exit loop - recovery successful
                    }
                    Err(e) => {
                        // Check if it's a network error
                        if Self::is_network_error_str(&e.to_string()) {
                            gstreamer::warning!(
                                CAT,
                                "Network still unavailable: {} - will retry with backoff",
                                e
                            );

                            // Increment failure counter for next backoff
                            let mut state = imp.state.lock().unwrap();
                            state.consecutive_failures += 1;
                        } else {
                            gstreamer::warning!(
                                CAT,
                                "Non-network error during recovery: {} - will retry",
                                e
                            );
                        }

                        // Continue loop with increased backoff
                    }
                }
            }
        });

        // Store task handle
        {
            let mut state = self.state.lock().unwrap();
            state.recovery_task = Some(task_handle);
        }

        gstreamer::info!(CAT, imp = self, "Network recovery task spawned");
        Ok(())
    }

    /// Process buffer manager events (upload completion, errors)
    async fn process_buffer_events(&self) {
        let events: Vec<BufferManagerEvent> = {
            let mut state = self.state.lock().unwrap();
            let mut events = Vec::new();

            if let Some(rx) = &mut state.buffer_event_rx {
                while let Ok(event) = rx.try_recv() {
                    events.push(event);
                }
            }
            events
        };

        for event in events {
            match event {
                BufferManagerEvent::HistoricalUploadComplete {
                    switch_to_streaming,
                } => {
                    gstreamer::info!(
                        CAT,
                        imp = self,
                        "Historical upload complete - switch_to_streaming: {}",
                        switch_to_streaming
                    );

                    if switch_to_streaming {
                        // Reset failure counter and cancel recovery task (successful upload)
                        {
                            let mut state = self.state.lock().unwrap();
                            state.consecutive_failures = 0;
                        }

                        if self.cancel_recovery_task() {
                            gstreamer::info!(
                                CAT,
                                imp = self,
                                "Cancelled recovery task - upload successful"
                            );
                        }

                        // Switch to Immediate mode for live streaming
                        self.switch_mode(UploadMode::Immediate).await;
                    } else {
                        // Switch to BufferOnly mode (for time-range uploads or when end is specified)
                        gstreamer::info!(
                            CAT,
                            imp = self,
                            "Switching to BufferOnly mode as requested (time range upload)"
                        );
                        self.switch_mode(UploadMode::BufferOnly).await;
                    }
                }
                BufferManagerEvent::UploadError(err) => {
                    gstreamer::warning!(
                        CAT,
                        imp = self,
                        "Upload error from buffer manager: {}",
                        err
                    );
                }
            }
        }
    }

    /// Schedule a mode switch to execute at the next keyframe boundary
    /// This prevents frame loss by allowing the current fragment to complete
    async fn switch_mode(&self, new_mode: UploadMode) {
        gstreamer::info!(
            CAT,
            imp = self,
            "Scheduling switch to {:?} mode (will execute at next keyframe)",
            new_mode
        );

        {
            let mut state = self.state.lock().unwrap();
            // Schedule mode switch for next keyframe boundary
            // This allows current fragment to complete without losing frames
            state.pending_mode_switch = Some(new_mode);
        }

        // Cancel recovery task if switching to Immediate mode (network recovered)
        if new_mode == UploadMode::Immediate && self.cancel_recovery_task() {
            gstreamer::debug!(CAT, imp = self, "Cancelled recovery task");
        }
    }

    /// Update fragment duration based on mode (adaptive cluster duration)
    /// - Immediate mode: 2s clusters (low latency, 0.5 fragments/sec)
    /// - BufferOnly mode: 15s clusters (fast catchup at 5 frag/sec)
    fn update_fragment_duration(&self, mode: UploadMode) -> Result<(), gstreamer::LoggableError> {
        let duration_ns = match mode {
            UploadMode::Immediate => 2_000_000_000u64,   // 2 seconds
            UploadMode::BufferOnly => 15_000_000_000u64, // 15 seconds
        };

        gstreamer::info!(
            CAT,
            imp = self,
            "Setting fragment duration to {}s for {:?} mode",
            duration_ns / 1_000_000_000,
            mode
        );

        // Update settings
        let mut settings = self.settings.lock().unwrap();
        settings.fragment_duration = gstreamer::ClockTime::from_nseconds(duration_ns);

        // Note: This takes effect on NEXT fragment boundary (current fragment completes first)

        Ok(())
    }
}

static CAT: Lazy<gstreamer::DebugCategory> = Lazy::new(|| {
    gstreamer::DebugCategory::new(
        "rskvssink",
        gstreamer::DebugColorFlags::empty(),
        Some("KVS Sink"),
    )
});
