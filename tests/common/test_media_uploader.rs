//! Test implementation of MediaUploader for integration tests

use anyhow::Result;
use gstrskvssink::advanced::{Fragment, KvsError, MediaUploader};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};
use webm_iterable::WebmIterator;
use webm_iterable::matroska_spec::{Master, MatroskaSpec};

/// Test implementation of MediaUploader that validates and collects fragments
#[derive(Clone, Debug)]
pub struct TestMediaUploader {
    inner: Arc<Mutex<TestUploaderInner>>,
    fail_on_corruption: bool,
    validate_mkv: bool,
}

#[derive(Debug)]
struct TestUploaderInner {
    fragments: Vec<Fragment>,
    segments: Vec<SegmentUpload>,
    session_state: SessionState,
    stream_name: String,
    initialized: bool,
    expected_frame_count: Option<usize>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SegmentUpload {
    size_bytes: usize,
    stream_name: String,
    region: String,
}

#[derive(Default, Debug)]
struct SessionState {
    /// Complete MKV stream accumulated from all fragments
    accumulated_mkv: Vec<u8>,

    /// Byte offsets where each fragment starts in accumulated_mkv
    fragment_boundaries: Vec<usize>,

    /// Track if first fragment had EBML headers
    first_fragment_has_headers: Option<bool>,

    /// Cluster timestamps for continuity checking
    cluster_timestamps: Vec<u64>,

    /// Track session resets
    session_number: u32,

    /// Fragment metadata for analysis
    fragment_metadata: Vec<FragmentMetadata>,
}

#[derive(Debug, Clone)]
pub struct FragmentMetadata {
    #[allow(dead_code)]
    pub fragment_number: Option<u64>,
    pub size_bytes: usize,
    pub has_ebml_header: bool,
    pub has_segment_header: bool,
    pub has_tracks: bool,
    pub cluster_count: usize,
    pub first_timestamp_ms: Option<u64>,
    pub last_timestamp_ms: Option<u64>,
    pub duration_ms: Option<u64>,
    pub frame_count: usize,
    pub expected_frame_count: Option<usize>,
}

impl TestMediaUploader {
    /// Create a new test uploader
    #[allow(dead_code)]
    pub fn new(fail_on_corruption: bool, validate_mkv: bool) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestUploaderInner {
                fragments: Vec::new(),
                segments: Vec::new(),
                session_state: SessionState::default(),
                stream_name: String::new(),
                initialized: false,
                expected_frame_count: None,
            })),
            fail_on_corruption,
            validate_mkv,
        }
    }

    /// Get all collected fragments
    #[allow(dead_code)]
    pub fn get_fragments(&self) -> Vec<Fragment> {
        self.inner.lock().unwrap().fragments.clone()
    }

    /// Get fragment count
    #[allow(dead_code)]
    pub fn fragment_count(&self) -> usize {
        self.inner.lock().unwrap().fragments.len()
    }

    /// Get total bytes uploaded
    #[allow(dead_code)]
    pub fn total_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .fragments
            .iter()
            .map(|f| f.total_size())
            .sum()
    }

    /// Get fragment metadata for analysis
    #[allow(dead_code)]
    pub fn get_fragment_metadata(&self) -> Vec<FragmentMetadata> {
        self.inner
            .lock()
            .unwrap()
            .session_state
            .fragment_metadata
            .clone()
    }

    /// Get segment upload count
    #[allow(dead_code)]
    pub fn segment_count(&self) -> usize {
        self.inner.lock().unwrap().segments.len()
    }

    /// Get total bytes uploaded via segments
    #[allow(dead_code)]
    pub fn segment_total_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .segments
            .iter()
            .map(|s| s.size_bytes)
            .sum()
    }

    /// Set expected frame count for validation
    #[allow(dead_code)]
    pub fn set_expected_frame_count(&self, expected: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.expected_frame_count = Some(expected);
    }

    /// Count actual decodable frames using ffprobe
    fn count_frames_with_ffprobe(&self, fragment_data: &[u8]) -> Result<usize> {
        use anyhow::Context;

        // Write to temp file
        let temp_file = format!("/tmp/test_fragment_{}.mkv", std::process::id());
        std::fs::write(&temp_file, fragment_data).context("Failed to write temp fragment file")?;

        // Run ffprobe with frame counting enabled
        let probe = ffprobe::Config::builder()
            .count_frames(true)
            .run(&temp_file)
            .map_err(|e| anyhow::anyhow!("ffprobe failed: {}", e))?;

        // Clean up temp file
        std::fs::remove_file(&temp_file).ok();

        // Extract frame count from video stream
        let video_stream = probe
            .streams
            .iter()
            .find(|s| s.codec_type.as_deref() == Some("video"))
            .ok_or_else(|| anyhow::anyhow!("No video stream found"))?;

        let frame_count = video_stream
            .nb_read_frames
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Frame count not available"))?
            .parse::<usize>()
            .context("Failed to parse frame count")?;

        Ok(frame_count)
    }

    /// Validate MKV structure of a fragment using webm_iterable
    fn validate_fragment(
        &self,
        fragment: &Fragment,
        expected_frame_count: Option<usize>,
    ) -> Result<FragmentMetadata> {
        let mut metadata = FragmentMetadata {
            fragment_number: fragment.fragment_number,
            size_bytes: fragment.total_size(),
            has_ebml_header: false,
            has_segment_header: false,
            has_tracks: false,
            cluster_count: 0,
            first_timestamp_ms: None,
            last_timestamp_ms: None,
            duration_ms: None,
            frame_count: 0,
            expected_frame_count,
        };

        // Compose full fragment data (headers + cluster) for MKV parsing
        let fragment_data = [&fragment.header_data[..], &fragment.cluster_data[..]].concat();

        // Parse the MKV structure using webm_iterable
        let mut reader = std::io::Cursor::new(&fragment_data);
        let iterator = WebmIterator::new(&mut reader, &[]);

        for element_result in iterator {
            match element_result {
                Ok(element) => match element {
                    MatroskaSpec::Ebml(Master::Start) => {
                        metadata.has_ebml_header = true;
                        debug!("Fragment {:?} has EBML header", fragment.fragment_number);
                    }
                    MatroskaSpec::Segment(Master::Start) => {
                        metadata.has_segment_header = true;
                        debug!("Fragment {:?} has Segment header", fragment.fragment_number);
                    }
                    MatroskaSpec::Tracks(Master::Start) => {
                        metadata.has_tracks = true;
                        debug!("Fragment {:?} has Tracks", fragment.fragment_number);
                    }
                    MatroskaSpec::Cluster(Master::Start) => {
                        metadata.cluster_count += 1;
                        debug!(
                            "Fragment {:?} has Cluster {}",
                            fragment.fragment_number, metadata.cluster_count
                        );
                    }
                    MatroskaSpec::Timestamp(timestamp) => {
                        if metadata.first_timestamp_ms.is_none() {
                            metadata.first_timestamp_ms = Some(timestamp);
                        }
                        metadata.last_timestamp_ms = Some(timestamp);
                        debug!(
                            "Fragment {:?} timestamp: {}ms",
                            fragment.fragment_number, timestamp
                        );
                    }
                    MatroskaSpec::Void(_) | MatroskaSpec::Crc32(_) => {
                        // Normal padding/checksum elements - ignore
                    }
                    _ => {
                        // Other valid elements
                    }
                },
                Err(e) => {
                    if self.fail_on_corruption {
                        anyhow::bail!(
                            "Fragment {:?} MKV parsing error: {}",
                            fragment.fragment_number,
                            e
                        );
                    } else {
                        warn!(
                            "Fragment {:?} MKV parsing warning: {}",
                            fragment.fragment_number, e
                        );
                    }
                }
            }
        }

        // Calculate duration if we have timestamps
        if let (Some(first), Some(last)) = (metadata.first_timestamp_ms, metadata.last_timestamp_ms)
        {
            metadata.duration_ms = Some(last.saturating_sub(first));
        }

        // Validate structure based on fragment number
        // Note: fragment_number starts from 1, not 0
        if fragment.fragment_number == Some(1) {
            // First fragment must have headers
            if !metadata.has_ebml_header || !metadata.has_segment_header || !metadata.has_tracks {
                let msg = format!(
                    "First fragment {:?} missing headers: EBML={}, Segment={}, Tracks={}",
                    fragment.fragment_number,
                    metadata.has_ebml_header,
                    metadata.has_segment_header,
                    metadata.has_tracks
                );
                if self.fail_on_corruption {
                    anyhow::bail!("{}", msg);
                } else {
                    warn!("{}", msg);
                }
            }
        } else {
            // Subsequent fragments should only have clusters (unless after reset)
            if metadata.has_ebml_header || metadata.has_segment_header || metadata.has_tracks {
                debug!(
                    "Fragment {:?} has headers (might be after reset): EBML={}, Segment={}, Tracks={}",
                    fragment.fragment_number,
                    metadata.has_ebml_header,
                    metadata.has_segment_header,
                    metadata.has_tracks
                );
            }
        }

        // Must have at least one cluster (unless it's a pure header fragment)
        if metadata.cluster_count == 0 && !metadata.has_ebml_header {
            let msg = format!(
                "Fragment {:?} has no clusters and no headers",
                fragment.fragment_number
            );
            if self.fail_on_corruption {
                anyhow::bail!("{}", msg);
            } else {
                warn!("{}", msg);
            }
        }

        // Reasonable size check
        if fragment.total_size() < 50 {
            let msg = format!(
                "Fragment {:?} unusually small: {} bytes",
                fragment.fragment_number,
                fragment.total_size()
            );
            if self.fail_on_corruption {
                anyhow::bail!("{}", msg);
            } else {
                warn!("{}", msg);
            }
        }

        // Validate actual decodable frame count using ffprobe
        match self.count_frames_with_ffprobe(&fragment_data) {
            Ok(frame_count) => {
                metadata.frame_count = frame_count;

                // Validate against expected count if set
                if let Some(expected) = metadata.expected_frame_count {
                    let min_acceptable = expected.saturating_sub(expected / 5); // 20% tolerance
                    if metadata.frame_count < min_acceptable {
                        let msg = format!(
                            "Fragment {:?} has only {} frames, expected ~{} frames",
                            fragment.fragment_number, metadata.frame_count, expected
                        );
                        if self.fail_on_corruption {
                            anyhow::bail!("{}", msg);
                        } else {
                            warn!("{}", msg);
                        }
                    }
                }

                debug!(
                    "Fragment {:?} validated: {} frames counted by ffprobe",
                    fragment.fragment_number, metadata.frame_count
                );
            }
            Err(e) => {
                if self.fail_on_corruption {
                    anyhow::bail!("ffprobe frame counting failed: {}", e);
                } else {
                    warn!("ffprobe frame counting failed: {}", e);
                }
            }
        }

        Ok(metadata)
    }
}

impl MediaUploader for TestMediaUploader {
    fn initialize(
        &self,
        stream_name: &str,
        region: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + '_>> {
        let stream_name = stream_name.to_string();
        let region = region.to_string();

        Box::pin(async move {
            let mut inner = self.inner.lock().unwrap();

            info!(
                "Test uploader initialized for stream: {} in region: {}",
                stream_name, region
            );
            inner.stream_name = stream_name;
            inner.initialized = true;

            Ok(())
        })
    }

    fn put_fragment<'a>(
        &'a self,
        fragment: &'a Fragment,
    ) -> Pin<Box<dyn Future<Output = Result<(), KvsError>> + Send + 'a>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().unwrap();

            if !inner.initialized {
                return Err(KvsError::Internal(anyhow::anyhow!(
                    "Test uploader not initialized"
                )));
            }

            debug!(
                "Test uploader received fragment {:?} ({} bytes)",
                fragment.fragment_number,
                fragment.total_size()
            );

            // Validate MKV structure if enabled
            if self.validate_mkv {
                // Get expected frame count while we have the lock
                let expected_frame_count = inner.expected_frame_count;

                // Release the lock before calling validate_fragment to avoid deadlock
                drop(inner);

                let metadata = self
                    .validate_fragment(fragment, expected_frame_count)
                    .map_err(KvsError::Internal)?;

                // Re-acquire the lock to update state
                let mut inner = self.inner.lock().unwrap();

                // Compose full fragment data for accumulation
                let fragment_data =
                    [&fragment.header_data[..], &fragment.cluster_data[..]].concat();

                // Update session state
                let offset = inner.session_state.accumulated_mkv.len();
                inner.session_state.fragment_boundaries.push(offset);
                inner
                    .session_state
                    .accumulated_mkv
                    .extend_from_slice(&fragment_data);
                inner.session_state.fragment_metadata.push(metadata.clone());

                // Track first fragment headers
                if fragment.fragment_number == Some(1) {
                    inner.session_state.first_fragment_has_headers = Some(metadata.has_ebml_header);
                }

                // Check for session reset (new headers after first fragment)
                if fragment.fragment_number.is_some_and(|n| n > 1) && metadata.has_ebml_header {
                    info!(
                        "Detected session reset at fragment {:?}",
                        fragment.fragment_number
                    );
                    inner.session_state.session_number += 1;

                    // Clear accumulated data for new session
                    inner.session_state.accumulated_mkv.clear();
                    inner.session_state.fragment_boundaries.clear();
                    inner.session_state.fragment_metadata.clear();
                    inner.session_state.cluster_timestamps.clear();

                    // Start new accumulation
                    inner
                        .session_state
                        .accumulated_mkv
                        .extend_from_slice(&fragment_data);
                    inner.session_state.fragment_boundaries.push(0);
                }

                info!(
                    "Fragment {:?} validated: {} clusters, {} bytes, duration: {:?}ms",
                    fragment.fragment_number,
                    metadata.cluster_count,
                    metadata.size_bytes,
                    metadata.duration_ms
                );

                // Store fragment - now simpler since we don't signal resets
                inner.fragments.push(fragment.clone());
            } else {
                // Store fragment without validation
                inner.fragments.push(fragment.clone());
            }

            // Note: With simplified session management, the timer in imp.rs handles resets
            // No need to simulate reset signals from the uploader

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
            // Just log for test observability
            info!("Test uploader: reset_session called (no-op for tests)");
            Ok(())
        })
    }
}

/// Builder for creating test uploaders with different configurations
#[allow(dead_code)]
pub struct TestUploaderBuilder {
    fail_on_corruption: bool,
    validate_mkv: bool,
}

impl TestUploaderBuilder {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            fail_on_corruption: false,
            validate_mkv: true,
        }
    }

    #[allow(dead_code)]
    pub fn fail_on_corruption(mut self, fail: bool) -> Self {
        self.fail_on_corruption = fail;
        self
    }

    #[allow(dead_code)]
    pub fn validate_mkv(mut self, validate: bool) -> Self {
        self.validate_mkv = validate;
        self
    }

    #[allow(dead_code)]
    pub fn build(self) -> TestMediaUploader {
        TestMediaUploader::new(self.fail_on_corruption, self.validate_mkv)
    }
}
