/*!
# Amazon Kinesis Video Streams (KVS) MKV Fragment Structure

Based on analysis of Amazon's official PIC (Platform Independent Code) library:
- Repository: amazon-kinesis-video-streams-pic
- File: src/mkvgen/src/MkvGenerator.c (mkvgenPackageFrame function)
- Used by all Amazon SDKs: C, C++, Java

## Official KVS Fragment Structure

Amazon's MKV generator uses a **per-fragment approach** where each fragment contains only
the required data for that specific fragment:

### Fragment Content (from `mkvgenPackageFrame`):

**Fragment 1 (`MKV_STATE_START_STREAM`):**
```text
- EBML Header           (mkvgenEbmlEncodeHeader)
- Segment Header        (mkvgenEbmlEncodeSegmentHeader)
- Segment Info          (mkvgenEbmlEncodeSegmentInfo)
- Track Info            (mkvgenEbmlEncodeTrackInfo)
- Cluster Header        (mkvgenEbmlEncodeClusterInfo)
- Simple Block          (mkvgenEbmlEncodeSimpleBlock)
```

**Fragment 2+ (`MKV_STATE_START_CLUSTER`):**
```text
- Cluster Header        (mkvgenEbmlEncodeClusterInfo)
- Simple Block          (mkvgenEbmlEncodeSimpleBlock)
```

### Key Implementation Details:

1. **Per-Fragment Generation**: Each fragment is generated independently
2. **Fragment 2+ Structure**: Contains NO EBML, Segment, Info, or Tracks headers - only cluster data
3. **Timestamp Handling**: Stream timestamps normalized to start from zero, cluster timestamps relative
4. **Session Reset Behavior**: After 45-minute timeout, fragment numbering restarts from 1

This implementation follows the rs-kvs-streamer pattern for maximum KVS compatibility.
*/

use anyhow::{Context, Result};
use gstreamer::{Buffer, ClockTime};
use tracing::{debug, info};
use webm_iterable::matroska_spec::{EbmlTag, Master, MatroskaSpec};
use webm_iterable::{WebmWriter, WriteOptions};

use crate::fragment::Fragment;

/// MKV writer that generates complete fragments (headers + cluster)
/// Always generates headers for every fragment - KvsConnection decides when to include them
pub struct MkvWriter {
    /// First PTS seen in current session (for timestamp normalization)
    stream_start_timestamp: Option<ClockTime>,
    /// PTS offset for session reset (pipeline PTS continues, MKV timestamps start from zero)
    pts_offset: ClockTime,
    /// Wall-clock time when this stream session started (for absolute timestamps)
    session_start_wall_time: Option<std::time::SystemTime>,
    /// Video dimensions
    width: u32,
    height: u32,
    /// H.264 codec private data (SPS/PPS from GStreamer caps)
    codec_data: Option<Vec<u8>>,
}

impl MkvWriter {
    pub fn new() -> Self {
        Self {
            stream_start_timestamp: None,
            pts_offset: ClockTime::ZERO,
            session_start_wall_time: None,
            width: 1920,
            height: 1080,
            codec_data: None,
        }
    }

    /// Generate MKV headers (EBML + Segment + Info + Tracks) without cluster data
    /// This is used for segment files that contain multiple clusters
    pub fn generate_mkv_headers(&self) -> Result<Vec<u8>> {
        let buffer = Vec::new();
        let mut writer = WebmWriter::new(buffer);

        // EBML Header
        writer.write(&MatroskaSpec::Ebml(Master::Start))?;
        writer.write(&MatroskaSpec::DocType("matroska".to_owned()))?;
        writer.write(&MatroskaSpec::DocTypeVersion(4))?;
        writer.write(&MatroskaSpec::Ebml(Master::End))?;

        // Segment Header (streaming with unknown size)
        writer.write_advanced(
            &MatroskaSpec::Segment(Master::Start),
            WriteOptions::is_unknown_sized_element(),
        )?;

        // Segment Info
        writer.write(&MatroskaSpec::Info(Master::Start))?;
        writer.write(&MatroskaSpec::TimestampScale(1_000_000))?; // 1ms precision
        writer.write(&MatroskaSpec::MuxingApp("gstrskvssink".to_string()))?;
        writer.write(&MatroskaSpec::WritingApp("gstrskvssink".to_string()))?;
        writer.write(&MatroskaSpec::Info(Master::End))?;

        // Track Info
        writer.write(&MatroskaSpec::Tracks(Master::Start))?;
        writer.write(&MatroskaSpec::TrackEntry(Master::Start))?;
        writer.write(&MatroskaSpec::TrackNumber(1))?;
        writer.write(&MatroskaSpec::TrackUID(1))?;
        writer.write(&MatroskaSpec::TrackType(0x01))?; // Video
        writer.write(&MatroskaSpec::CodecID("V_MPEG4/ISO/AVC".to_string()))?;

        if let Some(codec_data) = self.codec_data.as_deref() {
            writer.write(&MatroskaSpec::CodecPrivate(codec_data.to_vec()))?;
        }

        writer.write(&MatroskaSpec::Video(Master::Start))?;
        writer.write(&MatroskaSpec::PixelWidth(self.width as u64))?;
        writer.write(&MatroskaSpec::PixelHeight(self.height as u64))?;
        writer.write(&MatroskaSpec::Video(Master::End))?;
        writer.write(&MatroskaSpec::TrackEntry(Master::End))?;
        writer.write(&MatroskaSpec::Tracks(Master::End))?;

        writer
            .into_inner()
            .context("Failed to generate MKV headers")
    }

    /// Reset for new KVS session - pipeline PTS continues, MKV timestamps start from zero
    pub fn reset_for_new_session(&mut self, current_pipeline_pts: ClockTime) {
        info!(
            "Resetting MKV writer for new KVS session - pipeline PTS: {}ms",
            current_pipeline_pts.mseconds()
        );

        // Set PTS offset so MKV timestamps start from zero in new session
        self.pts_offset = current_pipeline_pts;
        self.stream_start_timestamp = None;
        // Reset session start time - new session starts now
        self.session_start_wall_time = None;
    }

    pub fn set_codec_metadata(&mut self, width: u32, height: u32, codec_data: Option<Vec<u8>>) {
        self.width = width;
        self.height = height;
        self.codec_data = codec_data;
        debug!(
            "Updated codec metadata: {}x{}, codec_data: {}",
            width,
            height,
            if self.codec_data.is_some() {
                "present"
            } else {
                "missing"
            }
        );
    }

    /// Normalize PTS by subtracting the offset (for session resets)
    /// Returns ZERO if pts is less than offset (shouldn't happen in normal operation)
    #[inline]
    fn normalize_pts(&self, pts: ClockTime) -> ClockTime {
        if pts >= self.pts_offset {
            pts - self.pts_offset
        } else {
            ClockTime::ZERO
        }
    }

    /// Generate complete fragment containing one or more frames
    /// Always generates headers + cluster data - KvsConnection decides when to include headers
    pub fn finalize_fragment(&mut self, frames: &[&Buffer]) -> Result<Fragment> {
        if frames.is_empty() {
            // Empty fragment (shouldn't happen, but handle gracefully)
            let empty_headers = self.generate_mkv_headers()?;
            return Ok(Fragment::new(empty_headers, Vec::new(), ClockTime::ZERO));
        }

        // Log detailed buffer statistics for debugging corruption
        let total_bytes: usize = frames.iter().map(|b| b.size()).sum();
        let avg_bytes = total_bytes / frames.len();
        let keyframe_count = frames
            .iter()
            .filter(|b| !b.flags().contains(gstreamer::BufferFlags::DELTA_UNIT))
            .count();

        debug!(
            "Fragment buffer stats: {} frames, {} total bytes, {} avg/frame, {} keyframes",
            frames.len(),
            total_bytes,
            avg_bytes,
            keyframe_count
        );

        let first_frame_pts = frames[0].pts().unwrap_or(ClockTime::ZERO);
        let last_frame_pts = frames.last().unwrap().pts().unwrap_or(ClockTime::ZERO);

        // Calculate actual fragment duration from frames
        // IMPORTANT: Must include the duration of the last frame itself to ensure
        // contiguous timestamps (no gaps between fragments in KVS)
        let fragment_duration = if last_frame_pts >= first_frame_pts {
            let base_duration = last_frame_pts - first_frame_pts;
            // Add the last frame's duration to get true content duration
            // Without this, we create ~33ms gaps between fragments (causing KVS 500 errors)
            let last_frame_duration = frames.last().unwrap().duration().unwrap_or(ClockTime::ZERO);
            base_duration + last_frame_duration
        } else {
            ClockTime::ZERO
        };

        // Normalize first frame PTS (used for absolute timestamp calculation)
        let normalized_pts = self.normalize_pts(first_frame_pts);

        // Initialize stream timestamp and wall time on first fragment
        if self.stream_start_timestamp.is_none() {
            self.stream_start_timestamp = Some(normalized_pts);
            self.session_start_wall_time = Some(std::time::SystemTime::now());
            info!(
                "Starting new stream session at wall time: {:?}",
                self.session_start_wall_time.unwrap()
            );
        }

        debug!(
            "Generated fragment: normalized_pts={}ms, duration={}ms, frames={}",
            normalized_pts.mseconds(),
            fragment_duration.mseconds(),
            frames.len()
        );

        // Always generate headers
        let headers = self.generate_mkv_headers()?;

        // Generate cluster data (absolute timestamp calculated internally)
        let cluster_data = self.generate_pure_cluster_data(normalized_pts, frames)?;

        debug!(
            "Generated {} bytes of headers + {} bytes of cluster data",
            headers.len(),
            cluster_data.len()
        );

        // Return fragment with calculated duration (not ZERO)
        Ok(Fragment::new(headers, cluster_data, fragment_duration))
    }

    /// Generate pure cluster data without segment wrapper
    fn generate_pure_cluster_data(
        &self,
        normalized_cluster_start: ClockTime,
        frames: &[&Buffer],
    ) -> Result<Vec<u8>> {
        let buffer = Vec::new();
        let mut writer = WebmWriter::new(buffer);

        // Write cluster directly without segment wrapper
        // Using unknown_sized_element skips validation that requires Segment parent
        self.write_cluster_and_frames(&mut writer, normalized_cluster_start, frames)?;

        // Get the cluster data directly - no extraction needed!
        let cluster_data = writer.into_inner()?;

        debug!(
            "Generated cluster: {} bytes (pure cluster, no segment wrapper)",
            cluster_data.len()
        );

        Ok(cluster_data)
    }

    /// Write cluster header and frames
    fn write_cluster_and_frames(
        &self,
        writer: &mut WebmWriter<Vec<u8>>,
        normalized_cluster_start: ClockTime,
        frames: &[&Buffer],
    ) -> Result<()> {
        // Calculate absolute timestamp for cluster header (KVS ABSOLUTE mode)
        let cluster_timestamp = self.calculate_absolute_timestamp(normalized_cluster_start)?;

        debug!(
            "Cluster timestamp: {}ms (normalized_pts: {}ms, absolute timestamp for KVS)",
            cluster_timestamp.mseconds(),
            normalized_cluster_start.mseconds()
        );

        // Cluster Header (unknown size for streaming, skips Segment parent validation)
        writer.write_advanced(
            &MatroskaSpec::Cluster(Master::Start),
            WriteOptions::is_unknown_sized_element(),
        )?;

        // Write Timestamp using write_raw to bypass validation
        // Timestamp is UnsignedInt, so we need to encode it manually
        let timestamp_spec = MatroskaSpec::Timestamp(cluster_timestamp.mseconds());
        let timestamp_id = timestamp_spec.get_id();
        let timestamp_value = cluster_timestamp.mseconds();

        // Encode as big-endian bytes with minimal length (following TagWriter convention)
        let timestamp_bytes = if timestamp_value <= 0xFF {
            vec![(timestamp_value & 0xFF) as u8]
        } else if timestamp_value <= 0xFFFF {
            vec![
                ((timestamp_value >> 8) & 0xFF) as u8,
                (timestamp_value & 0xFF) as u8,
            ]
        } else if timestamp_value <= 0xFFFFFFFF {
            timestamp_value.to_be_bytes()[4..].to_vec()
        } else {
            timestamp_value.to_be_bytes().to_vec()
        };

        writer.write_raw(timestamp_id, &timestamp_bytes)?;

        // Write all frames as SimpleBlocks using write_raw
        for frame in frames.iter() {
            let frame_pts = frame.pts().unwrap_or(ClockTime::ZERO);
            let normalized_pts = self.normalize_pts(frame_pts);

            // Calculate relative timestamp within cluster (SimpleBlocks are relative to cluster)
            // Use normalized PTS difference, not absolute timestamps
            let cluster_relative_ms = if normalized_pts >= normalized_cluster_start {
                (normalized_pts.mseconds() - normalized_cluster_start.mseconds()) as i16
            } else {
                0i16
            };

            self.write_simple_block(writer, frame, cluster_relative_ms)?;
        }

        Ok(())
    }

    fn write_simple_block(
        &self,
        writer: &mut WebmWriter<Vec<u8>>,
        buffer: &Buffer,
        relative_timestamp: i16,
    ) -> Result<()> {
        // Extract frame data (already in AVCC format from x264enc)
        let map = buffer.map_readable().context("Failed to map buffer")?;
        let frame_data = map.as_slice();

        // Validate buffer has data - empty/tiny buffers indicate encoder corruption
        if frame_data.is_empty() {
            anyhow::bail!(
                "Empty GStreamer buffer detected (PTS: {:?}) - encoder produced corrupt data",
                buffer.pts()
            );
        }
        if frame_data.len() < 10 {
            tracing::warn!(
                "Suspiciously small buffer: {} bytes (PTS: {:?}) - possible encoder corruption",
                frame_data.len(),
                buffer.pts()
            );
        }

        // Validate AVCC format (should start with NAL size as 4-byte big-endian)
        // AVCC format: [size:4][NAL unit][size:4][NAL unit]...
        if frame_data.len() >= 4 {
            let nal_size =
                u32::from_be_bytes([frame_data[0], frame_data[1], frame_data[2], frame_data[3]])
                    as usize;
            if nal_size + 4 > frame_data.len() {
                tracing::warn!(
                    "Invalid AVCC format: NAL size {} exceeds buffer size {} (PTS: {:?})",
                    nal_size,
                    frame_data.len(),
                    buffer.pts()
                );
            } else if nal_size == 0 {
                tracing::warn!(
                    "AVCC NAL size is zero (buffer size: {}, PTS: {:?}) - encoder corruption?",
                    frame_data.len(),
                    buffer.pts()
                );
            }
        }

        // Check if keyframe
        let is_keyframe = !buffer.flags().contains(gstreamer::BufferFlags::DELTA_UNIT);

        // Create properly formed SimpleBlock using new_unchecked
        let block = webm_iterable::matroska_spec::SimpleBlock::new_uncheked(
            frame_data,
            1, // track number
            relative_timestamp,
            false, // invisible
            None,  // lacing
            false, // discardable
            is_keyframe,
        );

        // Convert to MatroskaSpec and extract ID and binary data
        let block_spec: MatroskaSpec = block.into();
        let block_id = block_spec.get_id();
        let block_data = block_spec
            .as_binary()
            .context("Failed to get binary data from SimpleBlock")?;

        // Write using write_raw to bypass validation
        writer.write_raw(block_id, block_data)?;
        Ok(())
    }

    /// Calculate absolute Unix timestamp in milliseconds for ABSOLUTE mode
    fn calculate_absolute_timestamp(&self, normalized_pts: ClockTime) -> Result<ClockTime> {
        let session_start_wall_time = self
            .session_start_wall_time
            .ok_or_else(|| anyhow::anyhow!("Session start wall time not initialized"))?;

        let stream_start_pts = self
            .stream_start_timestamp
            .ok_or_else(|| anyhow::anyhow!("Stream start timestamp not initialized"))?;

        // Calculate how much time has elapsed since stream start (in PTS terms)
        let elapsed_stream_time = if normalized_pts >= stream_start_pts {
            normalized_pts - stream_start_pts
        } else {
            ClockTime::ZERO
        };

        // Convert to wall-clock time by adding elapsed time to session start
        let elapsed_duration = std::time::Duration::from_nanos(elapsed_stream_time.nseconds());
        let absolute_time = session_start_wall_time + elapsed_duration;

        // Convert to Unix timestamp in milliseconds
        let unix_timestamp_ms = absolute_time
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| anyhow::anyhow!("Failed to calculate Unix timestamp: {}", e))?
            .as_millis();

        // Convert back to ClockTime for consistency with existing code
        let absolute_timestamp = ClockTime::from_mseconds(unix_timestamp_ms as u64);

        Ok(absolute_timestamp)
    }
}
