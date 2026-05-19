use crate::fragment::Fragment;
use crate::media_uploader::MediaUploader;
use anyhow::{Context, Result};
use gstreamer::ClockTime;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tracing::{debug, info, warn};

// Upload retry configuration
const MAX_UPLOAD_RETRIES: u32 = 5;

// Mega-cluster file configuration
const MEGA_CLUSTER_DURATION: Duration = Duration::from_secs(1800); // 30 minutes
const MEGA_CLUSTER_MAX_SIZE: u64 = 45_000_000; // 45MB (leave margin under 50MB KVS limit)
const _MAX_GAP_FOR_SINGLE_CONNECTION: Duration = Duration::from_secs(3600); // 1 hour (reserved for future use)

/// Mega-cluster file metadata (append-only cluster storage)
#[derive(Clone, Debug)]
struct MegaClusterFile {
    file_path: PathBuf,
    start_time: SystemTime,
    end_time: SystemTime,
    file_size: u64,
    cluster_count: usize,

    // Upload tracking
    uploaded: bool,
    upload_attempts: u32,
    last_upload_attempt: Option<SystemTime>,
    last_upload_error: Option<String>,
}

/// Current mega-cluster file being written
/// Format: [u64: size][u64: timestamp_ms][u64: duration_ms][cluster_bytes] repeated
struct CurrentMegaClusterFile {
    file: tokio::fs::File,
    file_path: PathBuf,
    file_created_at: SystemTime, // Wall clock time when file was created (for rotation)
    start_time: SystemTime,      // Fragment timestamp of first fragment (for time range filtering)
    last_fragment_time: SystemTime,
    current_size: u64,
    cluster_count: usize,
}

/// Iterator for reading fragments from mega-cluster file
/// Handles crash recovery by stopping at incomplete records
/// New format: [cluster_size][timestamp][duration][header_size][headers][cluster]
pub struct ClusterFileReader {
    data: Vec<u8>,
    offset: usize,
}

impl ClusterFileReader {
    /// Load cluster file from disk
    pub async fn from_path(path: &Path) -> Result<Self> {
        let data = tokio::fs::read(path)
            .await
            .context("Failed to read cluster file")?;
        Ok(Self { data, offset: 0 })
    }
}

impl Iterator for ClusterFileReader {
    type Item = Result<Fragment>;

    fn next(&mut self) -> Option<Self::Item> {
        // Need at least 32 bytes for header: cluster_size + timestamp + duration + header_size
        if self.offset + 32 > self.data.len() {
            return None; // EOF or incomplete header
        }

        // Read metadata header (4 × u64 = 32 bytes)
        let cluster_size_bytes = match self.data[self.offset..self.offset + 8].try_into() {
            Ok(b) => b,
            Err(_) => return None,
        };
        let cluster_size = u64::from_le_bytes(cluster_size_bytes);

        let timestamp_bytes = match self.data[self.offset + 8..self.offset + 16].try_into() {
            Ok(b) => b,
            Err(_) => return None,
        };
        let timestamp_ms = u64::from_le_bytes(timestamp_bytes);

        let duration_bytes = match self.data[self.offset + 16..self.offset + 24].try_into() {
            Ok(b) => b,
            Err(_) => return None,
        };
        let duration_ms = u64::from_le_bytes(duration_bytes);

        let header_size_bytes = match self.data[self.offset + 24..self.offset + 32].try_into() {
            Ok(b) => b,
            Err(_) => return None,
        };
        let header_size = u64::from_le_bytes(header_size_bytes);

        self.offset += 32;

        // Read headers
        let header_end = self.offset + header_size as usize;
        if header_end > self.data.len() {
            warn!(
                "Incomplete headers at offset {} (expected {} bytes, only {} available)",
                self.offset,
                header_size,
                self.data.len() - self.offset
            );
            return None;
        }
        let headers = self.data[self.offset..header_end].to_vec();
        self.offset = header_end;

        // Read cluster data
        let cluster_end = self.offset + cluster_size as usize;
        if cluster_end > self.data.len() {
            warn!(
                "Incomplete cluster at offset {} (expected {} bytes, only {} available)",
                self.offset,
                cluster_size,
                self.data.len() - self.offset
            );
            return None;
        }
        let cluster_data = self.data[self.offset..cluster_end].to_vec();
        self.offset = cluster_end;

        // Create Fragment with headers and cluster, no fragment number yet
        let mut fragment =
            Fragment::new(headers, cluster_data, ClockTime::from_mseconds(duration_ms));
        fragment.timestamp = UNIX_EPOCH + Duration::from_millis(timestamp_ms);

        Some(Ok(fragment))
    }
}

/// Event notifications from BufferedUploadManager
#[derive(Debug)]
pub enum BufferManagerEvent {
    /// Upload complete - indicates whether to switch to streaming (true) or stay buffering (false)
    HistoricalUploadComplete {
        switch_to_streaming: bool,
    },
    UploadError(String),
}

/// Buffered upload manager - mega-cluster architecture (passive storage/upload component)
pub struct BufferedUploadManager {
    // Configuration
    stream_name: String,
    base_dir: PathBuf,
    max_buffer_duration: Duration,

    last_fragment_time: Option<SystemTime>,

    // Current mega-cluster file being appended to
    current_mega_cluster: Option<CurrentMegaClusterFile>,

    // Completed mega-cluster files ready for upload
    mega_cluster_files: VecDeque<MegaClusterFile>,

    // Media uploader
    uploader: Arc<AsyncMutex<Box<dyn MediaUploader>>>,

    // Event channel for notifying imp.rs of completion
    event_tx: Option<mpsc::UnboundedSender<BufferManagerEvent>>,
}

impl BufferedUploadManager {
    pub fn new(
        stream_name: String,
        _region: String,
        base_dir: PathBuf,
        max_buffer_duration: Duration,
        uploader: Arc<AsyncMutex<Box<dyn MediaUploader>>>,
    ) -> Result<Self> {
        // Create base directory if it doesn't exist
        std::fs::create_dir_all(&base_dir)
            .with_context(|| format!("Failed to create buffer directory: {base_dir:?}"))?;

        let mut manager = Self {
            stream_name,
            base_dir,
            max_buffer_duration,
            last_fragment_time: None,
            current_mega_cluster: None,
            mega_cluster_files: VecDeque::new(),
            uploader,
            event_tx: None,
        };

        // Scan for existing mega-cluster files from previous runs
        manager.scan_existing_mega_cluster_files()?;

        Ok(manager)
    }

    /// Set event channel for notifying imp.rs of upload completion
    pub fn set_event_channel(&mut self, tx: mpsc::UnboundedSender<BufferManagerEvent>) {
        self.event_tx = Some(tx);
    }

    /// Scan for existing mega-cluster files on disk (called during initialization)
    /// This recovers files from previous runs that may not have been uploaded
    fn scan_existing_mega_cluster_files(&mut self) -> Result<()> {
        info!(
            "Scanning {} for existing mega-cluster files",
            self.base_dir.display()
        );

        let entries = match std::fs::read_dir(&self.base_dir) {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read buffer directory: {}", e);
                return Ok(()); // Not fatal, continue without recovery
            }
        };

        let mut recovered_files = Vec::new();

        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!("Failed to read directory entry: {}", e);
                    continue;
                }
            };

            let path = entry.path();

            // Look for mega-cluster files matching pattern: {stream_name}_clusters_{timestamp}.dat
            if let Some(filename) = path.file_name().and_then(|n| n.to_str())
                && filename.starts_with(&self.stream_name)
                && filename.contains("_clusters_")
                && filename.ends_with(".dat")
            {
                let metadata = match entry.metadata() {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("Failed to get metadata for {:?}: {}", filename, e);
                        continue;
                    }
                };

                // Get file modification time as approximation
                let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);

                recovered_files.push((path.clone(), modified, metadata.len()));

                info!(
                    "Found existing mega-cluster file: {:?} ({:.2} MB)",
                    filename,
                    metadata.len() as f64 / 1_000_000.0
                );
            }
        }

        // Sort by timestamp (filename contains timestamp)
        recovered_files.sort_by_key(|(path, _, _)| path.clone());

        for (path, modified, size) in recovered_files {
            // FIXME: Major red flag in the implementation?!

            // Estimate start/end times (will be refined during parsing if needed)
            let start_time = modified;
            let end_time = modified;

            self.mega_cluster_files.push_back(MegaClusterFile {
                file_path: path,
                start_time,
                end_time,
                file_size: size,
                cluster_count: 0, // Will be determined during parsing
                uploaded: false,
                upload_attempts: 0,
                last_upload_attempt: None,
                last_upload_error: None,
            });
        }

        if !self.mega_cluster_files.is_empty() {
            let total_size: u64 = self.mega_cluster_files.iter().map(|f| f.file_size).sum();
            info!(
                "Recovered {} mega-cluster files (total {:.2} MB)",
                self.mega_cluster_files.len(),
                total_size as f64 / 1_000_000.0
            );
        } else {
            info!("No existing mega-cluster files found");
        }

        Ok(())
    }

    /// Push fragment - always buffers to disk (imp.rs handles upload decisions)
    pub async fn push_fragment(&mut self, fragment: Fragment) -> Result<()> {
        // Always buffer to mega-cluster file
        self.append_fragment_to_mega_cluster(fragment).await?;

        // FIXME: This could be done nicer?
        // Periodically cleanup old files (check every 100th call)
        // Note: Can't use fragment_number since it's None during buffering
        static mut CALL_COUNTER: u64 = 0;
        unsafe {
            CALL_COUNTER += 1;
            if CALL_COUNTER.is_multiple_of(100) {
                self.cleanup_old_mega_cluster_files().await?;
            }
        }

        Ok(())
    }

    /// Append fragment (headers + cluster) to mega-cluster file
    async fn append_fragment_to_mega_cluster(&mut self, fragment: Fragment) -> Result<()> {
        // Check if we need to rotate to a new mega-cluster file
        let should_rotate = self.should_rotate_mega_cluster_file(&fragment);

        if should_rotate {
            // Finalize current file
            if let Some(current) = self.current_mega_cluster.take() {
                self.finalize_mega_cluster_file(current).await?;
            }

            // Start new file
            self.start_new_mega_cluster_file(fragment.timestamp).await?;
        }

        // Append fragment with headers + cluster to current file
        if let Some(ref mut current) = self.current_mega_cluster {
            let header_size = fragment.header_data.len() as u64;
            let cluster_size = fragment.cluster_data.len() as u64;
            let timestamp_ms = fragment
                .timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_millis() as u64;
            let duration_ms = fragment.duration.mseconds();

            // Write metadata header: [cluster_size: u64][timestamp_ms: u64][duration_ms: u64][header_size: u64]
            current.file.write_u64_le(cluster_size).await?;
            current.file.write_u64_le(timestamp_ms).await?;
            current.file.write_u64_le(duration_ms).await?;
            current.file.write_u64_le(header_size).await?;

            // Write MKV headers
            current
                .file
                .write_all(fragment.header_data.as_ref())
                .await
                .context("Failed to write headers to mega-cluster file")?;

            // Write cluster data
            current
                .file
                .write_all(fragment.cluster_data.as_ref())
                .await
                .context("Failed to write cluster to mega-cluster file")?;

            current.file.flush().await?;

            current.current_size += 32 + header_size + cluster_size; // metadata (4 x u64) + headers + cluster
            current.last_fragment_time = fragment.timestamp;
            current.cluster_count += 1;

            debug!(
                "Appended fragment to mega-cluster (headers: {} bytes, cluster: {} bytes, total: {:.2} MB)",
                header_size,
                cluster_size,
                current.current_size as f64 / 1_000_000.0
            );
        } else {
            return Err(anyhow::anyhow!(
                "No current mega-cluster file (should not happen)"
            ));
        }

        self.last_fragment_time = Some(fragment.timestamp);

        Ok(())
    }

    /// Check if we should rotate to a new mega-cluster file
    fn should_rotate_mega_cluster_file(&self, _fragment: &Fragment) -> bool {
        if let Some(ref current) = self.current_mega_cluster {
            let elapsed = SystemTime::now()
                .duration_since(current.file_created_at)
                .unwrap_or(Duration::ZERO);

            // Rotate if duration or size threshold exceeded (based on wall clock time)
            elapsed >= MEGA_CLUSTER_DURATION || current.current_size >= MEGA_CLUSTER_MAX_SIZE
        } else {
            true // No current file, need to start one
        }
    }

    /// Start a new mega-cluster file
    async fn start_new_mega_cluster_file(&mut self, start_time: SystemTime) -> Result<()> {
        let now = SystemTime::now();
        // Use milliseconds for unique filenames (avoids collision when creating multiple files in same second)
        let timestamp_ms = now.duration_since(UNIX_EPOCH)?.as_millis();
        let filename = format!("{}_clusters_{}.dat", self.stream_name, timestamp_ms);
        let file_path = self.base_dir.join(&filename);

        info!("Starting new mega-cluster file: {:?}", filename);

        let file = tokio::fs::File::create(&file_path)
            .await
            .context("Failed to create mega-cluster file")?;

        self.current_mega_cluster = Some(CurrentMegaClusterFile {
            file,
            file_path,
            file_created_at: now,
            start_time,
            last_fragment_time: start_time,
            current_size: 0,
            cluster_count: 0,
        });

        Ok(())
    }

    /// Finalize current mega-cluster file and add to upload queue
    async fn finalize_mega_cluster_file(
        &mut self,
        mut current: CurrentMegaClusterFile,
    ) -> Result<()> {
        // Flush buffers and sync to disk
        current.file.flush().await?;
        current.file.sync_all().await?; // Ensure data is written to disk
        drop(current.file); // Close file

        info!(
            "Finalized mega-cluster file: {:?} ({} clusters, {:.2} MB)",
            current.file_path.file_name(),
            current.cluster_count,
            current.current_size as f64 / 1_000_000.0
        );

        // Add to upload queue
        self.mega_cluster_files.push_back(MegaClusterFile {
            file_path: current.file_path,
            start_time: current.start_time,
            end_time: current.last_fragment_time,
            file_size: current.current_size,
            cluster_count: current.cluster_count,
            uploaded: false,
            upload_attempts: 0,
            last_upload_attempt: None,
            last_upload_error: None,
        });

        Ok(())
    }

    /// Cleanup old mega-cluster files that exceed max buffer duration
    async fn cleanup_old_mega_cluster_files(&mut self) -> Result<()> {
        let now = SystemTime::now();

        while let Some(file) = self.mega_cluster_files.front() {
            let age = now
                .duration_since(file.start_time)
                .unwrap_or(Duration::ZERO);

            // FIXME: I don't think we should check for `file.uploaded` here?
            if age > self.max_buffer_duration && file.uploaded {
                if let Some(file) = self.mega_cluster_files.pop_front() {
                    info!(
                        "Removing old mega-cluster file: {:?}",
                        file.file_path.file_name()
                    );
                    if let Err(e) = tokio::fs::remove_file(&file.file_path).await {
                        warn!(
                            "Failed to remove mega-cluster file {:?}: {}",
                            file.file_path, e
                        );
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Trigger upload of buffered mega-cluster files
    ///
    /// Time range semantics:
    /// - `None, None` → Upload ALL → switch to Immediate (live streaming)
    /// - `Some(X), None` → Upload from X onwards → switch to Immediate
    /// - `None, Some(Y)` → Upload up to Y → stay BufferOnly
    /// - `Some(X), Some(Y)` → Upload window [X,Y] → stay BufferOnly
    ///
    /// Implementation flow to avoid timestamp ordering violations:
    /// 1. Finalize current mega-cluster
    /// 2. Start NEW mega-cluster (continue buffering during upload)
    /// 3. Upload historical files BLOCKING (in chronological order)
    /// 4. Finalize NEW mega-cluster
    /// 5. Upload NEW mega-cluster
    /// 6. Notify completion with target mode
    ///
    /// This ensures fragments always arrive in chronological order (no out-of-order timestamps).
    pub async fn trigger_upload_all(
        &mut self,
        start: Option<SystemTime>,
        end: Option<SystemTime>,
    ) -> Result<()> {
        info!("Upload trigger received: start={:?}, end={:?}", start, end);

        // Determine target mode based on end parameter
        // If end is specified, stay in BufferOnly mode
        // If no end, switch to Immediate mode (live streaming)
        let switch_to_streaming = end.is_none();
        info!(
            "Upload target mode: {}",
            if switch_to_streaming {
                "Immediate (live streaming)"
            } else {
                "BufferOnly (continue buffering)"
            }
        );

        // STEP 1: Finalize current mega-cluster file (captures everything in RAM/disk)
        if let Some(current) = self.current_mega_cluster.take() {
            info!(
                "Finalizing current mega-cluster with {} clusters",
                current.cluster_count
            );
            self.finalize_mega_cluster_file(current).await?;
        }

        // STEP 2: Find mega-cluster files within time range
        let files_to_upload: Vec<MegaClusterFile> = self
            .mega_cluster_files
            .iter()
            .filter(|file| {
                !file.uploaded
                    && file.upload_attempts < MAX_UPLOAD_RETRIES
                    && Self::overlaps_time_range(file, start, end)
            })
            .cloned()
            .collect();

        if files_to_upload.is_empty() {
            info!("No mega-cluster files in requested time range");
            // Notify completion even if nothing to upload
            if let Some(tx) = &self.event_tx {
                let _ = tx.send(BufferManagerEvent::HistoricalUploadComplete {
                    switch_to_streaming,
                });
            }
            return Ok(());
        }

        info!(
            "Found {} mega-cluster files to upload (BLOCKING)",
            files_to_upload.len()
        );

        // STEP 3: Start NEW mega-cluster immediately
        // While we upload historical data, new fragments buffer to this NEW file
        self.start_new_mega_cluster_file(SystemTime::now()).await?;
        info!("Started NEW mega-cluster for incoming fragments during upload");

        // STEP 4: Upload historical files BLOCKING (foreground, in chronological order)
        let mut uploaded_count = 0;
        let mut failed_count = 0;

        for file in &files_to_upload {
            info!(
                "Uploading historical mega-cluster: {:?}",
                file.file_path.file_name(),
            );

            let result = Self::upload_mega_cluster_file(file, Arc::clone(&self.uploader)).await;

            match result {
                Ok(()) => {
                    info!("✓ Uploaded: {:?}", file.file_path.file_name());
                    self.mark_mega_cluster_uploaded(&file.file_path)?;
                    uploaded_count += 1;
                }
                Err(e) => {
                    warn!("✗ Upload failed: {:?} - {}", file.file_path.file_name(), e);
                    self.mark_mega_cluster_failed(&file.file_path, e.to_string())?;
                    failed_count += 1;

                    // Notify error but continue with next file
                    if let Some(tx) = &self.event_tx {
                        let _ = tx.send(BufferManagerEvent::UploadError(e.to_string()));
                    }
                }
            }
        }

        info!(
            "Historical upload complete: {} succeeded, {} failed",
            uploaded_count, failed_count
        );

        // STEP 5: Finalize the NEW mega-cluster created in step 3
        // (It contains fragments that arrived during historical upload)
        if let Some(current) = self.current_mega_cluster.take() {
            if current.cluster_count > 0 {
                info!(
                    "Finalizing NEW mega-cluster with {} clusters buffered during upload",
                    current.cluster_count
                );
                self.finalize_mega_cluster_file(current).await?;

                // STEP 6: Upload this NEW mega-cluster too
                if let Some(new_file) = self.mega_cluster_files.back()
                    && !new_file.uploaded
                {
                    info!(
                        "Uploading NEW mega-cluster: {:?}",
                        new_file.file_path.file_name(),
                    );

                    // Clone file_path before upload to avoid borrow conflicts
                    let file_path = new_file.file_path.clone();

                    let result =
                        Self::upload_mega_cluster_file(new_file, Arc::clone(&self.uploader)).await;

                    match result {
                        Ok(()) => {
                            info!("✓ Uploaded NEW mega-cluster");
                            self.mark_mega_cluster_uploaded(&file_path)?;
                        }
                        Err(e) => {
                            warn!("✗ NEW mega-cluster upload failed: {}", e);
                            self.mark_mega_cluster_failed(&file_path, e.to_string())?;

                            if let Some(tx) = &self.event_tx {
                                let _ = tx.send(BufferManagerEvent::UploadError(e.to_string()));
                            }
                        }
                    }
                }
            } else {
                info!("NEW mega-cluster was empty (no fragments during upload)");
            }
        }

        // STEP 7: Notify completion with target mode
        info!(
            "✓ Upload flow complete - all historical data uploaded in order (switch_to_streaming: {})",
            switch_to_streaming
        );
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(BufferManagerEvent::HistoricalUploadComplete {
                switch_to_streaming,
            });
        }

        Ok(())
    }

    /// Upload a single mega-cluster file by streaming fragments through persistent connection
    /// KvsConnection handles fragment numbering and header inclusion automatically
    async fn upload_mega_cluster_file(
        file: &MegaClusterFile,
        uploader: Arc<AsyncMutex<Box<dyn MediaUploader>>>,
    ) -> Result<()> {
        info!(
            "Uploading mega-cluster file: {:?} ({} clusters, {:.2} MB)",
            file.file_path.file_name(),
            file.cluster_count,
            file.file_size as f64 / 1_000_000.0
        );

        // Load and iterate through fragments (headers + cluster already in file)
        let reader = ClusterFileReader::from_path(&file.file_path).await?;

        let mut fragments_uploaded = 0;

        for result in reader {
            let fragment = result?; // Fragment has headers + cluster, no fragment_number yet

            // Upload (KvsConnection assigns fragment number and decides on headers)
            let uploader = uploader.lock().await;
            uploader.put_fragment(&fragment).await?;
            drop(uploader);

            fragments_uploaded += 1;

            // Rate limit: sleep 200ms after EVERY fragment to respect KVS limits
            // This gives ~5 fragments/second, allowing KVS to process and acknowledge
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        info!(
            "✓ Uploaded {} clusters from {:?}",
            fragments_uploaded,
            file.file_path.file_name(),
        );

        Ok(())
    }

    /// Check if mega-cluster file overlaps with time range
    fn overlaps_time_range(
        file: &MegaClusterFile,
        start: Option<SystemTime>,
        end: Option<SystemTime>,
    ) -> bool {
        // File overlaps if:
        // - file.end >= range.start (or no range.start)
        // - file.start <= range.end (or no range.end)

        if let Some(range_start) = start
            && file.end_time < range_start
        {
            return false;
        }

        if let Some(range_end) = end
            && file.start_time > range_end
        {
            return false;
        }

        true
    }

    fn mark_mega_cluster_uploaded(&mut self, path: &PathBuf) -> Result<()> {
        if let Some(file) = self
            .mega_cluster_files
            .iter_mut()
            .find(|f| &f.file_path == path)
        {
            file.uploaded = true;
            file.last_upload_attempt = Some(SystemTime::now());
            file.last_upload_error = None;

            info!("Marked mega-cluster as uploaded: {:?}", path.file_name());

            // Delete file after successful upload (free disk space)
            if let Err(e) = std::fs::remove_file(path) {
                warn!(
                    "Failed to delete uploaded mega-cluster {:?}: {}",
                    path.file_name(),
                    e
                );
            } else {
                info!("Deleted uploaded mega-cluster: {:?}", path.file_name());
            }
        }
        Ok(())
    }

    fn mark_mega_cluster_failed(&mut self, path: &PathBuf, error: String) -> Result<()> {
        if let Some(file) = self
            .mega_cluster_files
            .iter_mut()
            .find(|f| &f.file_path == path)
        {
            file.upload_attempts += 1;
            file.last_upload_attempt = Some(SystemTime::now());
            file.last_upload_error = Some(error.clone());

            warn!(
                "Mega-cluster upload failed (attempt {}): {:?} - {}",
                file.upload_attempts,
                path.file_name(),
                error
            );
        }
        Ok(())
    }

    /// Get buffer statistics
    pub fn stats(&self) -> BufferStats {
        let total_bytes: u64 = self.mega_cluster_files.iter().map(|f| f.file_size).sum();
        let current_file_size = self
            .current_mega_cluster
            .as_ref()
            .map(|c| c.current_size)
            .unwrap_or(0);

        BufferStats {
            total_clusters: self
                .mega_cluster_files
                .iter()
                .map(|f| f.cluster_count)
                .sum(),
            total_bytes: total_bytes + current_file_size,
            mega_cluster_files: self.mega_cluster_files.len(),
        }
    }
}

#[derive(Debug)]
pub struct BufferStats {
    pub total_clusters: usize,
    pub total_bytes: u64,
    pub mega_cluster_files: usize,
}
