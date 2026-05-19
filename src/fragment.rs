use bytes::Bytes;
use gstreamer::ClockTime;
use std::time::SystemTime;

/// Video fragment with efficient cloning via Bytes
///
/// Fragment separates MKV headers from cluster data for explicit control over
/// when headers are included. This is critical for KVS streaming where:
/// - First fragment of each HTTP connection requires full MKV headers
/// - Subsequent fragments on same connection contain only cluster data
/// - Disk storage includes both headers and cluster data
///
/// Both fields use `Bytes` which provides reference counting for cheap cloning.
///
/// Fragment numbers are assigned by KvsConnection at send time, so fragment_number
/// is None until the fragment is sent to KVS.
#[derive(Clone, Debug)]
pub struct Fragment {
    /// MKV headers (EBML + Segment + Info + Tracks)
    /// Always present from MkvWriter (cheap to clone via refcounting)
    pub header_data: Bytes,

    /// Pure cluster data (Cluster element + SimpleBlocks)
    /// Always present (cheap to clone via refcounting)
    pub cluster_data: Bytes,

    /// Fragment sequence number (assigned by KvsConnection at send time)
    /// None when created by MkvWriter or loaded from storage
    pub fragment_number: Option<u64>,

    /// Duration of this fragment
    pub duration: ClockTime,

    /// Timestamp when this fragment was created
    pub timestamp: SystemTime,
}

impl Fragment {
    /// Create fragment with headers and cluster data (primary constructor)
    /// Used by MkvWriter and BufferedUploadManager
    /// Fragment number is None - will be assigned by KvsConnection at send time
    pub fn new(header_data: Vec<u8>, cluster_data: Vec<u8>, duration: ClockTime) -> Self {
        Self {
            header_data: Bytes::from(header_data),
            cluster_data: Bytes::from(cluster_data),
            fragment_number: None,
            duration,
            timestamp: SystemTime::now(),
        }
    }

    /// Assign fragment number (used internally by KvsConnection)
    pub fn with_number(mut self, num: u64) -> Self {
        self.fragment_number = Some(num);
        self
    }

    /// Check if this fragment has MKV headers
    pub fn has_headers(&self) -> bool {
        !self.header_data.is_empty()
    }

    /// Get total size (headers + cluster) in bytes
    pub fn total_size(&self) -> usize {
        self.header_data.len() + self.cluster_data.len()
    }

    /// Get cluster size only (without headers) in bytes
    pub fn cluster_size(&self) -> usize {
        self.cluster_data.len()
    }

    /// Get size in bytes (alias for total_size for backwards compatibility)
    pub fn size(&self) -> usize {
        self.total_size()
    }
}
