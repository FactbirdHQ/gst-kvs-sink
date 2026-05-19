//! Tests for mega-cluster buffered upload manager architecture
//!
//! Key behaviors tested:
//! - Mega-cluster file creation with fixed-header format: [u64: size][u64: timestamp_ms][u64: duration_ms][cluster_bytes]
//! - Triggered upload with chronological ordering (blocking flow)
//! - Time range semantics for selective uploads
//! - Crash recovery via ClusterFileReader
//! - File rotation based on duration/size thresholds
//! - Cleanup of old uploaded files

use anyhow::Result;
use gstreamer::ClockTime;
use gstrskvssink::advanced::{BufferedUploadManager, ClusterFileReader, Fragment, MediaUploader};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;

mod common;
use common::test_media_uploader::TestMediaUploader;

use crate::common::init_gstreamer;

/// Helper to create a test fragment (with empty headers for testing)
fn create_fragment(_number: u64, data: Vec<u8>, timestamp: SystemTime) -> Fragment {
    let mut fragment = Fragment::new(
        vec![], // Empty headers for testing
        data,
        ClockTime::from_seconds(2),
    );
    fragment.timestamp = timestamp;
    fragment
}

// =============================================================================
// MEGA-CLUSTER FILE FORMAT TESTS
// =============================================================================

#[tokio::test]
async fn test_mega_cluster_file_creation() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader = TestMediaUploader::new(false, false);

    let mut buffer_mgr = BufferedUploadManager::new(
        "test-stream".to_string(),
        "us-east-1".to_string(),
        temp_dir.path().to_path_buf(),
        Duration::from_secs(3600),
        Arc::new(AsyncMutex::new(Box::new(uploader) as Box<dyn MediaUploader>)),
    )
    .unwrap();

    // Push fragments to create a mega-cluster file
    for i in 1..=5 {
        let timestamp = SystemTime::now();
        let fragment = create_fragment(i, vec![i as u8; 100], timestamp);
        buffer_mgr.push_fragment(fragment).await.unwrap();
    }

    // Wait for file creation (mega-cluster files are created on first fragment)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify .dat file was created (not .mkv)
    let dat_files: Vec<_> = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.starts_with("test-stream") && n.ends_with(".dat"))
                .unwrap_or(false)
        })
        .collect();

    assert_eq!(
        dat_files.len(),
        1,
        "Should have created one mega-cluster .dat file"
    );

    // Note: stats() only counts finalized mega-cluster files, not the current one being written
    // The current file is in-progress and won't be counted until trigger_upload_all() is called
    // So we just verify the file exists on disk
    assert!(
        dat_files[0].metadata().unwrap().len() > 500,
        "File should contain data"
    );
}

#[tokio::test]
async fn test_cluster_file_reader_format() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader = TestMediaUploader::new(false, false);
    uploader
        .initialize("test-stream", "us-east-1")
        .await
        .unwrap();

    let test_data = [
        (
            1u64,
            vec![1, 2, 3, 4],
            SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
        ),
        (
            2u64,
            vec![5, 6, 7, 8],
            SystemTime::UNIX_EPOCH + Duration::from_secs(2000),
        ),
        (
            3u64,
            vec![9, 10, 11, 12],
            SystemTime::UNIX_EPOCH + Duration::from_secs(3000),
        ),
    ];

    let dat_file_path = {
        let mut buffer_mgr = BufferedUploadManager::new(
            "test-stream".to_string(),
            "us-east-1".to_string(),
            temp_dir.path().to_path_buf(),
            Duration::from_secs(3600),
            Arc::new(AsyncMutex::new(Box::new(uploader) as Box<dyn MediaUploader>)),
        )
        .unwrap();

        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
        buffer_mgr.set_event_channel(event_tx);

        // Push fragments
        for (num, data, timestamp) in test_data.iter() {
            let fragment = create_fragment(*num, data.clone(), *timestamp);
            buffer_mgr.push_fragment(fragment).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Finalize current mega-cluster file (but don't upload - use time range that excludes our test data)
        // Our test data has timestamps from UNIX_EPOCH, so use a range that excludes them
        let future_start = SystemTime::UNIX_EPOCH + Duration::from_secs(10000);
        let future_end = SystemTime::UNIX_EPOCH + Duration::from_secs(20000);
        buffer_mgr
            .trigger_upload_all(Some(future_start), Some(future_end))
            .await
            .unwrap();

        // Find the finalized .dat file path
        let mut dat_files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with(".dat"))
                    .unwrap_or(false)
            })
            .collect();

        // Sort by modification time and get the first (oldest) file
        dat_files.sort_by_key(|e| e.metadata().unwrap().modified().unwrap());
        dat_files[0].path()
    };

    // Now read with ClusterFileReader
    let reader = ClusterFileReader::from_path(&dat_file_path).await.unwrap();
    let clusters: Vec<_> = reader.collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(clusters.len(), 3, "Should read 3 clusters");

    // Verify data integrity
    for (i, fragment) in clusters.iter().enumerate() {
        let expected_timestamp_ms = test_data[i]
            .2
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let actual_timestamp_ms = fragment
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert_eq!(
            actual_timestamp_ms, expected_timestamp_ms,
            "Timestamp mismatch for cluster {i}"
        );
        assert_eq!(
            fragment.duration.mseconds(),
            2000,
            "Duration should be 2000ms (2 seconds)"
        );
        assert_eq!(
            fragment.cluster_data.as_ref(),
            &test_data[i].1,
            "Data mismatch for cluster {i}"
        );
    }
}

#[tokio::test]
async fn test_cluster_file_reader_crash_recovery() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let test_file = temp_dir.path().join("test_corrupted.dat");

    // Create a file with complete and incomplete records
    // NEW FORMAT: [cluster_size: u64][timestamp_ms: u64][duration_ms: u64][header_size: u64][headers][cluster]
    let mut file_data = Vec::new();

    // Complete record 1: [cluster_size: 4][timestamp: 1000][duration: 2000][header_size: 0][cluster: 4 bytes]
    file_data.extend_from_slice(&4u64.to_le_bytes()); // cluster_size
    file_data.extend_from_slice(&1000u64.to_le_bytes()); // timestamp_ms
    file_data.extend_from_slice(&2000u64.to_le_bytes()); // duration_ms
    file_data.extend_from_slice(&0u64.to_le_bytes()); // header_size (no headers in test)
    file_data.extend_from_slice(&[1, 2, 3, 4]); // cluster data

    // Complete record 2: [cluster_size: 3][timestamp: 2000][duration: 2000][header_size: 0][cluster: 3 bytes]
    file_data.extend_from_slice(&3u64.to_le_bytes()); // cluster_size
    file_data.extend_from_slice(&2000u64.to_le_bytes()); // timestamp_ms
    file_data.extend_from_slice(&2000u64.to_le_bytes()); // duration_ms
    file_data.extend_from_slice(&0u64.to_le_bytes()); // header_size (no headers in test)
    file_data.extend_from_slice(&[5, 6, 7]); // cluster data

    // Incomplete record 3: [cluster_size: 10][timestamp: 3000][duration: 2000][header_size: 0][cluster: only 5 bytes - CRASH!]
    file_data.extend_from_slice(&10u64.to_le_bytes()); // cluster_size
    file_data.extend_from_slice(&3000u64.to_le_bytes()); // timestamp_ms
    file_data.extend_from_slice(&2000u64.to_le_bytes()); // duration_ms
    file_data.extend_from_slice(&0u64.to_le_bytes()); // header_size (no headers in test)
    file_data.extend_from_slice(&[8, 9, 10, 11, 12]); // Only 5 of 10 bytes - incomplete!

    tokio::fs::write(&test_file, &file_data).await.unwrap();

    // Read with ClusterFileReader - should stop at incomplete record
    let reader = ClusterFileReader::from_path(&test_file).await.unwrap();
    let clusters: Vec<_> = reader.collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(
        clusters.len(),
        2,
        "Should only read 2 complete clusters, skipping incomplete"
    );

    // Verify first cluster
    let timestamp_0_ms = clusters[0]
        .timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(timestamp_0_ms, 1000); // timestamp
    assert_eq!(clusters[0].duration.mseconds(), 2000); // duration
    assert_eq!(clusters[0].cluster_data.as_ref(), &vec![1, 2, 3, 4]); // data

    // Verify second cluster
    let timestamp_1_ms = clusters[1]
        .timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(timestamp_1_ms, 2000); // timestamp
    assert_eq!(clusters[1].duration.mseconds(), 2000); // duration
    assert_eq!(clusters[1].cluster_data.as_ref(), &vec![5, 6, 7]); // data
}

// =============================================================================
// TIME RANGE SEMANTICS TESTS
// =============================================================================

#[tokio::test]
async fn test_upload_all_switches_to_streaming() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader = TestMediaUploader::new(false, false);
    uploader
        .initialize("test-stream", "us-east-1")
        .await
        .unwrap();
    let uploader_check = uploader.clone();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut buffer_mgr = BufferedUploadManager::new(
        "test-stream".to_string(),
        "us-east-1".to_string(),
        temp_dir.path().to_path_buf(),
        Duration::from_secs(3600),
        Arc::new(AsyncMutex::new(Box::new(uploader) as Box<dyn MediaUploader>)),
    )
    .unwrap();

    buffer_mgr.set_event_channel(event_tx);

    // Push some fragments
    for i in 1..=5 {
        let fragment = create_fragment(i, vec![i as u8; 100], SystemTime::now());
        buffer_mgr.push_fragment(fragment).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Trigger upload with (None, None) → should switch to streaming
    buffer_mgr.trigger_upload_all(None, None).await.unwrap();

    // Check event
    let event = event_rx.recv().await.unwrap();
    match event {
        gstrskvssink::advanced::BufferManagerEvent::HistoricalUploadComplete {
            switch_to_streaming,
        } => {
            assert!(
                switch_to_streaming,
                "Should switch to streaming when end=None"
            );
        }
        _ => panic!("Expected HistoricalUploadComplete event"),
    }

    // Verify fragments were uploaded via trigger_upload_all
    tokio::time::sleep(Duration::from_millis(500)).await;
    let count = uploader_check.fragment_count();
    println!("DEBUG test_upload_all: Fragment count = {count}");
    assert!(
        count >= 5,
        "Should have uploaded at least 5 fragments, got {count}"
    );
}

#[tokio::test]
async fn test_upload_from_time_switches_to_streaming() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader = TestMediaUploader::new(false, false);
    uploader
        .initialize("test-stream", "us-east-1")
        .await
        .unwrap();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut buffer_mgr = BufferedUploadManager::new(
        "test-stream".to_string(),
        "us-east-1".to_string(),
        temp_dir.path().to_path_buf(),
        Duration::from_secs(3600),
        Arc::new(AsyncMutex::new(Box::new(uploader) as Box<dyn MediaUploader>)),
    )
    .unwrap();

    buffer_mgr.set_event_channel(event_tx);

    // Push fragments
    for i in 1..=3 {
        let fragment = create_fragment(i, vec![i as u8; 100], SystemTime::now());
        buffer_mgr.push_fragment(fragment).await.unwrap();
    }

    let start_time = SystemTime::now() - Duration::from_secs(10);

    // Trigger upload with (Some(X), None) → should switch to streaming
    buffer_mgr
        .trigger_upload_all(Some(start_time), None)
        .await
        .unwrap();

    // Check event
    let event = event_rx.recv().await.unwrap();
    match event {
        gstrskvssink::advanced::BufferManagerEvent::HistoricalUploadComplete {
            switch_to_streaming,
        } => {
            assert!(
                switch_to_streaming,
                "Should switch to streaming when end=None"
            );
        }
        _ => panic!("Expected HistoricalUploadComplete event"),
    }
}

#[tokio::test]
async fn test_upload_until_time_stays_buffering() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader = TestMediaUploader::new(false, false);
    uploader
        .initialize("test-stream", "us-east-1")
        .await
        .unwrap();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut buffer_mgr = BufferedUploadManager::new(
        "test-stream".to_string(),
        "us-east-1".to_string(),
        temp_dir.path().to_path_buf(),
        Duration::from_secs(3600),
        Arc::new(AsyncMutex::new(Box::new(uploader) as Box<dyn MediaUploader>)),
    )
    .unwrap();

    buffer_mgr.set_event_channel(event_tx);

    // Push fragments
    for i in 1..=3 {
        let fragment = create_fragment(i, vec![i as u8; 100], SystemTime::now());
        buffer_mgr.push_fragment(fragment).await.unwrap();
    }

    let end_time = SystemTime::now() + Duration::from_secs(3600);

    // Trigger upload with (None, Some(Y)) → should stay in BufferOnly
    buffer_mgr
        .trigger_upload_all(None, Some(end_time))
        .await
        .unwrap();

    // Check event
    let event = event_rx.recv().await.unwrap();
    match event {
        gstrskvssink::advanced::BufferManagerEvent::HistoricalUploadComplete {
            switch_to_streaming,
        } => {
            assert!(
                !switch_to_streaming,
                "Should stay in BufferOnly when end is specified"
            );
        }
        _ => panic!("Expected HistoricalUploadComplete event"),
    }
}

#[tokio::test]
async fn test_upload_time_window_stays_buffering() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader = TestMediaUploader::new(false, false);
    uploader
        .initialize("test-stream", "us-east-1")
        .await
        .unwrap();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut buffer_mgr = BufferedUploadManager::new(
        "test-stream".to_string(),
        "us-east-1".to_string(),
        temp_dir.path().to_path_buf(),
        Duration::from_secs(3600),
        Arc::new(AsyncMutex::new(Box::new(uploader) as Box<dyn MediaUploader>)),
    )
    .unwrap();

    buffer_mgr.set_event_channel(event_tx);

    // Push fragments
    for i in 1..=3 {
        let fragment = create_fragment(i, vec![i as u8; 100], SystemTime::now());
        buffer_mgr.push_fragment(fragment).await.unwrap();
    }

    let start_time = SystemTime::now() - Duration::from_secs(10);
    let end_time = SystemTime::now() + Duration::from_secs(3600);

    // Trigger upload with (Some(X), Some(Y)) → should stay in BufferOnly
    buffer_mgr
        .trigger_upload_all(Some(start_time), Some(end_time))
        .await
        .unwrap();

    // Check event
    let event = event_rx.recv().await.unwrap();
    match event {
        gstrskvssink::advanced::BufferManagerEvent::HistoricalUploadComplete {
            switch_to_streaming,
        } => {
            assert!(
                !switch_to_streaming,
                "Should stay in BufferOnly when both start and end are specified"
            );
        }
        _ => panic!("Expected HistoricalUploadComplete event"),
    }
}

// =============================================================================
// BLOCKING UPLOAD FLOW WITH CHRONOLOGICAL ORDERING
// =============================================================================

#[tokio::test]
async fn test_blocking_upload_chronological_order() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader = TestMediaUploader::new(false, false); // Disable MKV validation (dummy test data)
    uploader
        .initialize("test-stream", "us-east-1")
        .await
        .unwrap();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut buffer_mgr = BufferedUploadManager::new(
        "test-stream".to_string(),
        "us-east-1".to_string(),
        temp_dir.path().to_path_buf(),
        Duration::from_secs(3600),
        Arc::new(AsyncMutex::new(
            Box::new(uploader.clone()) as Box<dyn MediaUploader>
        )),
    )
    .unwrap();

    buffer_mgr.set_event_channel(event_tx);

    // Push historical fragments with timestamps
    let base_time = SystemTime::now();
    for i in 1..=10 {
        let timestamp = base_time + Duration::from_secs(i);
        let fragment = create_fragment(i, vec![i as u8; 100], timestamp);
        buffer_mgr.push_fragment(fragment).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Debug: check stats
    let stats = buffer_mgr.stats();
    println!(
        "DEBUG: Before upload - total_clusters={}, total_bytes={}, files={}",
        stats.total_clusters, stats.total_bytes, stats.mega_cluster_files
    );

    // Trigger upload - this tests the blocking upload flow
    buffer_mgr.trigger_upload_all(None, None).await.unwrap();

    // Check completion event
    let event = event_rx.recv().await.unwrap();
    match event {
        gstrskvssink::advanced::BufferManagerEvent::HistoricalUploadComplete {
            switch_to_streaming,
        } => {
            assert!(switch_to_streaming);
        }
        _ => panic!("Expected HistoricalUploadComplete event"),
    }

    // Verify fragments were uploaded
    let fragment_count = uploader.fragment_count();
    println!("DEBUG: Fragment count after upload: {fragment_count}");
    assert!(
        fragment_count >= 10,
        "Should have uploaded at least 10 fragments, got {fragment_count}"
    );
}

#[tokio::test]
#[ignore]
async fn test_new_mega_cluster_created_during_upload() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader = TestMediaUploader::new(false, false);
    uploader
        .initialize("test-stream", "us-east-1")
        .await
        .unwrap();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut buffer_mgr = BufferedUploadManager::new(
        "test-stream".to_string(),
        "us-east-1".to_string(),
        temp_dir.path().to_path_buf(),
        Duration::from_secs(3600),
        Arc::new(AsyncMutex::new(
            Box::new(uploader.clone()) as Box<dyn MediaUploader>
        )),
    )
    .unwrap();

    buffer_mgr.set_event_channel(event_tx);

    // Push historical fragments
    for i in 1..=5 {
        let fragment = create_fragment(i, vec![i as u8; 100], SystemTime::now());
        buffer_mgr.push_fragment(fragment).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start upload in background task
    let mgr_clone = Arc::new(AsyncMutex::new(buffer_mgr));
    let upload_handle = {
        let mgr = Arc::clone(&mgr_clone);
        tokio::spawn(async move {
            mgr.lock()
                .await
                .trigger_upload_all(None, None)
                .await
                .unwrap();
        })
    };

    // While upload is happening, push NEW fragments
    // (simulating continuous streaming during upload)
    tokio::time::sleep(Duration::from_millis(50)).await;
    for i in 6..=8 {
        let fragment = create_fragment(i, vec![i as u8; 100], SystemTime::now());
        mgr_clone
            .lock()
            .await
            .push_fragment(fragment)
            .await
            .unwrap();
    }

    // Wait for upload to complete
    upload_handle.await.unwrap();

    // Check completion event
    let event = event_rx.recv().await.unwrap();
    match event {
        gstrskvssink::advanced::BufferManagerEvent::HistoricalUploadComplete { .. } => {}
        _ => panic!("Expected HistoricalUploadComplete event"),
    }

    // Verify all fragments were uploaded (historical + new)
    let total_uploaded = uploader.fragment_count();
    assert!(
        total_uploaded >= 8,
        "Should have uploaded at least 8 fragments (5 historical + 3 new), got {total_uploaded}"
    );
}

// =============================================================================
// FILE ROTATION AND CLEANUP TESTS
// =============================================================================

#[tokio::test]
async fn test_stats_reporting() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader = TestMediaUploader::new(false, false);

    let mut buffer_mgr = BufferedUploadManager::new(
        "test-stream".to_string(),
        "us-east-1".to_string(),
        temp_dir.path().to_path_buf(),
        Duration::from_secs(3600),
        Arc::new(AsyncMutex::new(Box::new(uploader) as Box<dyn MediaUploader>)),
    )
    .unwrap();

    // Initial stats
    let stats = buffer_mgr.stats();
    assert_eq!(stats.total_clusters, 0);
    assert_eq!(stats.mega_cluster_files, 0);

    // Push fragments
    for i in 1..=10 {
        let fragment = create_fragment(i, vec![i as u8; 100], SystemTime::now());
        buffer_mgr.push_fragment(fragment).await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Trigger finalization to make stats accurate
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
    buffer_mgr.set_event_channel(event_tx);

    // Trigger upload with end time to stay in BufferOnly mode and finalize current file
    buffer_mgr
        .trigger_upload_all(None, Some(SystemTime::now() + Duration::from_secs(3600)))
        .await
        .unwrap();

    // Check stats after finalization
    let stats = buffer_mgr.stats();
    assert!(
        stats.total_clusters >= 10,
        "Should have at least 10 clusters buffered"
    );
    assert!(stats.total_bytes >= 1000, "Should have at least 1000 bytes");
    assert!(
        stats.mega_cluster_files >= 1,
        "Should have at least 1 mega-cluster file"
    );
}

#[tokio::test]
async fn test_existing_file_recovery_on_startup() {
    init_gstreamer().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let uploader1 = TestMediaUploader::new(false, false);

    // First manager: create some mega-cluster files
    {
        let mut buffer_mgr = BufferedUploadManager::new(
            "test-stream".to_string(),
            "us-east-1".to_string(),
            temp_dir.path().to_path_buf(),
            Duration::from_secs(3600),
            Arc::new(AsyncMutex::new(
                Box::new(uploader1) as Box<dyn MediaUploader>
            )),
        )
        .unwrap();

        // Push fragments to create files
        for i in 1..=5 {
            let fragment = create_fragment(i, vec![i as u8; 100], SystemTime::now());
            buffer_mgr.push_fragment(fragment).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify files exist
        let files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("dat"))
            .collect();

        assert!(!files.is_empty(), "Should have created .dat files");
    } // Drop first manager

    // Second manager: should recover existing files
    let uploader2 = TestMediaUploader::new(false, false);
    let buffer_mgr2 = BufferedUploadManager::new(
        "test-stream".to_string(),
        "us-east-1".to_string(),
        temp_dir.path().to_path_buf(),
        Duration::from_secs(3600),
        Arc::new(AsyncMutex::new(
            Box::new(uploader2) as Box<dyn MediaUploader>
        )),
    )
    .unwrap();

    // Check stats - should have recovered files
    let stats = buffer_mgr2.stats();
    assert!(
        stats.mega_cluster_files > 0,
        "Should have recovered existing mega-cluster files"
    );
}
