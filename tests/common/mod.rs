//! Common test utilities for stream-manager tests

pub mod failable_uploader;
pub mod test_media_uploader;

use anyhow::Result;
use gstreamer as gst;

/// Initialize GStreamer for tests
pub fn init_gstreamer() -> Result<()> {
    // Use try_init() to avoid panic if subscriber is already initialized
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "debug,serial_test::rwlock=warn",
        ))
        .try_init();

    gst::init()?;
    Ok(())
}
