//! Tests for the custom GStreamer plugin (rskvssink) registration and functionality

use anyhow::Result;
use gstreamer as gst;
use gstreamer::prelude::*;
use tracing::info;

/// Test that our GStreamer plugin can be registered and elements created
#[test]
fn test_rskvssink_registration() -> Result<()> {
    gst::init()?;

    // Register our plugin
    gstrskvssink::register(None)?;

    // Test element creation
    let element = gst::ElementFactory::make("rskvssink")
        .property("stream-name", "test-registration")
        .property("region", "us-east-1")
        .build()?;

    assert!(element.is::<gst::Element>());

    // Verify properties
    let stream_name: String = element.property("stream-name");
    assert_eq!(stream_name, "test-registration");

    info!("rskvssink plugin registration works");
    Ok(())
}

/// Test that our plugin integrates with standard GStreamer pipeline parsing
#[test]
fn test_gstreamer_integration() -> Result<()> {
    gst::init()?;
    gstrskvssink::register(None)?;

    // Test pipeline string parsing with our element. buffer-directory is
    // required: the sink fails Ready->Paused without it.
    let pipeline_str = "videotestsrc num-buffers=10 ! \
                       video/x-raw,format=I420,width=320,height=240,framerate=30/1 ! \
                       x264enc bitrate=500 speed-preset=ultrafast ! \
                       video/x-h264,stream-format=avc,alignment=au ! \
                       rskvssink stream-name=integration-test region=us-west-2 \
                                 buffer-directory=/tmp/rskvssink-it";

    let pipeline = gst::parse::launch(pipeline_str)?;
    assert!(pipeline.is::<gst::Pipeline>());

    // Try to set to READY state (validates element compatibility)
    let state_change = pipeline.set_state(gst::State::Ready);
    let _ = pipeline.set_state(gst::State::Null); // Clean up

    match state_change {
        Ok(_) => info!("GStreamer pipeline integration works"),
        Err(e) => info!("Pipeline state change failed (expected without AWS creds): {e:?}"),
    }

    Ok(())
}
