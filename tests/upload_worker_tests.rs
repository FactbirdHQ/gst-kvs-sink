//! Integration tests for the upload worker: the bounded render -> worker
//! hand-off queue, its drop policy, session-reset retry behaviour, and
//! drain-on-stop.
//!
//! Frames are pushed through `appsrc` with hand-crafted PTS/keyframe flags so
//! fragment boundaries land deterministically, and `sync=false` keeps the
//! streaming thread from pacing to the clock.

mod common;

use anyhow::Result;
use common::gated_uploader::GatedUploader;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstrskvssink::KvsSink;
use serial_test::serial;
use std::time::Duration;

/// 25 fps -> 40 ms frames, so the 2 s Immediate-mode fragment boundary lands
/// exactly on every 50th frame (which is also a keyframe).
const FRAME_NS: u64 = 40_000_000;
const KEYFRAME_INTERVAL: u64 = 25;
const FRAMES_PER_FRAGMENT: u64 = 50;

/// Must match FRAGMENT_QUEUE_CAPACITY in src/kvssink/imp.rs.
const QUEUE_CAPACITY: usize = 64;

fn build_pipeline(
    uploader: GatedUploader,
    test_name: &str,
) -> Result<(gst::Pipeline, gst::Element)> {
    let pipeline = gst::Pipeline::default();

    let caps = gst::Caps::builder("video/x-h264")
        .field("stream-format", "avc")
        .field("alignment", "au")
        .build();

    let appsrc = gst::ElementFactory::make("appsrc")
        .property("caps", &caps)
        .property("format", gst::Format::Time)
        .build()?;

    let sink = KvsSink::with_uploader(uploader);
    sink.set_property("stream-name", "worker-test");
    sink.set_property("region", "us-west-2");
    sink.set_property("mode", "continuous");
    // Don't pace rendering to the pipeline clock - boundaries are driven purely
    // by buffer PTS so the tests run at push speed.
    sink.set_property("sync", false);

    let buffer_dir = format!(
        "/tmp/upload-worker-test-{}-{}",
        test_name,
        std::process::id()
    );
    std::fs::create_dir_all(&buffer_dir).ok();
    sink.set_property("buffer-directory", &buffer_dir);

    pipeline.add_many([&appsrc, sink.upcast_ref()])?;
    appsrc.link(sink.upcast_ref::<gst::Element>())?;

    Ok((pipeline, appsrc))
}

/// Push frames `range` (frame index = PTS / 40 ms), keyframe every 25th frame.
fn push_frames(appsrc: &gst::Element, range: std::ops::Range<u64>) {
    for i in range {
        let mut buffer = gst::Buffer::with_size(256).unwrap();
        {
            let b = buffer.get_mut().unwrap();
            b.set_pts(gst::ClockTime::from_nseconds(i * FRAME_NS));
            b.set_duration(gst::ClockTime::from_nseconds(FRAME_NS));
            if !i.is_multiple_of(KEYFRAME_INTERVAL) {
                b.set_flags(gst::BufferFlags::DELTA_UNIT);
            }
        }
        let ret = appsrc.emit_by_name::<gst::FlowReturn>("push-buffer", &[&buffer]);
        assert_eq!(ret, gst::FlowReturn::Ok, "push-buffer failed at frame {i}");
    }
}

fn push_eos_and_wait(pipeline: &gst::Pipeline, appsrc: &gst::Element) -> Result<()> {
    let ret = appsrc.emit_by_name::<gst::FlowReturn>("end-of-stream", &[]);
    assert_eq!(ret, gst::FlowReturn::Ok);

    let bus = pipeline.bus().unwrap();
    for msg in bus.iter_timed(gst::ClockTime::from_seconds(30)) {
        match msg.view() {
            gst::MessageView::Eos(..) => return Ok(()),
            gst::MessageView::Error(err) => {
                return Err(anyhow::anyhow!("Pipeline error: {}", err.error()));
            }
            _ => {}
        }
    }
    Err(anyhow::anyhow!("Timed out waiting for EOS"))
}

/// A stalled uploader must never block the streaming thread: fragments beyond
/// the queue bound are dropped, everything the queue holds survives, and the
/// worker drains once the uploader recovers.
#[test]
#[serial(gst)]
fn stalled_uploader_bounds_memory_and_drops_instead_of_blocking() -> Result<()> {
    common::init_gstreamer()?;

    let uploader = GatedUploader::new(false); // gate closed - uploads stall
    let handle = uploader.clone();

    let (pipeline, appsrc) = build_pipeline(uploader, "stall")?;
    pipeline.set_state(gst::State::Playing)?;

    // 70 full fragments; the worker blocks on the first one, the queue holds
    // the next 64, and the rest must be dropped without ever back-pressuring
    // the streaming thread (these pushes would hang forever otherwise).
    let total_fragments = (QUEUE_CAPACITY + 6) as u64;
    push_frames(&appsrc, 0..total_fragments * FRAMES_PER_FRAGMENT + 1);

    assert!(
        handle.wait_until(Duration::from_secs(5), |u| u.put_calls() >= 1),
        "worker should have picked up the first fragment"
    );

    // EOS finalizes the trailing partial fragment while the queue is still full.
    push_eos_and_wait(&pipeline, &appsrc)?;

    // Recover: everything still queued must drain.
    handle.open_gate();
    assert!(
        handle.wait_until(Duration::from_secs(10), |u| u.uploaded_count()
            >= QUEUE_CAPACITY),
        "queued fragments should drain once the uploader recovers, got {}",
        handle.uploaded_count()
    );

    pipeline.set_state(gst::State::Null)?;

    let uploaded = handle.uploaded_count();
    let finalized = total_fragments as usize + 1; // + EOS partial
    // In-flight fragment + full queue, with a +/-1 scheduling race on whether
    // the worker had dequeued the first fragment before the queue filled.
    assert!(
        (QUEUE_CAPACITY..=QUEUE_CAPACITY + 2).contains(&uploaded),
        "expected ~{} uploads (1 in-flight + {} queued), got {uploaded}",
        QUEUE_CAPACITY + 1,
        QUEUE_CAPACITY
    );
    assert!(
        uploaded < finalized,
        "overflow fragments should have been dropped ({uploaded} of {finalized} uploaded)"
    );

    Ok(())
}

/// A failed session reset must be retried at a later keyframe boundary. The
/// uploader keeps reporting the session as expired until a reset succeeds
/// (KvsClient rebases its expiry clock only after a successful reconnect), and
/// the sink must pick that up again once the failed reset clears the in-flight
/// guard.
#[test]
#[serial(gst)]
fn failed_session_reset_is_retried_at_next_boundary() -> Result<()> {
    common::init_gstreamer()?;

    let uploader = GatedUploader::new(true); // gate open - uploads succeed
    uploader.set_expired(true);
    uploader.fail_next_resets(1);
    let handle = uploader.clone();

    let (pipeline, appsrc) = build_pipeline(uploader, "reset-retry")?;
    pipeline.set_state(gst::State::Playing)?;

    // Boundary 1 (frame 50): expiry observed -> fragment 1 enqueued untagged,
    // reset armed for the next fragment.
    push_frames(&appsrc, 0..FRAMES_PER_FRAGMENT + 1);
    assert!(
        handle.wait_until(Duration::from_secs(5), |u| u.uploaded_count() >= 1),
        "fragment 1 should upload"
    );

    // Boundary 2: fragment 2 carries the reset tag; reset attempt 1 fails,
    // clearing the in-flight guard. The fragment itself still uploads.
    push_frames(
        &appsrc,
        FRAMES_PER_FRAGMENT + 1..2 * FRAMES_PER_FRAGMENT + 1,
    );
    assert!(
        handle.wait_until(Duration::from_secs(5), |u| {
            u.reset_calls() >= 1 && u.uploaded_count() >= 2
        }),
        "first reset attempt should have run and failed"
    );
    assert!(
        handle.session_expired(),
        "session must still report expired after a failed reset"
    );

    // Boundary 3: still expired, guard cleared -> reset re-armed.
    // Boundary 4: fragment 4 carries the retry; reset attempt 2 succeeds.
    push_frames(
        &appsrc,
        2 * FRAMES_PER_FRAGMENT + 1..4 * FRAMES_PER_FRAGMENT + 1,
    );
    assert!(
        handle.wait_until(Duration::from_secs(5), |u| {
            u.reset_calls() >= 2 && u.uploaded_count() >= 4
        }),
        "failed reset should be retried at a later boundary (reset_calls={})",
        handle.reset_calls()
    );
    assert!(
        !handle.session_expired(),
        "successful reset should clear expiry"
    );

    // One more fragment: no further resets once the session is fresh.
    push_frames(
        &appsrc,
        4 * FRAMES_PER_FRAGMENT + 1..5 * FRAMES_PER_FRAGMENT + 1,
    );
    push_eos_and_wait(&pipeline, &appsrc)?;
    pipeline.set_state(gst::State::Null)?;

    assert_eq!(
        handle.reset_calls(),
        2,
        "exactly two reset attempts expected (one failure, one success)"
    );
    assert_eq!(
        handle.uploaded_count(),
        6,
        "all fragments including the EOS partial should upload"
    );

    Ok(())
}

/// stop() must drain fragments that were finalized but not yet uploaded -
/// including the partial fragment flushed by EOS.
#[test]
#[serial(gst)]
fn stop_drains_pending_fragments() -> Result<()> {
    common::init_gstreamer()?;

    let uploader = GatedUploader::new(false); // gate closed while streaming
    let handle = uploader.clone();

    let (pipeline, appsrc) = build_pipeline(uploader, "drain")?;
    pipeline.set_state(gst::State::Playing)?;

    // Two full fragments plus one frame; EOS flushes the third, partial one.
    push_frames(&appsrc, 0..2 * FRAMES_PER_FRAGMENT + 1);
    push_eos_and_wait(&pipeline, &appsrc)?;

    assert_eq!(
        handle.uploaded_count(),
        0,
        "gate is closed - nothing uploaded yet"
    );

    // Recover before teardown; stop() waits for the worker to finish the queue.
    handle.open_gate();
    pipeline.set_state(gst::State::Null)?;

    assert_eq!(
        handle.uploaded_count(),
        3,
        "all finalized fragments (2 full + 1 EOS partial) must survive stop()"
    );

    Ok(())
}
