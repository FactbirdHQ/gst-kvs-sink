//! GStreamer sink that publishes H.264 media fragments to AWS Kinesis Video
//! Streams.
//!
//! ## Usage as a Rust library
//!
//! Register the element once during process startup, then build pipelines
//! that reference the factory name `rskvssink`:
//!
//! ```no_run
//! gstreamer::init()?;
//! gstrskvssink::register(None)?;
//!
//! let pipeline = gstreamer::parse_launch(
//!     "videotestsrc ! x264enc ! video/x-h264,stream-format=avc,alignment=au ! \
//!      rskvssink stream-name=demo buffer-directory=/tmp/kvs-buf",
//! )?;
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Usage as a standalone GStreamer plugin
//!
//! Build with `cargo build --release` and point `GST_PLUGIN_PATH` at the
//! resulting `target/release/` directory; the plugin is `libgstrskvssink.so`
//! (or the platform equivalent).

mod buffered_upload_manager;
mod fragment;
mod kvs_client;
mod kvssink;
mod media_uploader;
mod mkv_writer;

pub use kvssink::{KvsSink, register};

/// Lower-level types for advanced embedders.
///
/// The headline API is [`KvsSink`] + [`register`]. The items below are only
/// needed when integrating directly with the on-disk buffer, supplying a
/// custom [`advanced::MediaUploader`] implementation for tests, or inspecting
/// fragments outside the element.
pub mod advanced {
    pub use crate::buffered_upload_manager::{
        BufferManagerEvent, BufferStats, BufferedUploadManager, ClusterFileReader,
    };
    pub use crate::fragment::Fragment;
    pub use crate::kvs_client::KvsError;
    pub use crate::media_uploader::MediaUploader;
}

fn plugin_init(plugin: &gstreamer::Plugin) -> Result<(), gstreamer::glib::BoolError> {
    kvssink::register(Some(plugin))
}

gstreamer::plugin_define!(
    rskvssink,
    env!("CARGO_PKG_DESCRIPTION"),
    plugin_init,
    concat!(env!("CARGO_PKG_VERSION"), "-", env!("COMMIT_ID")),
    "MIT",
    env!("CARGO_PKG_NAME"),
    env!("CARGO_PKG_NAME"),
    env!("CARGO_PKG_REPOSITORY"),
    env!("BUILD_REL_DATE")
);
