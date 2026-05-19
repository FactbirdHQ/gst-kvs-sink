use glib::subclass::types::ObjectSubclassIsExt as _;
use gstreamer::glib;
use gstreamer::prelude::StaticType;

mod imp;
mod properties;

use crate::buffered_upload_manager::BufferStats;
use crate::kvs_client::KvsClient;
use crate::media_uploader::MediaUploader;

glib::wrapper! {
    /// The KVS sink GObject element.
    ///
    /// Registered with GStreamer under the factory name `rskvssink`. End users
    /// configure the element through GObject properties (see crate-level docs
    /// or `gst-inspect-1.0 rskvssink`). For embedding in a Rust pipeline you
    /// will normally use the standard [`gstreamer::ElementFactory::make`]
    /// rather than constructing this type directly.
    pub struct KvsSink(ObjectSubclass<imp::KvsSink>)
        @extends gstreamer_base::BaseSink, gstreamer::Element, gstreamer::Object;
}

impl KvsSink {
    /// Construct a sink with a custom [`MediaUploader`].
    ///
    /// Primarily intended for integration tests where the network-bound
    /// production uploader needs to be replaced by an in-memory fake. The
    /// uploader must be set before the element transitions out of the NULL
    /// state.
    pub fn with_uploader(uploader: impl MediaUploader + 'static) -> Self {
        let sink: Self = glib::Object::builder().build();
        sink.imp().set_uploader(uploader);
        sink
    }

    /// Trigger an asynchronous upload of all currently buffered fragments.
    ///
    /// Equivalent to emitting the `trigger-upload` action signal with no time
    /// range. Returns once the trigger has been accepted; the upload itself
    /// runs in a background task.
    pub async fn trigger_upload(&self) -> Result<(), gstreamer::ErrorMessage> {
        self.imp().trigger_upload().await
    }

    /// Snapshot of the on-disk buffer statistics, or `None` if the sink has
    /// not yet been started.
    pub fn buffer_stats(&self) -> Option<BufferStats> {
        self.imp().buffer_stats()
    }
}

impl Default for KvsSink {
    fn default() -> Self {
        let sink: Self = glib::Object::builder().build();
        sink.imp().set_uploader(KvsClient::new());
        sink
    }
}

/// Register the `rskvssink` element with GStreamer.
///
/// Pass `Some(plugin)` from a `plugin_init` callback when building the cdylib
/// distribution, or `None` for in-process registration from a Rust binary.
pub fn register(plugin: Option<&gstreamer::Plugin>) -> Result<(), glib::BoolError> {
    gstreamer::Element::register(
        plugin,
        "rskvssink",
        gstreamer::Rank::PRIMARY,
        KvsSink::static_type(),
    )
}
