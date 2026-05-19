use gstreamer::{glib, prelude::*};
use once_cell::sync::Lazy;

#[derive(Debug, Clone)]
pub struct Settings {
    pub stream_name: String,
    pub region: String,
    pub fragment_duration: gstreamer::ClockTime,
    /// Session duration in seconds
    pub session_duration_secs: u64,
    /// Upload mode: "continuous" or "trigger"
    pub mode: String,
    /// Buffer directory for local fragment storage. Required: the sink fails
    /// to transition to PAUSED if this is not set.
    pub buffer_directory: Option<String>,
    /// Maximum buffer duration in hours
    pub max_buffer_hours: u64,
    /// Segment duration in seconds
    pub segment_duration_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            stream_name: String::new(),
            region: std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "eu-west-1".to_string()),
            fragment_duration: gstreamer::ClockTime::from_nseconds(2_000_000_000), // 2 seconds
            session_duration_secs: 40 * 60,                                        // 40 minutes
            mode: "continuous".to_string(),
            buffer_directory: None,
            max_buffer_hours: 24,
            segment_duration_secs: 60,
        }
    }
}

pub struct Properties;

impl Properties {
    pub fn properties() -> &'static [glib::ParamSpec] {
        static PROPERTIES: Lazy<Vec<glib::ParamSpec>> = Lazy::new(|| {
            let defaults = Settings::default();

            // Leak strings to get 'static lifetime for glib property specs
            let region: &'static str = Box::leak(defaults.region.into_boxed_str());
            let mode: &'static str = Box::leak(defaults.mode.into_boxed_str());

            vec![
                glib::ParamSpecString::builder("stream-name")
                    .nick("Stream Name")
                    .blurb("KVS stream name (required)")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("region")
                    .nick("AWS Region")
                    .blurb("AWS region for KVS")
                    .default_value(Some(region))
                    .mutable_ready()
                    .build(),
                glib::ParamSpecUInt64::builder("fragment-duration")
                    .nick("Fragment Duration")
                    .blurb("Fragment duration in nanoseconds")
                    .default_value(defaults.fragment_duration.nseconds())
                    .mutable_ready()
                    .build(),
                glib::ParamSpecUInt64::builder("session-duration-secs")
                    .nick("Session Duration")
                    .blurb("KVS session duration in seconds")
                    .minimum(1)
                    .maximum(45 * 60) // AWS KVS limit is 45 minutes
                    .default_value(defaults.session_duration_secs)
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("mode")
                    .nick("Mode")
                    .blurb("Upload mode: continuous or trigger")
                    .default_value(Some(mode))
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("buffer-directory")
                    .nick("Buffer Directory")
                    .blurb(
                        "Directory for local buffer storage (required, no default; \
                         element fails Ready->Paused state change if unset)",
                    )
                    .mutable_ready()
                    .build(),
                glib::ParamSpecUInt64::builder("max-buffer-hours")
                    .nick("Max Buffer Hours")
                    .blurb("Maximum buffer duration in hours")
                    .minimum(1)
                    .maximum(168) // 1 week
                    .default_value(defaults.max_buffer_hours)
                    .mutable_ready()
                    .build(),
                glib::ParamSpecUInt64::builder("segment-duration-secs")
                    .nick("Segment Duration")
                    .blurb("Segment duration in seconds")
                    .minimum(10)
                    .maximum(600) // 10 minutes
                    .default_value(defaults.segment_duration_secs)
                    .mutable_ready()
                    .build(),
            ]
        });
        PROPERTIES.as_ref()
    }

    pub fn set_property(settings: &mut Settings, value: &glib::Value, pspec: &glib::ParamSpec) {
        match pspec.name() {
            "stream-name" => {
                settings.stream_name = value.get::<String>().unwrap_or_default();
            }
            "region" => {
                settings.region = value.get::<String>().unwrap_or_else(|_| {
                    std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "eu-west-1".to_string())
                });
            }
            "fragment-duration" => {
                let duration_ns = value.get::<u64>().unwrap_or(2_000_000_000);
                settings.fragment_duration = gstreamer::ClockTime::from_nseconds(duration_ns);
            }
            "session-duration-secs" => {
                settings.session_duration_secs = value.get::<u64>().unwrap_or(40 * 60);
            }
            "mode" => {
                settings.mode = value
                    .get::<String>()
                    .unwrap_or_else(|_| "continuous".to_string());
            }
            "buffer-directory" => {
                let v = value.get::<String>().unwrap_or_default();
                settings.buffer_directory = if v.is_empty() { None } else { Some(v) };
            }
            "max-buffer-hours" => {
                settings.max_buffer_hours = value.get::<u64>().unwrap_or(24);
            }
            "segment-duration-secs" => {
                settings.segment_duration_secs = value.get::<u64>().unwrap_or(60);
            }
            _ => {}
        }
    }

    pub fn get_property(settings: &Settings, pspec: &glib::ParamSpec) -> glib::Value {
        match pspec.name() {
            "stream-name" => settings.stream_name.to_value(),
            "region" => settings.region.to_value(),
            "fragment-duration" => settings.fragment_duration.nseconds().to_value(),
            "session-duration-secs" => settings.session_duration_secs.to_value(),
            "mode" => settings.mode.to_value(),
            "buffer-directory" => settings
                .buffer_directory
                .clone()
                .unwrap_or_default()
                .to_value(),
            "max-buffer-hours" => settings.max_buffer_hours.to_value(),
            "segment-duration-secs" => settings.segment_duration_secs.to_value(),
            _ => unimplemented!(),
        }
    }
}
