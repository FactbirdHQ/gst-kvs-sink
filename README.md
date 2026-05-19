# gst-kvs-sink

A GStreamer sink that publishes H.264 media fragments to **AWS Kinesis Video Streams** (KVS) using the streaming PutMedia API.

Element factory name: **`rskvssink`** (rank: PRIMARY).

## Features

- Fragmented MKV streaming writer (no external `mkvmerge`/`ffmpeg` dependency)
- Signed-HTTP PutMedia client built on `aws-sigv4` (no SDK call for the streaming part)
- Disk-backed fragment buffer for offline / retry scenarios, bounded by configurable retention
- Two upload modes: `continuous` (live) and `trigger` (on-demand)
- `trigger-upload` GObject action signal for time-range uploads from MQTT / IPC
- Network-failure auto-recovery with exponential backoff
- Pluggable `MediaUploader` trait for testing

## Building

```sh
cargo build --release
```

This produces:

- `target/release/libgstrskvssink.so` — distributable GStreamer plugin (cdylib)
- An rlib for in-process use from other Rust crates

System dependencies (Ubuntu/Debian):

```sh
sudo apt install libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev gstreamer1.0-tools
```

## Use as a standalone plugin

```sh
GST_PLUGIN_PATH=$PWD/target/release gst-inspect-1.0 rskvssink

gst-launch-1.0 -v \
  videotestsrc num-buffers=300 ! \
  x264enc tune=zerolatency ! \
  video/x-h264,stream-format=avc,alignment=au ! \
  rskvssink \
    stream-name=my-test-stream \
    region=eu-west-1 \
    buffer-directory=/tmp/kvs-buf \
    mode=continuous
```

## Use as a Rust library

```toml
[dependencies]
gst-kvs-sink = { git = "https://github.com/FactbirdHQ/gst-kvs-sink", rev = "<sha>" }
```

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    gstreamer::init()?;

    // Register the element in-process; no `.so` plugin file needed.
    gstrskvssink::register(None)?;

    let pipeline = gstreamer::parse_launch(
        "videotestsrc ! x264enc ! \
         video/x-h264,stream-format=avc,alignment=au ! \
         rskvssink stream-name=demo buffer-directory=/tmp/kvs-buf",
    )?;
    // ... run pipeline ...
    Ok(())
}
```

## Properties

| Property | Type | Default | Description |
|---|---|---|---|
| `stream-name` | string | *(required)* | KVS stream name. |
| `region` | string | `$AWS_DEFAULT_REGION` or `"eu-west-1"` | AWS region. |
| `fragment-duration` | uint64 (ns) | `2_000_000_000` (2 s) | Target fragment duration. |
| `session-duration-secs` | uint64 | `2400` (40 min) | KVS session lifetime; max 45 min. |
| `mode` | string | `"continuous"` | `continuous` (live upload) or `trigger` (buffer-only until triggered). |
| `buffer-directory` | string | **(required, no default)** | Directory for on-disk fragment buffer. |
| `max-buffer-hours` | uint64 | `24` | Retention window for buffered fragments (1..=168). |
| `segment-duration-secs` | uint64 | `60` | Segment rotation period for buffer files (10..=600). |

`buffer-directory` has no default — the element fails the `Ready → Paused` state change with a bus ERROR if it is unset.

## Signals

- **`trigger-upload`** (action signal) — `(start: Option<String>, end: Option<String>) -> bool`. Triggers an asynchronous upload of buffered fragments within the optional RFC3339 time range. Returns `true` if accepted, `false` if the element is not ready.

## AWS credentials

Credentials are resolved through the standard `aws-config` provider chain (env vars → shared config → IMDS). On EC2 / ECS / Greengrass: typically works without explicit config. On a developer laptop you'll need to set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (or `AWS_PROFILE`) before running.

The IAM principal needs `kinesisvideo:PutMedia`, `kinesisvideo:GetDataEndpoint`, and `kinesisvideo:DescribeStream` for the configured `stream-name`.

## License

MIT
