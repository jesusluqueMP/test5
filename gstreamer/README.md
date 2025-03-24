# CasparCG GStreamer Module

The GStreamer module provides an alternative media framework for CasparCG, offering additional codec support, streaming protocols, and hardware acceleration compared to the standard FFmpeg module.

## Features

- **Media Playback**: Play video files in various formats including MP4, MOV, MKV, WebM, FLV, and more
- **Stream Input**: Support for live stream sources including RTMP, RTSP, HTTP(S), and UDP
- **Output Streaming**: Stream to multiple protocols:
  - RTMP (Real-Time Messaging Protocol)
  - RTSP (Real-Time Streaming Protocol)
  - HLS (HTTP Live Streaming)
  - UDP (with RTP packaging)
- **Hardware Acceleration**: Support for NVIDIA NVENC, VA-API, and other hardware encoders (when available in GStreamer)
- **Modern Codecs**: Support for H.264, H.265/HEVC, VP8, VP9, and AV1 (depending on installed GStreamer plugins)

## Installation

### Dependencies

- GStreamer 1.18 or later
- Required GStreamer plugins:
  - gstreamer-base
  - gstreamer-video
  - gstreamer-audio
  - gstreamer-app
  - gst-plugins-base
  - gst-plugins-good
  - gst-plugins-bad (recommended)
  - gst-plugins-ugly (optional, for additional codecs)
  - gst-libav (recommended for wider format support)

### Windows Installation

1. Install GStreamer from [the official website](https://gstreamer.freedesktop.org/download/)
2. Make sure to select "Complete" installation or include all the required plugins
3. Ensure GStreamer's `bin` directory is in your system PATH
4. Build CasparCG with GStreamer support enabled

### Linux Installation

```bash
# Ubuntu/Debian
sudo apt-get install libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
  gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad \
  gstreamer1.0-plugins-ugly gstreamer1.0-libav

# CentOS/RHEL/Fedora
sudo dnf install gstreamer1-devel gstreamer1-plugins-base-devel \
  gstreamer1-plugins-good gstreamer1-plugins-bad-free gstreamer1-plugins-ugly-free
```

## Usage

### Producer

Use the GStreamer producer to play media files or streams:

```
PLAY 1-1 "GSTREAMER_PRODUCER" [filename or URI]
```

#### Examples:

```
PLAY 1-1 "GSTREAMER_PRODUCER" myfile.mp4
PLAY 1-1 "GSTREAMER_PRODUCER" rtmp://example.com/live/stream
PLAY 1-1 "GSTREAMER_PRODUCER" http://example.com/stream.m3u8
PLAY 1-1 "GSTREAMER_PRODUCER" udp://239.0.0.1:1234
```

#### Parameters:

The producer supports the same parameters as the FFmpeg producer:

```
PLAY 1-1 "GSTREAMER_PRODUCER" myfile.mp4 LOOP IN 100 OUT 500 SEEK 150
```

- `LOOP`: Loop the clip
- `IN`: Start frame
- `OUT`: End frame
- `SEEK`: Start at specified frame
- `LENGTH`: Play a specific number of frames
- `FILTER` or `VF`: Apply video filters
- `SCALE_MODE`: Choose between `STRETCH`, `FILL`, `FIT`, or `CROP`

### Consumer

Use the GStreamer consumer to output video to files or streams:

```
ADD 1 STREAM "rtmp://server/live/stream" -vcodec [codec] -vbitrate [bitrate]
```

or

```
ADD 1 FILE "output.mp4" -vcodec [codec] -vbitrate [bitrate]
```

#### Examples:

```
ADD 1 STREAM "rtmp://streaming-server/live/stream" -vcodec x264 -vbitrate 3000
ADD 1 FILE "output.mp4" -vcodec x264 -vbitrate 5000
ADD 1 STREAM "udp://239.0.0.1:1234" -vcodec x264 -vbitrate 4000
```

#### Parameters:

- `-vcodec`: Video codec to use (x264, openh264, nvenc, vp8, vp9)
- `-vbitrate`: Video bitrate in kbps
- `-abitrate`: Audio bitrate in kbps

## Configuration

In the `casparcg.config` file, you can add GStreamer-specific settings:

```xml
<configuration>
  <gstreamer>
    <debug-level>2</debug-level>
  </gstreamer>
</configuration>
```

### Parameters:

- `debug-level`: GStreamer debug level (0-5, where 0 is no debug and 5 is maximum debug information)

## Comparison with FFmpeg

| Feature | GStreamer | FFmpeg |
|---------|-----------|--------|
| File Format Support | Comprehensive (depends on plugins) | Comprehensive |
| Hardware Acceleration | Multiple options (NVENC, VA-API, etc.) | NVENC, QSV, VAAPI |
| Live Streaming | RTMP, RTSP, HLS, UDP | RTMP, UDP |
| Modern Codecs | H.264, HEVC, VP8, VP9, AV1 | H.264, HEVC, VP8, VP9 |
| Pipeline Flexibility | Dynamic pipeline construction | Fixed processing pipeline |
| Performance | Good, potentially better with hardware acceleration | Excellent |

## Troubleshooting

### Common Issues:

1. **Missing Codec/Format Support**:
   - Make sure you've installed all required GStreamer plugins
   - For Windows: Install "Complete" GStreamer package
   - For Linux: Install additional codec packages as needed

2. **Performance Issues**:
   - Enable hardware acceleration if available
   - Adjust buffer settings in the consumer if streaming is unstable

3. **Debug Information**:
   - Increase the debug level in the configuration
   - Check the console output for GStreamer pipeline errors

## License

This module is part of CasparCG and follows the same license terms (GPLv3).