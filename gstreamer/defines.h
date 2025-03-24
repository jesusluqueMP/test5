#pragma once

// GStreamer version-specific defines and compatibility helpers

// Check GStreamer version
#include <gst/gst.h>

// Define GST_API_VERSION macro for easier version checks
#define GST_API_VERSION (GST_VERSION_MAJOR * 1000 + GST_VERSION_MINOR * 100 + GST_VERSION_MICRO)

// Define macros for compatibility across GStreamer versions
#if GST_API_VERSION < 1000
    #error "GStreamer version 1.0.0 or higher is required"
#endif

// Define if we're using GStreamer 1.16 or higher, which has better hardware acceleration support
#define GST_HAS_IMPROVED_HW_ACCEL (GST_API_VERSION >= 1160)

// Define if we're using GStreamer 1.18 or higher, which has WebRTC support
#define GST_HAS_WEBRTC (GST_API_VERSION >= 1180)

// Define if we're using GStreamer 1.20 or higher, which has better AV1 support
#define GST_HAS_AV1 (GST_API_VERSION >= 1200)
