#pragma once

#include <core/frame/frame.h>
#include <core/frame/frame_factory.h>
#include <core/frame/pixel_format.h>
#include <core/video_format.h>
#include <common/bit_depth.h>

#include <gst/gst.h>
#include <gst/video/video.h>
#include <gst/audio/audio.h>

#include <memory>
#include <string>
#include <map>
#include <vector>

namespace caspar { namespace gstreamer {

// Smart pointer wrappers for GStreamer objects
template <typename T>
struct GstDeleter {
    void operator()(T* ptr) { gst_object_unref(GST_OBJECT(ptr)); }
};

template <typename T>
using gst_ptr = std::shared_ptr<T>;

template <typename T>
gst_ptr<T> make_gst_ptr(T* ptr) {
    return gst_ptr<T>(ptr, GstDeleter<T>());
}

// Specialization for GstElement
template <>
struct GstDeleter<GstElement> {
    void operator()(GstElement* ptr) { 
        if (ptr)
            gst_object_unref(GST_OBJECT(ptr)); 
    }
};

// Specialization for GstSample
template <>
struct GstDeleter<GstSample> {
    void operator()(GstSample* ptr) { 
        if (ptr)
            gst_sample_unref(ptr); 
    }
};

// Specialization for GstBuffer
template <>
struct GstDeleter<GstBuffer> {
    void operator()(GstBuffer* ptr) { 
        if (ptr)
            gst_buffer_unref(ptr); 
    }
};

// Specialization for GstCaps
template <>
struct GstDeleter<GstCaps> {
    void operator()(GstCaps* ptr) { 
        if (ptr)
            gst_caps_unref(ptr); 
    }
};

// Specialization for GstBus
template <>
struct GstDeleter<GstBus> {
    void operator()(GstBus* ptr) { 
        if (ptr)
            gst_object_unref(GST_OBJECT(ptr)); 
    }
};

// Helper for GstMessage
template <>
struct GstDeleter<GstMessage> {
    void operator()(GstMessage* ptr) { 
        if (ptr)
            gst_message_unref(ptr); 
    }
};

// CasparCG to GStreamer format conversion utilities
GstVideoFormat pixel_format_to_gst(core::pixel_format format, common::bit_depth depth);
core::pixel_format_desc gst_format_to_caspar(GstVideoInfo* video_info);

// Frame conversion utilities
core::mutable_frame make_frame(void* tag,
                              core::frame_factory& frame_factory,
                              GstSample* sample,
                              core::color_space color_space = core::color_space::bt709);

GstSample* make_gst_sample(const core::const_frame& frame, const core::video_format_desc& format_desc);

// Pipeline creation utilities
gst_ptr<GstElement> create_pipeline(const std::string& pipeline_description);
std::map<std::string, std::string> parse_gst_structure(GstStructure* structure);
std::string caps_to_string(GstCaps* caps);

}} // namespace caspar::gstreamer