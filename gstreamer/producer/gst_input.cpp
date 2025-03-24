#include "gst_input.h"

#include "../util/gst_assert.h"
#include "../util/gst_util.h"

#include <common/except.h>
#include <common/os/thread.h>
#include <common/param.h>
#include <common/scope_exit.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/filesystem.hpp>

#include <gst/app/gstappsink.h>

namespace caspar { namespace gstreamer {

GstInput::GstInput(const std::string& uri, std::shared_ptr<diagnostics::graph> graph, std::optional<bool> loop)
    : uri_(uri)
    , graph_(graph)
    , loop_(loop)
{
    graph_->set_color("seek", diagnostics::color(1.0f, 0.5f, 0.0f));
    graph_->set_color("input", diagnostics::color(0.7f, 0.4f, 0.4f));

    video_buffer_.set_capacity(64);
    audio_buffer_.set_capacity(128);

    // Initialize pipeline
    initialize_pipeline(uri_);
    
    // Start monitor thread
    thread_ = boost::thread([=] {
        try {
            set_thread_name(L"[gstreamer::GstInput]");
            
            // Setup bus monitoring
            gst_ptr<GstBus> bus(gst_element_get_bus(pipeline_.get()));
            while (!abort_request_) {
                gst_ptr<GstMessage> msg(gst_bus_timed_pop(bus.get(), 100 * GST_MSECOND));
                
                if (!msg) {
                    // Timeout - no message
                    continue;
                }
                
                switch (GST_MESSAGE_TYPE(msg.get())) {
                    case GST_MESSAGE_EOS:
                        if (loop_.value_or(false)) {
                            // If looping, seek back to the start
                            seek(0, true);
                        } else {
                            eof_ = true;
                        }
                        break;
                        
                    case GST_MESSAGE_ERROR: {
                        GError* err = nullptr;
                        gchar* dbg_info = nullptr;
                        
                        gst_message_parse_error(msg.get(), &err, &dbg_info);
                        CASPAR_LOG(error) << "GStreamer error: " << (err ? err->message : "unknown") 
                                         << " " << (dbg_info ? dbg_info : "");
                        
                        g_error_free(err);
                        g_free(dbg_info);
                        break;
                    }
                    
                    case GST_MESSAGE_WARNING: {
                        GError* warn = nullptr;
                        gchar* dbg_info = nullptr;
                        
                        gst_message_parse_warning(msg.get(), &warn, &dbg_info);
                        CASPAR_LOG(warning) << "GStreamer warning: " << (warn ? warn->message : "unknown") 
                                           << " " << (dbg_info ? dbg_info : "");
                        
                        g_error_free(warn);
                        g_free(dbg_info);
                        break;
                    }
                    
                    case GST_MESSAGE_STATE_CHANGED: {
                        // Only interested in pipeline state changes
                        if (GST_MESSAGE_SRC(msg.get()) == GST_OBJECT(pipeline_.get())) {
                            GstState old_state, new_state, pending_state;
                            gst_message_parse_state_changed(msg.get(), &old_state, &new_state, &pending_state);
                            
                            if (new_state == GST_STATE_PLAYING) {
                                // Get stream information when we reach PLAYING state
                                // Get stream duration
                                gint64 duration = 0;
                                if (gst_element_query_duration(pipeline_.get(), GST_FORMAT_TIME, &duration)) {
                                    // Store duration in milliseconds instead of nanoseconds
                                    duration_ = duration / GST_MSECOND;
                                }
                            }
                        }
                        break;
                    }
                    
                    default:
                        break;
                }
            }
        } catch (...) {
            CASPAR_LOG_CURRENT_EXCEPTION();
        }
    });
}

GstInput::~GstInput()
{
    abort_request_ = true;
    
    if (thread_.joinable()) {
        thread_.join();
    }
    
    if (pipeline_) {
        gst_element_set_state(pipeline_.get(), GST_STATE_NULL);
    }
    
    // Free any remaining samples in the queues
    GstSample* sample = nullptr;
    while (video_buffer_.try_pop(sample)) {
        if (sample) {
            gst_sample_unref(sample);
        }
    }
    
    while (audio_buffer_.try_pop(sample)) {
        if (sample) {
            gst_sample_unref(sample);
        }
    }
}

void GstInput::initialize_pipeline(const std::string& uri)
{
    try {
        create_pipeline(uri);
        
        if (!pipeline_) {
            CASPAR_LOG(error) << "Failed to create GStreamer pipeline for URI: " << uri;
            return;
        }
        
        // Wait for pipeline to be properly set up
        GstStateChangeReturn ret = gst_element_set_state(pipeline_.get(), GST_STATE_PAUSED);
        if (ret == GST_STATE_CHANGE_FAILURE) {
            CASPAR_LOG(error) << "Failed to set pipeline to PAUSED state";
            gst_element_set_state(pipeline_.get(), GST_STATE_NULL);
            pipeline_.reset();
            return;
        }
        
        // Get video information
        if (video_appsink_) {
            GstPad* pad = gst_element_get_static_pad(video_appsink_.get(), "sink");
            if (pad) {
                GstCaps* caps = gst_pad_get_current_caps(pad);
                if (caps) {
                    GstVideoInfo info;
                    if (gst_video_info_from_caps(&info, caps)) {
                        width_ = info.width;
                        height_ = info.height;
                    }
                    gst_caps_unref(caps);
                }
                gst_object_unref(pad);
            }
        }
        
        // Get audio information
        if (audio_appsink_) {
            GstPad* pad = gst_element_get_static_pad(audio_appsink_.get(), "sink");
            if (pad) {
                GstCaps* caps = gst_pad_get_current_caps(pad);
                if (caps) {
                    GstAudioInfo info;
                    if (gst_audio_info_from_caps(&info, caps)) {
                        audio_channels_ = info.channels;
                        audio_sample_rate_ = info.rate;
                    }
                    gst_caps_unref(caps);
                }
                gst_object_unref(pad);
            }
        }
        
        initialized_ = true;
    } catch (const std::exception& ex) {
        CASPAR_LOG(error) << "Error initializing GStreamer pipeline: " << ex.what();
        if (pipeline_) {
            gst_element_set_state(pipeline_.get(), GST_STATE_NULL);
            pipeline_.reset();
        }
    }
}

// Explicitly defining the signature for the wrapper function to match GStreamer's expectation
GstFlowReturn GstInput::new_video_sample(GstAppSink* sink, gpointer user_data)
{
    GstInput* self = static_cast<GstInput*>(user_data);
    
    GstSample* sample = gst_app_sink_pull_sample(sink);
    if (!sample) {
        return GST_FLOW_ERROR;
    }
    
    // Add ref for the sample so it stays alive in the queue
    gst_sample_ref(sample);
    
    if (!self->video_buffer_.try_push(sample)) {
        // Queue is full, free the sample we just created
        gst_sample_unref(sample);
        return GST_FLOW_OK;
    }
    
    self->graph_->set_value("input", static_cast<double>(self->video_buffer_.size()) / self->video_buffer_.capacity());
    
    return GST_FLOW_OK;
}

GstFlowReturn GstInput::new_audio_sample(GstAppSink* sink, gpointer user_data)
{
    GstInput* self = static_cast<GstInput*>(user_data);
    
    GstSample* sample = gst_app_sink_pull_sample(sink);
    if (!sample) {
        return GST_FLOW_ERROR;
    }
    
    // Add ref for the sample so it stays alive in the queue
    gst_sample_ref(sample);
    
    if (!self->audio_buffer_.try_push(sample)) {
        // Queue is full, free the sample we just created
        gst_sample_unref(sample);
        return GST_FLOW_OK;
    }
    
    return GST_FLOW_OK;
}

void GstInput::create_pipeline(const std::string& uri)
{
    if (uri.empty()) {
        CASPAR_THROW_EXCEPTION(caspar_exception() << msg_info_t("URI cannot be empty"));
    }
    
    // Create a basic playbin pipeline that will handle most formats
    std::string pipeline_desc = "playbin uri=\"" + uri + "\" ";
    
    // Check if we need to use specific protocols
    std::string protocol;
    std::string path = uri;
    
    size_t protocol_separator = uri.find("://");
    if (protocol_separator != std::string::npos) {
        protocol = uri.substr(0, protocol_separator);
        path = uri.substr(protocol_separator + 3);
    }
    
    if (protocol == "rtmp" || protocol == "rtmps") {
        // Use rtmpsrc for RTMP streams
        pipeline_desc = "rtmpsrc location=\"" + uri + "\" ! decodebin name=decode ";
    } else if (protocol == "http" || protocol == "https") {
        // For HTTP streams, configure appropriate settings
        pipeline_desc = "playbin uri=\"" + uri + "\" buffer-duration=2000000000 ";
    } else if (boost::filesystem::exists(uri)) {
        // Local file - use playbin with filesrc
        pipeline_desc = "playbin uri=\"file://" + uri + "\" ";
    }
    
    // Create separate video and audio sinks for the pipeline
    pipeline_desc += " video-sink=\"appsink name=video_sink max-buffers=64 drop=true sync=true\" ";
    pipeline_desc += " audio-sink=\"appsink name=audio_sink max-buffers=128 drop=false sync=true\" ";
    
    // Create the pipeline
    pipeline_ = create_pipeline(pipeline_desc);
    
    // Get the video appsink
    video_appsink_ = make_gst_ptr<GstElement>(gst_bin_get_by_name(GST_BIN(pipeline_.get()), "video_sink"));
    if (video_appsink_) {
        // Set up video sink
        gst_app_sink_set_emit_signals(GST_APP_SINK(video_appsink_.get()), FALSE);
        gst_app_sink_set_drop(GST_APP_SINK(video_appsink_.get()), TRUE);
        gst_app_sink_set_max_buffers(GST_APP_SINK(video_appsink_.get()), 64);
        
        // Set up video caps
        GstCaps* video_caps = gst_caps_new_simple("video/x-raw",
                                                 "format", G_TYPE_STRING, "BGRA",
                                                 NULL);
        gst_app_sink_set_caps(GST_APP_SINK(video_appsink_.get()), video_caps);
        gst_caps_unref(video_caps);
        
        // Setup callbacks for new samples
        GstAppSinkCallbacks video_callbacks;
        memset(&video_callbacks, 0, sizeof(GstAppSinkCallbacks));
        
        // Set the direct callback using the static method from our class
        // No need for wrapper functions anymore
        video_callbacks.new_sample = &GstInput::new_video_sample;
        
        gst_app_sink_set_callbacks(GST_APP_SINK(video_appsink_.get()), &video_callbacks, this, nullptr);
    }
    
    // Get the audio appsink
    audio_appsink_ = make_gst_ptr<GstElement>(gst_bin_get_by_name(GST_BIN(pipeline_.get()), "audio_sink"));
    if (audio_appsink_) {
        // Set up audio sink
        gst_app_sink_set_emit_signals(GST_APP_SINK(audio_appsink_.get()), FALSE);
        gst_app_sink_set_drop(GST_APP_SINK(audio_appsink_.get()), FALSE);
        gst_app_sink_set_max_buffers(GST_APP_SINK(audio_appsink_.get()), 128);
        
        // Set up audio caps
        GstCaps* audio_caps = gst_caps_new_simple("audio/x-raw",
                                                 "format", G_TYPE_STRING, "S32LE",
                                                 "rate", G_TYPE_INT, 48000,
                                                 "channels", G_TYPE_INT, 2,
                                                 "layout", G_TYPE_STRING, "interleaved",
                                                 NULL);
        gst_app_sink_set_caps(GST_APP_SINK(audio_appsink_.get()), audio_caps);
        gst_caps_unref(audio_caps);
        
        // Setup callbacks for new samples
        GstAppSinkCallbacks audio_callbacks;
        memset(&audio_callbacks, 0, sizeof(GstAppSinkCallbacks));
        
        // Set the direct callback using the static method from our class
        // No need for wrapper functions anymore
        audio_callbacks.new_sample = &GstInput::new_audio_sample;
        
        gst_app_sink_set_callbacks(GST_APP_SINK(audio_appsink_.get()), &audio_callbacks, this, nullptr);
    }
}

bool GstInput::try_pop_video(GstSample** sample)
{
    auto result = video_buffer_.try_pop(*sample);
    graph_->set_value("input", static_cast<double>(video_buffer_.size()) / video_buffer_.capacity());
    return result;
}

bool GstInput::try_pop_audio(GstSample** sample)
{
    return audio_buffer_.try_pop(*sample);
}

void GstInput::seek(int64_t position, bool flush)
{
    if (!pipeline_) {
        return;
    }
    
    if (position < 0) {
        position = 0;
    }
    
    CASPAR_LOG(debug) << "GstInput seeking to position: " << position;
    
    // Convert milliseconds to nanoseconds
    gint64 seek_pos = position * GST_MSECOND;
    
    // Flush the buffers if requested
    if (flush) {
        GstSample* sample = nullptr;
        while (video_buffer_.try_pop(sample)) {
            if (sample) {
                gst_sample_unref(sample);
            }
        }
        
        while (audio_buffer_.try_pop(sample)) {
            if (sample) {
                gst_sample_unref(sample);
            }
        }
    }
    
    // Flags for the seek operation
    GstSeekFlags flags = static_cast<GstSeekFlags>(GST_SEEK_FLAG_FLUSH | GST_SEEK_FLAG_KEY_UNIT);
    
    // Perform the seek operation
    if (!gst_element_seek_simple(pipeline_.get(), GST_FORMAT_TIME, flags, seek_pos)) {
        CASPAR_LOG(warning) << "GstInput seek failed";
    }
    
    eof_ = false;
    graph_->set_tag(diagnostics::tag_severity::INFO, "seek");
}

void GstInput::abort()
{
    abort_request_ = true;
    
    if (pipeline_) {
        gst_element_set_state(pipeline_.get(), GST_STATE_NULL);
    }
    
    GstSample* sample = nullptr;
    while (video_buffer_.try_pop(sample)) {
        if (sample) {
            gst_sample_unref(sample);
        }
    }
    
    while (audio_buffer_.try_pop(sample)) {
        if (sample) {
            gst_sample_unref(sample);
        }
    }
}

void GstInput::reset()
{
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Stop current pipeline
    if (pipeline_) {
        gst_element_set_state(pipeline_.get(), GST_STATE_NULL);
    }
    
    pipeline_.reset();
    video_appsink_.reset();
    audio_appsink_.reset();
    
    // Clear buffers
    GstSample* sample = nullptr;
    while (video_buffer_.try_pop(sample)) {
        if (sample) {
            gst_sample_unref(sample);
        }
    }
    
    while (audio_buffer_.try_pop(sample)) {
        if (sample) {
            gst_sample_unref(sample);
        }
    }
    
    // Reset state
    eof_ = false;
    initialized_ = false;
    
    // Recreate pipeline
    initialize_pipeline(uri_);
}

bool GstInput::eof() const
{
    return eof_;
}

int GstInput::width() const
{
    return width_;
}

int GstInput::height() const
{
    return height_;
}

int GstInput::audio_channels() const
{
    return audio_channels_;
}

int GstInput::audio_sample_rate() const
{
    return audio_sample_rate_;
}

int64_t GstInput::duration() const
{
    return duration_; // Already stored in milliseconds
}

void GstInput::start()
{
    if (pipeline_) {
        gst_element_set_state(pipeline_.get(), GST_STATE_PLAYING);
    }
}

void GstInput::stop()
{
    if (pipeline_) {
        gst_element_set_state(pipeline_.get(), GST_STATE_PAUSED);
    }
}

GstCaps* GstInput::get_video_caps() const
{
    if (!video_appsink_) {
        return nullptr;
    }
    
    GstPad* pad = gst_element_get_static_pad(video_appsink_.get(), "sink");
    if (!pad) {
        return nullptr;
    }
    
    GstCaps* caps = gst_pad_get_current_caps(pad);
    gst_object_unref(pad);
    
    return caps;
}

GstCaps* GstInput::get_audio_caps() const
{
    if (!audio_appsink_) {
        return nullptr;
    }
    
    GstPad* pad = gst_element_get_static_pad(audio_appsink_.get(), "sink");
    if (!pad) {
        return nullptr;
    }
    
    GstCaps* caps = gst_pad_get_current_caps(pad);
    gst_object_unref(pad);
    
    return caps;
}

}} // namespace caspar::gstreamer
