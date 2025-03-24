#pragma once

#include "../util/gst_util.h"
#include <common/diagnostics/graph.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>

#include <tbb/concurrent_queue.h>

#include <boost/thread.hpp>

namespace caspar { namespace gstreamer {

class GstInput
{
  public:
    GstInput(const std::string& uri, std::shared_ptr<diagnostics::graph> graph, std::optional<bool> loop = std::nullopt);
    ~GstInput();

    // Get video and audio samples
    bool try_pop_video(GstSample** sample);
    bool try_pop_audio(GstSample** sample);
    
    // Query pipeline information
    int width() const;
    int height() const;
    int audio_channels() const;
    int audio_sample_rate() const;
    
    // Control methods
    void seek(int64_t position, bool flush = true);
    void abort();
    void reset();
    bool eof() const;
    int64_t duration() const;
    void start();
    void stop();
    
    // Get stream information
    GstCaps* get_video_caps() const;
    GstCaps* get_audio_caps() const;
    
    // Status information
    bool is_valid() const { return pipeline_ != nullptr; }
    
    // Static callback handlers for AppSink
    static GstFlowReturn new_video_sample(GstAppSink* sink, gpointer user_data);
    static GstFlowReturn new_audio_sample(GstAppSink* sink, gpointer user_data);

  private:
    void initialize_pipeline(const std::string& uri);
    void create_pipeline(const std::string& uri);
    
    std::string                              uri_;
    std::shared_ptr<diagnostics::graph>      graph_;
    std::optional<bool>                      loop_;

    // Pipeline elements
    gst_ptr<GstElement>                      pipeline_;
    gst_ptr<GstElement>                      video_appsink_;
    gst_ptr<GstElement>                      audio_appsink_;
    
    // Sample buffers
    tbb::concurrent_bounded_queue<GstSample*> video_buffer_;
    tbb::concurrent_bounded_queue<GstSample*> audio_buffer_;
    
    // Pipeline state
    std::atomic<bool>                        initialized_{false};
    std::atomic<bool>                        eof_{false};
    std::atomic<bool>                        abort_request_{false};
    
    // Stream info
    std::atomic<int>                         width_{0};
    std::atomic<int>                         height_{0};
    std::atomic<int>                         audio_channels_{0};
    std::atomic<int>                         audio_sample_rate_{0};
    std::atomic<int64_t>                     duration_{0};  // Store in milliseconds instead of GstClockTime
    
    // Synchronization
    mutable std::mutex                       mutex_;
    std::condition_variable                  cond_;
    
    // Monitoring thread
    boost::thread                            thread_;
};

}} // namespace caspar::gstreamer
