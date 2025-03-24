#include "gst_producer.h"
#include "gst_input.h"

#include "../util/gst_assert.h"
#include "../util/gst_util.h"

#include <boost/format.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/range/algorithm/rotate.hpp>

#include <common/diagnostics/graph.h>
#include <common/env.h>
#include <common/except.h>
#include <common/executor.h>
#include <common/os/thread.h>
#include <common/scope_exit.h>
#include <common/timer.h>

#include <core/frame/draw_frame.h>
#include <core/frame/frame_factory.h>
#include <core/monitor/monitor.h>

#include <algorithm>
#include <atomic>
#include <deque>
#include <iomanip>
#include <memory>
#include <sstream>
#include <thread>

namespace caspar { namespace gstreamer {

struct Frame
{
    GstSample*              video = nullptr;
    GstSample*              audio = nullptr;
    core::draw_frame        frame;
    int64_t                 start_time  = 0;
    int64_t                 pts         = 0;
    int64_t                 duration    = 0;
    int64_t                 frame_count = 0;
};

struct GstProducer::Impl
{
    caspar::core::monitor::state state_;
    mutable boost::mutex         state_mutex_;

    spl::shared_ptr<diagnostics::graph> graph_;

    const std::shared_ptr<core::frame_factory> frame_factory_;
    const core::video_format_desc              format_desc_;
    const std::string                          name_;
    const std::string                          path_;

    GstInput                input_;
    std::string             vfilter_;

    std::atomic<int64_t>    start_{0};
    std::atomic<int64_t>    duration_{std::numeric_limits<int64_t>::max()};
    std::atomic<int64_t>    input_duration_{0};
    std::atomic<int64_t>    seek_{-1};
    std::atomic<bool>       loop_{false};

    core::frame_geometry::scale_mode scale_mode_;
    int64_t                          frame_count_    = 0;
    bool                             frame_flush_    = true;
    int64_t                          frame_time_     = 0;
    int64_t                          frame_duration_ = 0;
    core::draw_frame                 frame_;

    std::deque<Frame>               buffer_;
    mutable boost::mutex            buffer_mutex_;
    boost::condition_variable       buffer_cond_;
    std::atomic<bool>               buffer_eof_{false};
    int                             buffer_capacity_ = static_cast<int>(format_desc_.fps) / 4;

    caspar::executor                executor_ { L"gstreamer_producer" };

    int latency_ = 0;

    boost::thread thread_;

    Impl(std::shared_ptr<core::frame_factory> frame_factory,
         core::video_format_desc              format_desc,
         std::string                          name,
         std::string                          path,
         std::string                          vfilter,
         std::optional<int64_t>               start,
         std::optional<int64_t>               seek,
         std::optional<int64_t>               duration,
         std::optional<bool>                  loop,
         core::frame_geometry::scale_mode     scale_mode)
        : frame_factory_(frame_factory)
        , format_desc_(format_desc)
        , name_(name)
        , path_(path)
        , input_(path, graph_)
        , vfilter_(vfilter)
        , start_(start.value_or(0))
        , duration_(duration.value_or(std::numeric_limits<int64_t>::max()))
        , loop_(loop.value_or(false))
        , scale_mode_(scale_mode)
    {
        diagnostics::register_graph(graph_);
        graph_->set_color("underflow", diagnostics::color(0.6f, 0.3f, 0.9f));
        graph_->set_color("frame-time", diagnostics::color(0.0f, 1.0f, 0.0f));
        graph_->set_color("buffer", diagnostics::color(1.0f, 1.0f, 0.0f));

        state_["file/name"] = u8(name_);
        state_["file/path"] = u8(path_);
        state_["loop"]      = loop_;
        update_state();

        input_.start();

        // If we have a specific seek position
        if (seek && *seek > 0) {
            seek_ = *seek;
        }

        thread_ = boost::thread([=] {
            try {
                set_thread_name(L"[gstreamer::producer]");
                run();
            } catch (boost::thread_interrupted&) {
                // Do nothing...
            } catch (...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
            }
        });
    }

    ~Impl()
    {
        try {
            if (thread_.joinable()) {
                thread_.interrupt();
                thread_.join();
            }
        } catch (boost::thread_interrupted&) {
            // Do nothing...
        }

        input_.abort();
    }

    void run()
    {
        std::vector<int> audio_cadence = format_desc_.audio_cadence;
        boost::range::rotate(audio_cadence, std::end(audio_cadence) - 1);

        Frame frame;
        timer frame_timer;

        int warning_debounce = 0;

        while (!thread_.interruption_requested()) {
            {
                const auto seek_pos = seek_.exchange(-1);
                if (seek_pos >= 0) {
                    // Perform seek
                    input_.seek(seek_pos);
                    frame = Frame{};
                    frame_flush_ = true;
                    continue;
                }
            }

            // Check if we've reached the end of the clip
            {
                auto start = start_.load();
                auto duration = duration_.load();

                auto end = (duration != std::numeric_limits<int64_t>::max()) ? start + duration : INT64_MAX;
                auto time = frame.pts + frame.duration;
                
                buffer_eof_ = input_.eof() || time >= end;

                if (buffer_eof_) {
                    if (loop_ && frame_count_ > 2) {
                        frame = Frame{};
                        input_.seek(start);
                        frame_flush_ = true;
                    } else {
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }
                    continue;
                }
            }

            // Get a video sample from GStreamer
            GstSample* video_sample = nullptr;
            if (input_.try_pop_video(&video_sample)) {
                if (video_sample) {
                    // Create a new frame
                    frame.video = video_sample;
                    
                    // Extract timing information
                    GstBuffer* buffer = gst_sample_get_buffer(video_sample);
                    frame.pts = GST_BUFFER_PTS(buffer) / 1000000; // Convert from ns to ms
                    frame.duration = format_desc_.duration;
                    
                    // Convert to a CasparCG frame
                    frame.frame = core::draw_frame(make_frame(this, *frame_factory_, video_sample));
                    frame.frame_count = frame_count_++;
                    
                    // Add to buffer
                    {
                        boost::unique_lock<boost::mutex> buffer_lock(buffer_mutex_);
                        buffer_cond_.wait(buffer_lock, [&] { return buffer_.size() < buffer_capacity_; });
                        if (seek_ == -1) {
                            buffer_.push_back(frame);
                        }
                    }
                    
                    graph_->set_value("buffer", static_cast<double>(buffer_.size()) / static_cast<double>(buffer_capacity_));
                    graph_->set_value("frame-time", frame_timer.elapsed() * format_desc_.fps * 0.5);
                    frame_timer.restart();
                    
                    // Clear frame to prepare for next
                    frame = Frame{};
                }
            } else {
                if (warning_debounce++ % 500 == 100) {
                    CASPAR_LOG(warning) << print() << " Waiting for video frame...";
                }
                
                // No frame available yet, sleep and try again
                std::this_thread::sleep_for(std::chrono::milliseconds(warning_debounce > 25 ? 20 : 5));
            }
        }
    }

    void update_state()
    {
        graph_->set_text(u16(print()));
        boost::lock_guard<boost::mutex> lock(state_mutex_);
        state_["file/clip"] = {start() / format_desc_.fps, duration() / format_desc_.fps};
        state_["file/time"] = {time() / format_desc_.fps, file_duration().value_or(0) / format_desc_.fps};
        state_["loop"]      = loop_;
    }

    core::draw_frame prev_frame(const core::video_field field)
    {
        CASPAR_SCOPE_EXIT { update_state(); };

        // Don't start a new frame on the 2nd field
        if (field != core::video_field::b) {
            if (frame_flush_ || !frame_) {
                boost::lock_guard<boost::mutex> lock(buffer_mutex_);

                if (!buffer_.empty()) {
                    frame_          = buffer_[0].frame;
                    frame_time_     = buffer_[0].pts;
                    frame_duration_ = buffer_[0].duration;
                    frame_flush_    = false;
                }
            }
        }

        return core::draw_frame::still(frame_);
    }

    bool is_ready()
    {
        boost::lock_guard<boost::mutex> lock(buffer_mutex_);
        return !buffer_.empty() || frame_;
    }

    core::draw_frame next_frame(const core::video_field field)
    {
        CASPAR_SCOPE_EXIT { update_state(); };

        boost::lock_guard<boost::mutex> lock(buffer_mutex_);

        if (buffer_.empty() || (frame_flush_ && buffer_.size() < 4)) {
            auto start    = start_.load();
            auto duration = duration_.load();

            auto end = (duration != std::numeric_limits<int64_t>::max()) ? start + duration : INT64_MAX;

            if (buffer_eof_ && !frame_flush_) {
                if (frame_time_ < end && frame_duration_ != 0) {
                    frame_time_ += frame_duration_;
                } else if (frame_time_ < end) {
                    frame_time_ = input_duration_;
                }
                return core::draw_frame::still(frame_);
            }
            
            graph_->set_tag(diagnostics::tag_severity::WARNING, "underflow");
            latency_ += 1;
            return core::draw_frame{};
        }

        if (format_desc_.field_count == 2) {
            // Check if the next frame is the correct 'field'
            auto is_field_1 = (buffer_[0].frame_count % 2) == 0;
            if ((field == core::video_field::a && !is_field_1) || (field == core::video_field::b && is_field_1)) {
                graph_->set_tag(diagnostics::tag_severity::WARNING, "underflow");
                latency_ += 1;
                return core::draw_frame{};
            }
        }

        if (latency_ != -1) {
            CASPAR_LOG(warning) << print() << " Latency: " << latency_;
            latency_ = -1;
        }

        frame_          = buffer_[0].frame;
        frame_time_     = buffer_[0].pts;
        frame_duration_ = buffer_[0].duration;
        frame_flush_    = false;
        
        // Free GStreamer memory if needed
        if (buffer_[0].video) {
            gst_sample_unref(buffer_[0].video);
        }
        if (buffer_[0].audio) {
            gst_sample_unref(buffer_[0].audio);
        }

        buffer_.pop_front();
        buffer_cond_.notify_all();

        graph_->set_value("buffer", static_cast<double>(buffer_.size()) / static_cast<double>(buffer_capacity_));

        return frame_;
    }

    void seek(int64_t time)
    {
        CASPAR_SCOPE_EXIT { update_state(); };

        seek_ = time;

        {
            boost::lock_guard<boost::mutex> lock(buffer_mutex_);
            buffer_.clear();
            
            // Free GStreamer memory
            for (auto& frame : buffer_) {
                if (frame.video) {
                    gst_sample_unref(frame.video);
                }
                if (frame.audio) {
                    gst_sample_unref(frame.audio);
                }
            }
            
            buffer_cond_.notify_all();
            graph_->set_value("buffer", static_cast<double>(buffer_.size()) / static_cast<double>(buffer_capacity_));
        }
    }

    int64_t time() const
    {
        return frame_time_;
    }

    void loop(bool loop)
    {
        CASPAR_SCOPE_EXIT { update_state(); };

        loop_ = loop;
    }

    bool loop() const { return loop_; }

    void start(int64_t start)
    {
        CASPAR_SCOPE_EXIT { update_state(); };
        start_ = start;
    }

    int64_t start() const
    {
        return start_.load();
    }

    void duration(int64_t duration)
    {
        CASPAR_SCOPE_EXIT { update_state(); };

        duration_ = duration;
    }

    int64_t duration() const
    {
        const auto duration = duration_.load();
        return duration != std::numeric_limits<int64_t>::max() ? duration : 0;
    }

    std::optional<int64_t> file_duration() const
    {
        const auto input_duration = input_.duration();
        if (input_duration == 0) {
            return {};
        }
        return input_duration;
    }

    std::string print() const
    {
        const int          position = std::max(static_cast<int>(time() - start()), 0);
        std::ostringstream str;
        str << std::fixed << std::setprecision(4) << "gstreamer[" << name_ << "|"
            << position / format_desc_.fps << "/"
            << (duration() > 0 ? duration() / format_desc_.fps : -1) << "]";
        return str.str();
    }
};

GstProducer::GstProducer(std::shared_ptr<core::frame_factory> frame_factory,
                       core::video_format_desc              format_desc,
                       std::string                          name,
                       std::string                          path,
                       std::optional<std::string>           vfilter,
                       std::optional<int64_t>               start,
                       std::optional<int64_t>               seek,
                       std::optional<int64_t>               duration,
                       std::optional<bool>                  loop,
                       core::frame_geometry::scale_mode     scale_mode)
    : impl_(new Impl(std::move(frame_factory),
                     std::move(format_desc),
                     std::move(name),
                     std::move(path),
                     std::move(vfilter.value_or("")),
                     std::move(start),
                     std::move(seek),
                     std::move(duration),
                     std::move(loop),
                     scale_mode))
{
}

core::draw_frame GstProducer::next_frame(const core::video_field field) { return impl_->next_frame(field); }

core::draw_frame GstProducer::prev_frame(const core::video_field field) { return impl_->prev_frame(field); }

bool GstProducer::is_ready() { return impl_->is_ready(); }

GstProducer& GstProducer::seek(int64_t time)
{
    impl_->seek(time);
    return *this;
}

GstProducer& GstProducer::loop(bool loop)
{
    impl_->loop(loop);
    return *this;
}

bool GstProducer::loop() const { return impl_->loop(); }

GstProducer& GstProducer::start(int64_t start)
{
    impl_->start(start);
    return *this;
}

int64_t GstProducer::time() const { return impl_->time(); }

int64_t GstProducer::start() const { return impl_->start(); }

GstProducer& GstProducer::duration(int64_t duration)
{
    impl_->duration(duration);
    return *this;
}

int64_t GstProducer::duration() const { return impl_->duration(); }

core::monitor::state GstProducer::state() const
{
    boost::lock_guard<boost::mutex> lock(impl_->state_mutex_);
    return impl_->state_;
}

}} // namespace caspar::gstreamer