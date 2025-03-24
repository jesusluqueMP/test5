/*
 * This file is part of CasparCG (www.casparcg.com).
 *
 * CasparCG is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CasparCG is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with CasparCG. If not, see <http://www.gnu.org/licenses/>.
 */

#include "gstreamer_consumer.h"

#include "../util/gst_util.h"
#include "../util/gst_assert.h"

#include <common/bit_depth.h>
#include <common/diagnostics/graph.h>
#include <common/env.h>
#include <common/executor.h>
#include <common/future.h>
#include <common/memory.h>
#include <common/scope_exit.h>
#include <common/timer.h>

#include <core/frame/frame.h>
#include <core/video_format.h>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/regex.hpp>

#include <tbb/concurrent_queue.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_invoke.h>

#include <memory>
#include <thread>
#include <map>

namespace caspar { namespace gstreamer {

struct gstreamer_consumer : public core::frame_consumer
{
    core::monitor::state        state_;
    mutable std::mutex          state_mutex_;
    int                         channel_index_ = -1;
    core::video_format_desc     format_desc_;
    bool                        realtime_ = false;

    spl::shared_ptr<diagnostics::graph> graph_;

    std::string path_;
    std::string args_;

    std::exception_ptr exception_;
    std::mutex         exception_mutex_;

    tbb::concurrent_bounded_queue<core::const_frame> frame_buffer_;
    std::thread                                      frame_thread_;

    common::bit_depth depth_;
    
    // GStreamer pipeline
    gst_ptr<GstElement>     pipeline_;
    gst_ptr<GstElement>     appsrc_;
    
    // Frame buffer & processing
    std::atomic<bool>       is_running_{false};
    std::atomic<bool>       aborting_{false};

  public:
    gstreamer_consumer(std::string path, std::string args, bool realtime, common::bit_depth depth)
        : channel_index_(-1)  // Initialize to a default value
        , realtime_(realtime)
        , path_(std::move(path))
        , args_(std::move(args))
        , depth_(depth)
    {
        // Generate a consistent index based on the path 
        // We'll use a simple hash to avoid CRC dependency issues
        uint32_t hash = 0;
        for (char c : path_) {
            hash = hash * 31 + c;
        }
        channel_index_ = static_cast<int>(hash % 10000);
        
        state_["file/path"] = u8(path_);

        frame_buffer_.set_capacity(realtime_ ? 1 : 64);

        diagnostics::register_graph(graph_);
        graph_->set_color("frame-time", diagnostics::color(0.1f, 1.0f, 0.1f));
        graph_->set_color("dropped-frame", diagnostics::color(0.3f, 0.6f, 0.3f));
        graph_->set_color("input", diagnostics::color(0.7f, 0.4f, 0.4f));
        
        CASPAR_LOG(info) << "Created GStreamer consumer for " << path_;
    }

    ~gstreamer_consumer()
    {
        aborting_ = true;
        
        if (frame_thread_.joinable()) {
            frame_buffer_.push(core::const_frame{});
            frame_thread_.join();
        }
        
        if (pipeline_) {
            gst_element_set_state(pipeline_.get(), GST_STATE_NULL);
        }
    }

    // frame consumer
    void initialize(const core::video_format_desc& format_desc, int channel_index) override
    {
        if (frame_thread_.joinable()) {
            CASPAR_THROW_EXCEPTION(invalid_operation() << msg_info("Cannot reinitialize gstreamer-consumer."));
        }

        format_desc_   = format_desc;
        channel_index_ = channel_index;

        graph_->set_text(print());

        frame_thread_ = std::thread([this] {
            try {
                std::map<std::string, std::string> options;
                {
                    static boost::regex opt_exp("-(?<name>[^\\s]+)(\\s+(?<value>[^\\s]+))?");
                    for (auto it = boost::sregex_iterator(args_.begin(), args_.end(), opt_exp);
                         it != boost::sregex_iterator();
                         ++it) {
                        std::string name = (*it)["name"].str();
                        std::string value = (*it)["value"].matched ? (*it)["value"].str() : "";
                        options[name] = value;
                    }
                }

                // Create GStreamer pipeline with the extracted options
                create_pipeline(options);
                
                if (!pipeline_) {
                    CASPAR_LOG(error) << "Failed to create GStreamer pipeline for " << path_;
                    return;
                }
                
                // Start the pipeline
                GstStateChangeReturn ret = gst_element_set_state(pipeline_.get(), GST_STATE_PLAYING);
                if (ret == GST_STATE_CHANGE_FAILURE) {
                    CASPAR_LOG(error) << "Failed to start GStreamer pipeline for " << path_;
                    return;
                }
                
                is_running_ = true;
                
                process_frames();
            }
            catch(...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
                std::lock_guard<std::mutex> lock(exception_mutex_);
                exception_ = std::current_exception();
            }
        });
    }

    std::future<bool> send(core::video_field field, core::const_frame frame) override
    {
        {
            std::lock_guard<std::mutex> lock(exception_mutex_);
            if (exception_ != nullptr) {
                std::rethrow_exception(exception_);
            }
        }

        if (!frame_buffer_.try_push(frame)) {
            graph_->set_tag(diagnostics::tag_severity::WARNING, "dropped-frame");
        }
        graph_->set_value("input", static_cast<double>(frame_buffer_.size() + 0.001) / frame_buffer_.capacity());

        return make_ready_future(is_running_.load());
    }

    std::wstring print() const override { return L"gstreamer[" + u16(path_) + L"]"; }

    std::wstring name() const override { return L"gstreamer"; }

    bool has_synchronization_clock() const override { return false; }

    int index() const override { return 600000 + channel_index_; }

    core::monitor::state state() const override
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        return state_;
    }
    
private:
    // The create_pipeline method now takes a const reference to a map of options
    void create_pipeline(const std::map<std::string, std::string>& options) 
    {
        std::string pipeline_desc;
        
        // Check if we're streaming or writing to a file
        bool is_stream = path_.find("://") != std::string::npos;
        
        // Get format-specific options
        std::string video_codec = "x264";  // Default codec
        int video_bitrate = 3000;          // Default bitrate
        int audio_bitrate = 128;           // Default audio bitrate
        
        // Extract options from the map
        auto codec_it = options.find("vcodec");
        if (codec_it != options.end()) {
            video_codec = codec_it->second;
        }
        
        auto vbitrate_it = options.find("vbitrate");
        if (vbitrate_it != options.end()) {
            try {
                video_bitrate = std::stoi(vbitrate_it->second);
            } catch (...) {
                // Use default if conversion fails
            }
        }
        
        auto abitrate_it = options.find("abitrate");
        if (abitrate_it != options.end()) {
            try {
                audio_bitrate = std::stoi(abitrate_it->second);
            } catch (...) {
                // Use default if conversion fails
            }
        }
        
        // Create video source (appsrc)
        pipeline_desc += "appsrc name=video_src format=time do-timestamp=true is-live=true ";
        pipeline_desc += "caps=video/x-raw,format=BGRA,width=" + std::to_string(format_desc_.width) + 
                        ",height=" + std::to_string(format_desc_.height) + 
                        ",framerate=" + std::to_string(format_desc_.framerate.numerator()) + "/" + 
                        std::to_string(format_desc_.framerate.denominator()) + " ! ";
        
        // Add video conversion and encoding
        if (video_codec == "x264") {
            pipeline_desc += "videoconvert ! x264enc bitrate=" + std::to_string(video_bitrate) + 
                            " speed-preset=veryfast tune=zerolatency ! ";
        } else if (video_codec == "openh264") {
            pipeline_desc += "videoconvert ! openh264enc bitrate=" + std::to_string(video_bitrate*1000) + " ! ";
        } else if (video_codec == "nvenc") {
            pipeline_desc += "videoconvert ! nvh264enc bitrate=" + std::to_string(video_bitrate) + " ! ";
        } else if (video_codec == "vp8") {
            pipeline_desc += "videoconvert ! vp8enc target-bitrate=" + std::to_string(video_bitrate*1000) + " ! ";
        } else if (video_codec == "vp9") {
            pipeline_desc += "videoconvert ! vp9enc target-bitrate=" + std::to_string(video_bitrate*1000) + " ! ";
        } else {
            // Default to x264
            pipeline_desc += "videoconvert ! x264enc bitrate=" + std::to_string(video_bitrate) + " ! ";
        }
        
        // Add audio source and encoding (using a silent audio source for now)
        pipeline_desc += "queue ! ";
        
        if (is_stream) {
            // RTMP streaming
            if (path_.substr(0, 7) == "rtmp://") {
                pipeline_desc += "h264parse ! flvmux streamable=true ! rtmpsink location=\"" + path_ + "\" ";
            }
            // RTSP streaming
            else if (path_.substr(0, 7) == "rtsp://") {
                pipeline_desc += "h264parse ! rtph264pay ! udpsink host=" + path_.substr(7) + " port=5000 ";
            }
            // UDP streaming
            else if (path_.substr(0, 6) == "udp://") {
                pipeline_desc += "h264parse ! rtph264pay ! udpsink host=" + path_.substr(6) + " port=5000 ";
            }
            // HTTP streaming
            else if (path_.substr(0, 7) == "http://") {
                pipeline_desc += "h264parse ! mpegtsmux ! hlssink location=" + path_.substr(7) + " ";
            }
            // Default to file output
            else {
                pipeline_desc += "h264parse ! mp4mux ! filesink location=\"" + path_ + "\" ";
            }
        }
        // File output with specific container format
        else {
            std::string ext = boost::filesystem::path(path_).extension().string();
            boost::to_lower(ext);
            
            if (ext == ".flv") {
                pipeline_desc += "h264parse ! flvmux ! filesink location=\"" + path_ + "\" ";
            }
            else if (ext == ".mp4" || ext == ".mov") {
                pipeline_desc += "h264parse ! mp4mux ! filesink location=\"" + path_ + "\" ";
            }
            else if (ext == ".mkv") {
                pipeline_desc += "h264parse ! matroskamux ! filesink location=\"" + path_ + "\" ";
            }
            else if (ext == ".ts") {
                pipeline_desc += "h264parse ! mpegtsmux ! filesink location=\"" + path_ + "\" ";
            }
            else if (ext == ".webm") {
                // Adjust for WebM (VP8/VP9)
                if (video_codec == "vp8" || video_codec == "vp9") {
                    pipeline_desc += video_codec + "parse ! webmmux ! filesink location=\"" + path_ + "\" ";
                } else {
                    // If not using VP8/VP9, we can't output WebM
                    CASPAR_LOG(warning) << "WebM container requires VP8 or VP9 codec. Switching to MKV container.";
                    pipeline_desc += "h264parse ! matroskamux ! filesink location=\"" + 
                                    boost::filesystem::path(path_).replace_extension(".mkv").string() + "\" ";
                }
            }
            else {
                // Default to MP4
                pipeline_desc += "h264parse ! mp4mux ! filesink location=\"" + path_ + "\" ";
            }
        }
        
        CASPAR_LOG(info) << "Creating GStreamer pipeline: " << pipeline_desc;
        
        // Create the pipeline
        pipeline_ = create_pipeline(pipeline_desc);
        
        // Get elements
        appsrc_ = make_gst_ptr<GstElement>(gst_bin_get_by_name(GST_BIN(pipeline_.get()), "video_src"));
        
        if (appsrc_) {
            // Configure appsrc
            g_object_set(G_OBJECT(appsrc_.get()), "format", GST_FORMAT_TIME, NULL);
            g_object_set(G_OBJECT(appsrc_.get()), "do-timestamp", TRUE, NULL);
            g_object_set(G_OBJECT(appsrc_.get()), "is-live", realtime_, NULL);
            
            if (realtime_) {
                g_object_set(G_OBJECT(appsrc_.get()), "max-bytes", 1920 * 1080 * 4 * 4, NULL); // 4 frames of BGRA
            } else {
                g_object_set(G_OBJECT(appsrc_.get()), "max-bytes", 1920 * 1080 * 4 * 16, NULL); // 16 frames of BGRA
            }
        }
    }
    
    void process_frames() 
    {
        caspar::timer frame_timer;
        int64_t frame_count = 0;
        
        while (!aborting_) {
            core::const_frame frame;
            frame_buffer_.pop(frame);
            
            // Empty frame means exit
            if (!frame) {
                break;
            }
            
            frame_timer.restart();
            
            // Send frame to GStreamer
            try {
                GstSample* sample = make_gst_sample(frame, format_desc_);
                if (sample) {
                    GstBuffer* buffer = gst_sample_get_buffer(sample);
                    
                    // Set buffer timestamp and duration with proper conversion
                    // Convert frame count to seconds, then to nanoseconds for GstClockTime
                    double frame_seconds = static_cast<double>(frame_count) / format_desc_.fps;
                    GST_BUFFER_PTS(buffer) = static_cast<GstClockTime>(frame_seconds * GST_SECOND);
                    GST_BUFFER_DURATION(buffer) = static_cast<GstClockTime>(GST_SECOND / format_desc_.fps);
                    
                    // Increment frame counter
                    frame_count++;
                    
                    // Push buffer to appsrc
                    GstFlowReturn ret = gst_app_src_push_sample(GST_APP_SRC(appsrc_.get()), sample);
                    if (ret != GST_FLOW_OK) {
                        CASPAR_LOG(error) << "Error pushing sample to GStreamer pipeline: " << gst_flow_get_name(ret);
                    }
                    
                    // Release the sample
                    gst_sample_unref(sample);
                }
            }
            catch (const std::exception& e) {
                CASPAR_LOG(error) << "Error processing frame for GStreamer: " << e.what();
            }
            
            graph_->set_value("frame-time", frame_timer.elapsed() * format_desc_.fps * 0.5);
            graph_->set_value("input", static_cast<double>(frame_buffer_.size() + 0.001) / frame_buffer_.capacity());
        }
        
        // Send EOS to clean up the pipeline
        if (pipeline_ && appsrc_) {
            gst_app_src_end_of_stream(GST_APP_SRC(appsrc_.get()));
        }
        
        is_running_ = false;
    }
};

spl::shared_ptr<core::frame_consumer> create_consumer(const std::vector<std::wstring>&     params,
                                                      const core::video_format_repository& format_repository,
                                                      const std::vector<spl::shared_ptr<core::video_channel>>& channels,
                                                      common::bit_depth                                        depth)
{
    if (params.size() < 2 || (!boost::iequals(params.at(0), L"STREAM") && !boost::iequals(params.at(0), L"FILE")))
        return core::frame_consumer::empty();

    auto                     path = u8(params.at(1));
    std::vector<std::string> args;
    for (auto n = 2; n < params.size(); ++n) {
        args.emplace_back(u8(params[n]));
    }
    return spl::make_shared<gstreamer_consumer>(
        path, boost::join(args, " "), boost::iequals(params.at(0), L"STREAM"), depth);
}

spl::shared_ptr<core::frame_consumer>
create_preconfigured_consumer(const boost::property_tree::wptree&                      ptree,
                              const core::video_format_repository&                     format_repository,
                              const std::vector<spl::shared_ptr<core::video_channel>>& channels,
                              common::bit_depth                                        depth)
{
    return spl::make_shared<gstreamer_consumer>(u8(ptree.get<std::wstring>(L"path", L"")),
                                             u8(ptree.get<std::wstring>(L"args", L"")),
                                             ptree.get(L"realtime", false),
                                             depth);
}

}} // namespace caspar::gstreamer
